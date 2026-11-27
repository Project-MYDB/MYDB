#include <dirent.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "checksum.h"
#include "common.h"
#include "wal_flush.h"
#include "wal_frame_state.h"
#include "wal_manager.h"
#include "wal_page.h"
#include "wal_segment_pool.h"
#include "wal_insert.h"
#include "wal_types.h"

/*
 * test_wal_flush — the write and flush path (MYDB_WAL_DESIGN_V2.md §12),
 * on real segment files. The write step only moves bytes, so most tests
 * use a stand-in copier that fills the ring with a pattern of each byte's
 * LSN and adds to the block counters the way wal_insert_copy does; no
 * records are needed. The last ones insert real records with wal_insert
 * from many threads and read the log back from the files. (The scan's own
 * cases are in test_wal_insert, with counters by hand.)
 */

#define TEST_WAL_DIR "/tmp/mydb_test_wal_flush"
#define MB           (1024u * 1024u)

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

static void cleanup(void)
{
    DIR *d = opendir(TEST_WAL_DIR);
    if (d) {
        struct dirent *ent;
        char path[400];
        while ((ent = readdir(d)) != NULL) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
            snprintf(path, sizeof(path), "%s/%s", TEST_WAL_DIR, ent->d_name);
            unlink(path);
        }
        closedir(d);
    }
    rmdir(TEST_WAL_DIR);
}

/* A fresh directory and a WalManager on it: segment 0, write_lsn 0. */
static bool fresh(WalManager *wm)
{
    cleanup();
    memset(wm, 0, sizeof(*wm));
    return wal_manager_init(wm, TEST_WAL_DIR, 1) == MYDB_OK;
}

static void done(WalManager *wm)
{
    wal_manager_shutdown(wm);
    cleanup();
}

/* Gives a fresh wm a ring of frames frames, zeroed, before any insert. */
static bool set_ring(WalManager *wm, uint32_t frames)
{
    size_t   ctr_bytes = (size_t)frames * WAL_FRAME_BLOCKS * sizeof(WalBlockCounter);
    uint8_t *ring      = aligned_alloc(WAL_PAGE_SIZE, (size_t)frames * WAL_PAGE_SIZE);
    WalBlockCounter *ctrs = aligned_alloc(64, ctr_bytes);
    if (!ring || !ctrs) { free(ring); free(ctrs); return false; }
    memset(ring, 0, (size_t)frames * WAL_PAGE_SIZE);
    memset(ctrs, 0, ctr_bytes);
    free(wm->ring_buf);
    free(wm->fs_counters);
    wm->ring_buf        = ring;
    wm->fs_counters     = ctrs;
    wm->ring_buf_size   = frames;
    wm->ring_frame_mask = frames - 1;
    wm->ctr_mask        = frames * WAL_FRAME_BLOCKS - 1;
    return true;
}

/* Polls *v until it reaches want, for up to 2 s. */
static bool reaches(_Atomic uint64_t *v, uint64_t want)
{
    for (int i = 0; i < 2000 && atomic_load(v) < want; i++) usleep(1000);
    return atomic_load(v) >= want;
}

/* The byte the stand-in copier puts at lsn; differs between ring passes
 * and segments, so a byte from the wrong place shows. */
static uint8_t pat(uint64_t lsn)
{
    return (uint8_t)((lsn * 7) ^ (lsn >> 12) ^ (lsn >> 22) ^ 0x5A);
}

static uint64_t ring_bytes(const WalManager *wm)
{
    return (uint64_t)wm->ring_buf_size << WAL_PAGE_SHIFT;
}

/* Stand-in copier: bytes [from, to) into the ring, each block's share
 * added to its counter with release. */
static void copy_in(WalManager *wm, uint64_t from, uint64_t to)
{
    uint64_t mask = ring_bytes(wm) - 1;
    for (uint64_t lsn = from; lsn < to; lsn++) wm->ring_buf[lsn & mask] = pat(lsn);
    for (uint64_t b = from; b < to; ) {
        uint64_t block_end = (b | (WAL_BLOCK_SIZE - 1)) + 1;
        uint64_t n = (block_end < to ? block_end : to) - b;
        atomic_fetch_add_explicit(wal_block_counter(wm->fs_counters, wm->ctr_mask, b),
                                  (uint16_t)n, memory_order_release);
        b += n;
    }
}

/* current_pos for reservations up to lsn (a page end, or offset >= 24). */
static void reserve_to(WalManager *wm, uint64_t lsn)
{
    uint64_t pos = wal_lsn_offset(lsn) == 0 ? (lsn >> WAL_PAGE_SHIFT) * WAL_PAGE_USABLE
                                            : wal_lsn_to_byte(lsn);
    atomic_store(&wm->current_pos, pos);
}

static uint16_t ctr(WalManager *wm, uint32_t i) { return atomic_load(&wm->fs_counters[i].done); }

/* Copy, reserve and write [write_lsn, to), in rounds of at most 1 MB. */
static bool advance_to(WalManager *wm, uint64_t to)
{
    WalScanResult r;
    uint64_t pos = atomic_load(&wm->write_lsn);
    while (pos < to) {
        uint64_t next = pos + MB < to ? pos + MB : to;
        copy_in(wm, pos, next);
        reserve_to(wm, next);
        if (wal_flush_write_chunk(wm, &r) != MYDB_OK || r.end_lsn != next) return false;
        pos = next;
    }
    return true;
}

/* Does slot's file hold pat() for every LSN in [from, to)? One segment. */
static bool disk_matches(WalManager *wm, uint32_t slot, uint64_t from, uint64_t to)
{
    uint8_t page[WAL_PAGE_SIZE];
    for (uint64_t lsn = from; lsn < to; ) {
        if (wal_segment_pool_read_page(&wm->pool, slot, wal_lsn_page(lsn) + 1, page) != MYDB_OK)
            return false;
        uint64_t page_end = (lsn | (WAL_PAGE_SIZE - 1)) + 1;
        for (; lsn < to && lsn < page_end; lsn++)
            if (page[wal_lsn_offset(lsn)] != pat(lsn)) return false;
    }
    return true;
}

/* A tail, a hole, then the rest, inside the first frames. */
static void test_write_basic(void)
{
    printf("\n[test_write_basic]\n");
    WalManager wm;
    WalScanResult r;
    CHECK(fresh(&wm) && atomic_load(&wm.write_lsn) == 0, "fresh log: segment 0, write_lsn 0");
    uint32_t slot = wm.curr_slot;

    CHECK(wal_flush_write_chunk(&wm, &r) == MYDB_OK && r.end_lsn == 0 && atomic_load(&wm.write_lsn) == 0,
          "nothing copied: the write step writes nothing and succeeds");

    copy_in(&wm, 0, 1000);
    reserve_to(&wm, 1000);
    CHECK(wal_flush_write_chunk(&wm, &r) == MYDB_OK && r.end_lsn == 1000 && r.tail && r.full_blocks == 1,
          "a tail at 1000: block 0 full, block 1 complete so far");
    CHECK(atomic_load(&wm.write_lsn) == 1000 && wm.last_ctr_idx == 1,
          "write_lsn 1000, last_ctr_idx 1 (the block holding 1000)");
    CHECK(ctr(&wm, 0) == 0 && ctr(&wm, 1) == 488,
          "block 0's counter zeroed, the tail block's 488 left alone");
    CHECK(disk_matches(&wm, slot, 0, 1000), "LSN 0..1000 is in the file at 4096 + lsn");

    copy_in(&wm, 1000, 5000);
    reserve_to(&wm, 5000);
    CHECK(wal_flush_write_chunk(&wm, &r) == MYDB_OK && r.end_lsn == 5000 && r.tail && r.full_blocks == 8,
          "from mid-block 1000 to 5000: blocks 1..8 full, a tail in block 9");
    bool zero = true;
    for (uint32_t i = 1; i <= 8; i++) if (ctr(&wm, i) != 0) zero = false;
    CHECK(zero && ctr(&wm, 9) == 5000 - 4608 && wm.last_ctr_idx == 9,
          "blocks 1..8 zeroed, block 9 keeps its 392, last_ctr_idx 9");
    CHECK(disk_matches(&wm, slot, 0, 5000), "LSN 0..5000 in the file, the first 1000 not disturbed");

    /* A hole at [5300, 5500): block 10 (5120..5632) has 180 + 132 = 312. */
    copy_in(&wm, 5000, 5300);
    copy_in(&wm, 5500, 6000);
    reserve_to(&wm, 6000);
    CHECK(wal_flush_write_chunk(&wm, &r) == MYDB_OK && r.end_lsn == 5120 && !r.tail && r.full_blocks == 1,
          "a hole in block 10: block 9 completes, the write stops at 5120");
    CHECK(atomic_load(&wm.write_lsn) == 5120 && wm.last_ctr_idx == 10 && ctr(&wm, 9) == 0 && ctr(&wm, 10) == 312,
          "write_lsn 5120, block 9 zeroed, the hole block's 312 left alone");
    CHECK(disk_matches(&wm, slot, 0, 5120), "LSN 0..5120 in the file");

    copy_in(&wm, 5300, 5500);
    CHECK(wal_flush_write_chunk(&wm, &r) == MYDB_OK && r.end_lsn == 6000 && r.tail,
          "the hole filled: the next write step goes on to 6000");
    CHECK(disk_matches(&wm, slot, 0, 6000), "LSN 0..6000 in the file");

    done(&wm);
}

/* A write across the ring's end, inside one segment: a 1 MB ring. */
static void test_write_wraps_ring(void)
{
    printf("\n[test_write_wraps_ring]\n");
    WalManager wm;
    WalScanResult r;
    CHECK(fresh(&wm), "fresh log");
    wm.ring_buf_size   = 256;                        /* 1 MB of the 4 MB buffer */
    wm.ring_frame_mask = 255;
    wm.ctr_mask        = 256 * WAL_FRAME_BLOCKS - 1;

    CHECK(advance_to(&wm, MB - 200), "written up to 200 bytes before the ring's end");
    copy_in(&wm, MB - 200, MB + 300);
    reserve_to(&wm, MB + 300);
    CHECK(wal_flush_write_chunk(&wm, &r) == MYDB_OK && r.end_lsn == MB + 300,
          "one write step across the ring's end: two chunks, up to 1 MB + 300");
    CHECK(disk_matches(&wm, wm.curr_slot, 0, MB + 300),
          "the file holds every byte in LSN order, the wrapped part after the ring's end");
    CHECK(wm.last_ctr_idx == (((MB + 300) >> WAL_BLOCK_SHIFT) & wm.ctr_mask) && wm.last_ctr_idx == 0,
          "last_ctr_idx wrapped to counter 0");

    done(&wm);
}

/* A ring of exactly one segment (16 MB) that ends at the segment's end:
 * no rollover until a later write needs the next segment. */
static void test_write_full_ring_and_lazy_rollover(void)
{
    printf("\n[test_write_full_ring_and_lazy_rollover]\n");
    WalManager wm;
    WalScanResult r;
    CHECK(fresh(&wm) && set_ring(&wm, WAL_SEG_PAGES), "fresh log, a 16 MB ring (one segment)");
    uint32_t seg0 = wm.curr_slot;
    const uint64_t seg_end = WAL_SEG_SIZE;

    copy_in(&wm, 0, seg_end);
    reserve_to(&wm, seg_end);
    CHECK(wal_flush_write_chunk(&wm, &r) == MYDB_OK && r.end_lsn == seg_end &&
          r.full_blocks == wm.ctr_mask + 1 && !r.tail,
          "a completely full ring: 32768 full blocks, all 16 MB written in one step");
    bool zero = true;
    for (uint32_t i = 0; i <= wm.ctr_mask; i++) if (ctr(&wm, i) != 0) zero = false;
    CHECK(zero && wm.last_ctr_idx == 0 && atomic_load(&wm.write_lsn) == seg_end,
          "every counter zeroed, last_ctr_idx back at 0, write_lsn 16 MB");
    CHECK(wm.curr_slot == seg0 && wm.pool.slots[seg0].header.state == SEG_ACTIVE,
          "ending exactly at the segment's end: no rollover yet, segment 0 still active");
    CHECK(disk_matches(&wm, seg0, 0, seg_end), "all of segment 0 is in its file");

    copy_in(&wm, seg_end, seg_end + 300);
    reserve_to(&wm, seg_end + 300);
    CHECK(wal_flush_write_chunk(&wm, &r) == MYDB_OK && r.end_lsn == seg_end + 300,
          "the next write needs segment 1: rolls over, then writes");
    CHECK(wm.pool.slots[seg0].header.state == SEG_DONE &&
          wm.pool.slots[seg0].header.end_lsn == seg_end &&
          wm.pool.slots[seg0].header.data_pages == WAL_SEG_PAGES,
          "segment 0 is DONE, end_lsn 16 MB, data_pages 4096");
    CHECK(wm.curr_slot != seg0 && wm.pool.slots[wm.curr_slot].header.state == SEG_ACTIVE &&
          wal_segment_pool_segment_no(&wm.pool, wm.curr_slot) == 1,
          "curr_slot now holds segment 1, active");
    CHECK(disk_matches(&wm, wm.curr_slot, seg_end, seg_end + 300), "segment 1's first 300 bytes are in its file");
    CHECK(reaches(&wm.worker.synced_lsn, seg_end) && reaches(&wm.flushed_lsn, seg_end),
          "the worker syncs segment 0 and, with flush_lock free, publishes flushed_lsn 16 MB");

    done(&wm);
}

/* One write step that crosses a segment's end. */
static void test_write_splits_at_segment_end(void)
{
    printf("\n[test_write_splits_at_segment_end]\n");
    WalManager wm;
    WalScanResult r;
    CHECK(fresh(&wm), "fresh log");
    uint32_t seg0 = wm.curr_slot;
    const uint64_t seg_end = WAL_SEG_SIZE;

    CHECK(advance_to(&wm, seg_end - 100) && wm.curr_slot == seg0,
          "written in 1 MB rounds up to 100 bytes before the segment's end");
    copy_in(&wm, seg_end - 100, seg_end + 300);
    reserve_to(&wm, seg_end + 300);
    CHECK(wal_flush_write_chunk(&wm, &r) == MYDB_OK && r.end_lsn == seg_end + 300,
          "one write step from 16 MB - 100 to 16 MB + 300");
    CHECK(wm.pool.slots[seg0].header.state == SEG_DONE && wm.pool.slots[seg0].header.end_lsn == seg_end &&
          wal_segment_pool_segment_no(&wm.pool, wm.curr_slot) == 1,
          "split at the segment's end: segment 0 DONE at 16 MB, segment 1 active");
    CHECK(disk_matches(&wm, seg0, 0, seg_end), "segment 0's file holds all of it, the last 100 bytes included");
    CHECK(disk_matches(&wm, wm.curr_slot, seg_end, seg_end + 300), "segment 1's file holds the other 300");

    done(&wm);
}

/* A ring larger than a segment (32 MB): the segment's end is not a ring
 * end, so only the segment split cuts the write there. With a ring of
 * 16 MB or less every segment end is also a ring end (both powers of two). */
static void test_write_splits_at_segment_end_inside_ring(void)
{
    printf("\n[test_write_splits_at_segment_end_inside_ring]\n");
    WalManager wm;
    WalScanResult r;
    CHECK(fresh(&wm) && set_ring(&wm, 2 * WAL_SEG_PAGES), "fresh log, a 32 MB ring and its 65536 counters");
    uint32_t seg0 = wm.curr_slot;
    const uint64_t seg_end = WAL_SEG_SIZE;

    CHECK(advance_to(&wm, seg_end - 100), "written up to 100 bytes before the segment's end");
    copy_in(&wm, seg_end - 100, seg_end + 300);
    reserve_to(&wm, seg_end + 300);
    CHECK(wal_flush_write_chunk(&wm, &r) == MYDB_OK && r.end_lsn == seg_end + 300,
          "one write step across the segment's end, in the middle of the ring");
    CHECK(wm.pool.slots[seg0].header.state == SEG_DONE &&
          wal_segment_pool_segment_no(&wm.pool, wm.curr_slot) == 1,
          "split there: segment 0 DONE, segment 1 active");
    CHECK(disk_matches(&wm, seg0, seg_end - 100, seg_end) &&
          disk_matches(&wm, wm.curr_slot, seg_end, seg_end + 300),
          "100 bytes at segment 0's end, 300 at segment 1's start");

    done(&wm);
}

/* A failed pwrite leaves write_lsn, last_ctr_idx and the counters alone. */
static void test_write_io_error(void)
{
    printf("\n[test_write_io_error]\n");
    WalManager wm;
    WalScanResult r;
    CHECK(fresh(&wm), "fresh log");

    copy_in(&wm, 0, 1000);
    reserve_to(&wm, 1000);
    int fd = wm.pool.slots[wm.curr_slot].fd;
    wm.pool.slots[wm.curr_slot].fd = -1;             /* every pwrite fails: EBADF */
    CHECK(wal_flush_write_chunk(&wm, &r) == MYDB_ERR, "the pwrite fails: MYDB_ERR");
    CHECK(atomic_load(&wm.write_lsn) == 0 && wm.last_ctr_idx == 0 && ctr(&wm, 0) == 512 && ctr(&wm, 1) == 488,
          "nothing published: write_lsn 0, last_ctr_idx 0, counters as they were");

    wm.pool.slots[wm.curr_slot].fd = fd;
    CHECK(wal_flush_write_chunk(&wm, &r) == MYDB_OK && r.end_lsn == 1000 && disk_matches(&wm, wm.curr_slot, 0, 1000),
          "with the file back, the same bytes are written by the next step");

    done(&wm);
}

/* ------------------------------------------------------------------
 * With the locks: log_flush, the hole wait, the space waiter.
 * ------------------------------------------------------------------ */

/* The commit path alone: one thread, the stand-in copier. */
static void test_log_flush(void)
{
    printf("\n[test_log_flush]\n");
    WalManager wm;
    CHECK(fresh(&wm) && atomic_load(&wm.flushed_lsn) == 0, "fresh log: flushed_lsn 0");

    copy_in(&wm, 0, 1000);
    reserve_to(&wm, 1000);
    CHECK(wal_log_flush(&wm, 1000) == MYDB_OK &&
          atomic_load(&wm.write_lsn) == 1000 && atomic_load(&wm.flushed_lsn) == 1000,
          "a commit at 1000: written and synced, flushed_lsn 1000");
    CHECK(wal_log_flush(&wm, 500) == MYDB_OK && atomic_load(&wm.flushed_lsn) == 1000,
          "a commit at 500 is already covered: returns at once");

    copy_in(&wm, 1000, 3000);
    reserve_to(&wm, 3000);
    CHECK(wal_log_flush(&wm, 2000) == MYDB_OK && atomic_load(&wm.flushed_lsn) == 3000,
          "a commit at 2000 syncs everything written, up to 3000 (group commit)");
    CHECK(disk_matches(&wm, wm.curr_slot, 0, 3000), "LSN 0..3000 in the file");

    copy_in(&wm, 3000, 3500);
    reserve_to(&wm, 3500);
    int fd = wm.pool.slots[wm.curr_slot].fd;
    wm.pool.slots[wm.curr_slot].fd = -1;
    CHECK(wal_log_flush(&wm, 3500) == MYDB_ERR &&
          atomic_load(&wm.flushed_lsn) == 3000 && atomic_load(&wm.write_lsn) == 3000,
          "a failed write: MYDB_ERR, flushed_lsn and write_lsn stay at 3000");
    wm.pool.slots[wm.curr_slot].fd = fd;
    CHECK(wal_log_flush(&wm, 3500) == MYDB_OK && atomic_load(&wm.flushed_lsn) == 3500,
          "both locks were released: the next commit writes and syncs 3500");
    done(&wm);

    /* Across a segment's end: the worker syncs the old segment, the
     * commit syncs the new one and waits for the worker. */
    CHECK(fresh(&wm), "fresh log");
    uint32_t seg0 = wm.curr_slot;
    CHECK(advance_to(&wm, WAL_SEG_SIZE - 100), "written up to 100 bytes before segment 0's end");
    copy_in(&wm, WAL_SEG_SIZE - 100, WAL_SEG_SIZE + 300);
    reserve_to(&wm, WAL_SEG_SIZE + 300);
    CHECK(wal_log_flush(&wm, WAL_SEG_SIZE + 300) == MYDB_OK &&
          atomic_load(&wm.flushed_lsn) == WAL_SEG_SIZE + 300 &&
          wm.pool.slots[seg0].header.state == SEG_DONE,
          "a commit across the segment's end: segment 0 DONE, flushed_lsn 16 MB + 300");
    CHECK(atomic_load(&wm.worker.synced_lsn) >= WAL_SEG_SIZE,
          "the commit returned only after the worker had synced segment 0");
    done(&wm);

    /* A worker that has failed: the commit that needs it fails too. */
    CHECK(fresh(&wm), "fresh log");
    CHECK(advance_to(&wm, WAL_SEG_SIZE - 100), "written up to 100 bytes before segment 0's end");
    atomic_store(&wm.worker.io_error, true);         /* it serves no more requests */
    copy_in(&wm, WAL_SEG_SIZE - 100, WAL_SEG_SIZE + 300);
    reserve_to(&wm, WAL_SEG_SIZE + 300);
    CHECK(wal_log_flush(&wm, WAL_SEG_SIZE + 300) == MYDB_ERR &&
          atomic_load(&wm.flushed_lsn) < WAL_SEG_SIZE,
          "segment 0 never synced: the commit returns MYDB_ERR, flushed_lsn not past it");
    done(&wm);
}

typedef struct {
    WalManager   *wm;
    uint64_t      target;
    int           rc;
    _Atomic bool  done;
} LockJob;

static void *space_wait_job(void *arg)
{
    LockJob *j = arg;
    j->rc = wal_flush_space_wait(j->wm, j->target);
    atomic_store(&j->done, true);
    return NULL;
}

static void *log_write_job(void *arg)
{
    LockJob *j = arg;
    j->rc = wal_log_write(j->wm, j->target);
    atomic_store(&j->done, true);
    return NULL;
}

static bool done_within(LockJob *j, int ms)
{
    for (int i = 0; i < ms && !atomic_load(&j->done); i++) usleep(1000);
    return atomic_load(&j->done);
}

static bool has_waiter(WalGroupCommitLock *l)
{
    pthread_mutex_lock(&l->mtx);
    bool any = l->waiters != NULL;
    pthread_mutex_unlock(&l->mtx);
    return any;
}

/* A leader waiting out a hole wakes the waiters its publish covered,
 * while the hole is still open. */
static void test_hole_wait_wakes_covered(void)
{
    printf("\n[test_hole_wait_wakes_covered]\n");
    WalManager wm;
    CHECK(fresh(&wm), "fresh log");

    /* Held by the test, so the space waiter must sleep. */
    CHECK(wal_gcl_acquire(&wm.write_lock, 1500) == WAL_GCL_ACQUIRED, "the test holds write_lock");
    LockJob waiter = { .wm = &wm, .target = 1000 };
    pthread_t tw, tl;
    pthread_create(&tw, NULL, space_wait_job, &waiter);
    bool asleep = false;
    for (int i = 0; i < 2000 && !(asleep = has_waiter(&wm.write_lock)); i++) usleep(1000);
    CHECK(asleep && !atomic_load(&waiter.done), "a space waiter for 1000 sleeps on write_lock");

    /* A hole at [1100, 1200): block 2 (1024..1536) has 76 + 300 = 376. */
    copy_in(&wm, 0, 1100);
    copy_in(&wm, 1200, 1500);
    reserve_to(&wm, 1500);
    LockJob leader = { .wm = &wm, .target = 1500 };
    pthread_create(&tl, NULL, log_write_job, &leader);   /* leads in the test's place */

    CHECK(done_within(&waiter, 2000) && waiter.rc == MYDB_OK,
          "the write step publishes 1024, which covers 1000: the waiter is woken during the hole wait");
    CHECK(!done_within(&leader, 100) && atomic_load(&wm.write_lsn) == 1024,
          "the leader still waits for the hole, with write_lsn at 1024");

    copy_in(&wm, 1100, 1200);
    CHECK(done_within(&leader, 2000) && leader.rc == MYDB_OK && atomic_load(&wm.write_lsn) == 1500,
          "the hole filled: the leader writes up to 1500 and releases");
    if (!atomic_load(&leader.done)) copy_in(&wm, 1100, 1200);   /* never reached when it passes */
    pthread_join(tw, NULL);
    pthread_join(tl, NULL);
    CHECK(disk_matches(&wm, wm.curr_slot, 0, 1500), "LSN 0..1500 in the file");
    done(&wm);
}

/* ------------------------------------------------------------------
 * Real records through wal_insert, read back from the segment files the
 * way recovery would.
 * ------------------------------------------------------------------ */
#define CI_SESSIONS   8
#define CI_MAX_REC    (44 + 12000)
#define CI_MAX_RECS   20000
#define CI_DISK_SIZE  (32u << 20)

typedef struct {
    WalManager   *wm;
    uint32_t      sid;
    uint32_t      bytes;          /* to insert in all                       */
    uint32_t      commit_every;   /* log_flush every n-th batch, 0 never */
    uint64_t     *lsns;           /* record i's LSN, from its filled header */
    uint32_t     *lens;
    uint32_t      n_rec;
    uint64_t      max_end;
    bool          ok;
    _Atomic bool  done;
} CiSession;

/* Record seq of session sid, as a caller hands it over: page_no = seq and
 * table_id = sid say whose it is, the body is a pattern of both. */
static void ci_record(uint8_t *rec, uint32_t len, uint32_t sid, uint32_t seq)
{
    uint32_t body_len = len - WAL_RECORD_HEADER_SIZE;
    uint8_t *body = rec + WAL_RECORD_HEADER_SIZE;
    for (uint32_t j = 0; j < body_len; j++) body[j] = (uint8_t)(j * 13 + seq * 7 + sid);

    WalRecordHeader h = {0};
    h.lsn       = UINT64_MAX;
    h.prev_lsn  = UINT64_MAX;
    h.txn_id    = sid + 1;
    h.total_len = len;
    h.table_id  = sid;
    h.page_no   = seq;
    h.rec_type  = WAL_REC_INSERT;
    h.checksum  = body_len ? crc32c_update(CRC32C_INIT, body, body_len) : CRC32C_INIT;
    memcpy(rec, &h, WAL_RECORD_HEADER_SIZE);
}

/* One session: batches of 1-3 records, mostly small, every 17th up to
 * 12 KB (across pages), until it has inserted s->bytes; commits every
 * commit_every-th batch and checks flushed_lsn covers it after. */
static void *ci_session(void *arg)
{
    CiSession *s   = arg;
    uint8_t   *buf = malloc(3 * CI_MAX_REC);
    uint64_t   prev = 0, wc = 0, total = 0;   /* the session's prev_lsn and write_lsn copy */
    s->ok = (buf != NULL);

    for (uint32_t k = 0; s->ok && total < s->bytes; k++) {
        if (s->n_rec + 3 > CI_MAX_RECS) { s->ok = false; break; }
        uint32_t n = 1 + k % 3, off = 0, first = s->n_rec;
        for (uint32_t i = 0; i < n; i++) {
            uint32_t r   = s->sid * 7919u + s->n_rec * 131u;
            uint32_t len = (s->n_rec % 17 == 5) ? 44 + r % 12000 : 44 + r % 700;
            ci_record(buf + off, len, s->sid, s->n_rec);
            s->lens[s->n_rec++] = len;
            off += len;
        }

        uint64_t last, end;
        if (wal_insert(s->wm, buf, off, prev, &wc, &last, &end) != MYDB_OK) { s->ok = false; break; }
        off = 0;
        for (uint32_t i = first; i < s->n_rec; i++) {
            WalRecordHeader h;
            memcpy(&h, buf + off, WAL_RECORD_HEADER_SIZE);
            s->lsns[i] = h.lsn;
            off += s->lens[i];
        }
        if (last != s->lsns[s->n_rec - 1] || end != wal_lsn_advance(last, s->lens[s->n_rec - 1]))
            s->ok = false;
        if (s->commit_every && k % s->commit_every == 0 &&
            (wal_log_flush(s->wm, end) != MYDB_OK || atomic_load(&s->wm->flushed_lsn) < end))
            s->ok = false;
        if (end > s->max_end) s->max_end = end;
        prev   = last;
        total += off;
    }
    free(buf);
    atomic_store(&s->done, true);
    return NULL;
}

/* Copies n record bytes from the disk image starting at lsn, hopping
 * page headers; returns the LSN just past them. */
static uint64_t disk_read(const uint8_t *disk, uint64_t lsn, uint8_t *dst, uint32_t n)
{
    while (n) {
        if (wal_lsn_offset(lsn) == 0) lsn += WAL_PAGE_HEADER_SIZE;
        uint32_t take = WAL_PAGE_SIZE - wal_lsn_offset(lsn);
        if (take > n) take = n;
        memcpy(dst, disk + lsn, take);
        dst += take;
        n   -= take;
        lsn += take;
    }
    return lsn;
}

/* Reads the log up to end_lsn back from the segment files (segment seg in
 * slot seg % WAL_SEGMENT_POOL_SLOTS, as claim_next puts it) and walks it from LSN 24 by
 * total_len: every record is one the sessions inserted, at the LSN they
 * were given, in each session's order, with valid CRCs and page headers. */
static void verify_log(WalManager *wm, CiSession *s, uint32_t n_sess, uint64_t end_lsn)
{
    uint8_t  *disk = calloc(1, CI_DISK_SIZE);
    uint8_t  *rec  = malloc(CI_MAX_REC);
    uint32_t  pages = (uint32_t)((end_lsn + WAL_PAGE_SIZE - 1) >> WAL_PAGE_SHIFT);
    uint16_t *exp_cont = calloc(pages, sizeof(uint16_t));
    bool      read_ok = disk && rec && exp_cont && end_lsn <= CI_DISK_SIZE;

    for (uint64_t pg = 0; read_ok && pg < pages; pg++) {
        uint64_t lsn = pg << WAL_PAGE_SHIFT;
        uint32_t slot = (uint32_t)(wal_lsn_segment(lsn) % WAL_SEGMENT_POOL_SLOTS);
        if (wal_segment_pool_read_page(&wm->pool, slot, wal_lsn_page(lsn) + 1, disk + lsn) != MYDB_OK)
            read_ok = false;
    }
    CHECK(read_ok, "the log read back from the segment files");

    uint32_t next_seq[CI_SESSIONS] = {0};
    bool     walk_ok = read_ok, owner_ok = true, body_ok = true;
    uint64_t lsn = WAL_FIRST_RECORD_LSN;
    while (walk_ok && lsn < end_lsn) {
        if (wal_lsn_offset(lsn) == 0) lsn += WAL_PAGE_HEADER_SIZE;
        uint32_t total_len;
        disk_read(disk, lsn, rec, WAL_RECORD_HEADER_SIZE);
        memcpy(&total_len, rec + offsetof(WalRecordHeader, total_len), 4);
        if (total_len < WAL_RECORD_HEADER_SIZE || total_len > CI_MAX_REC) { walk_ok = false; break; }
        uint64_t next = disk_read(disk, lsn, rec, total_len);

        WalRecordHeader h;
        if (wal_record_header_deserialize(rec, total_len - WAL_RECORD_HEADER_SIZE, &h) != MYDB_OK) {
            walk_ok = false;
            break;
        }
        uint32_t sid = h.table_id, seq = h.page_no;
        if (sid >= n_sess || seq != next_seq[sid] || seq >= s[sid].n_rec ||
            h.lsn != lsn || s[sid].lsns[seq] != lsn || s[sid].lens[seq] != total_len ||
            h.prev_lsn != (seq ? s[sid].lsns[seq - 1] : 0)) {
            owner_ok = false;
        } else {
            next_seq[sid]++;
            for (uint32_t j = 0; j < total_len - WAL_RECORD_HEADER_SIZE; j++)
                if (rec[WAL_RECORD_HEADER_SIZE + j] != (uint8_t)(j * 13 + seq * 7 + sid)) body_ok = false;
        }

        /* Each page that starts inside this record continues it. */
        for (uint64_t pb = (lsn & ~(uint64_t)(WAL_PAGE_SIZE - 1)) + WAL_PAGE_SIZE; pb < next; pb += WAL_PAGE_SIZE) {
            uint64_t before = wal_lsn_to_byte(pb + WAL_PAGE_HEADER_SIZE) - wal_lsn_to_byte(lsn);
            uint64_t rest   = total_len - before;
            exp_cont[pb >> WAL_PAGE_SHIFT] = (uint16_t)(rest < WAL_PAGE_USABLE ? rest : WAL_PAGE_USABLE);
        }
        lsn = next;
    }
    for (uint32_t i = 0; i < n_sess; i++) if (next_seq[i] != s[i].n_rec) owner_ok = false;
    CHECK(walk_ok && lsn == end_lsn,
          "the files hold one unbroken log from LSN 24 to the end: every record found by total_len, every CRC valid");
    CHECK(owner_ok, "each session's records are all there, once, in its order, at the LSNs it was given, prev_lsn chained");
    CHECK(body_ok, "every record's body is the bytes its session wrote");

    bool pages_ok = walk_ok;
    for (uint32_t p = 0; pages_ok && p < pages; p++) {
        WalPageHeader ph;
        if (wal_page_header_deserialize(disk + (uint64_t)p * WAL_PAGE_SIZE, &ph) != MYDB_OK ||
            ph.start_lsn != (uint64_t)p * WAL_PAGE_SIZE || ph.continuation_len != exp_cont[p] ||
            ph.flags != (exp_cont[p] ? WAL_PAGE_INCOMING_CONTINUATION : 0))
            pages_ok = false;
    }
    CHECK(pages_ok, "every page header: start_lsn, continuation_len and flag");

    free(exp_cont);
    free(rec);
    free(disk);
}

/* Runs n sessions to the end; false if one could not start or one did not
 * finish within 60 s (stuck waiting), which then ends the whole test. */
static bool run_sessions(CiSession *s, uint32_t n)
{
    pthread_t th[CI_SESSIONS];
    uint32_t  started = 0;
    for (; started < n; started++)
        if (pthread_create(&th[started], NULL, ci_session, &s[started]) != 0) break;
    if (started < n) {
        for (uint32_t i = 0; i < started; i++) pthread_join(th[i], NULL);
        return false;
    }
    bool all = false;
    for (int t = 0; t < 60000 && !all; t++) {
        all = true;
        for (uint32_t i = 0; i < n; i++) if (!atomic_load(&s[i].done)) all = false;
        if (!all) usleep(1000);
    }
    if (!all) {
        CHECK(0, "every session finished within 60 s (none stuck)");
        printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
        exit(1);
    }
    for (uint32_t i = 0; i < n; i++) pthread_join(th[i], NULL);
    return true;
}

static bool sessions_setup(CiSession *s, uint32_t n, WalManager *wm, uint32_t bytes, uint32_t commit_every)
{
    bool mem = true;
    for (uint32_t i = 0; i < n; i++) {
        s[i] = (CiSession){ .wm = wm, .sid = i, .bytes = bytes, .commit_every = commit_every,
                            .lsns = malloc(CI_MAX_RECS * sizeof(uint64_t)),
                            .lens = malloc(CI_MAX_RECS * sizeof(uint32_t)) };
        if (!s[i].lsns || !s[i].lens) mem = false;
    }
    return mem;
}

static void sessions_free(CiSession *s, uint32_t n)
{
    for (uint32_t i = 0; i < n; i++) { free(s[i].lsns); free(s[i].lens); }
}

/* The log's end: every byte reserved, as an end (a page end stays one). */
static uint64_t log_end(WalManager *wm)
{
    return wal_lsn_advance(WAL_FIRST_RECORD_LSN, atomic_load(&wm->current_pos));
}

/* One session fills the ring with no commit: the insert that needs the
 * next lap's space writes the ring out itself, and never syncs. */
static void test_space_wait_writes_ring(void)
{
    printf("\n[test_space_wait_writes_ring]\n");
    WalManager wm;
    CiSession  s;
    const uint64_t R = (uint64_t)WAL_RING_FRAMES * WAL_PAGE_SIZE;
    CHECK(fresh(&wm) && sessions_setup(&s, 1, &wm, (uint32_t)(R + R / 2), 0),
          "fresh log, one session to insert 1.5 rings");

    s.sid = 0;
    CHECK(run_sessions(&s, 1) && s.ok, "every insert returned OK, its LSNs matching its headers");
    uint64_t end = log_end(&wm);
    CHECK(end > R && atomic_load(&wm.write_lsn) > 0 && atomic_load(&wm.write_lsn) + R + WAL_BLOCK_SIZE >= end,
          "past one ring: the inserts wrote the ring themselves, write_lsn within a ring of the end");
    uint64_t fl = atomic_load(&wm.flushed_lsn);
    CHECK(fl % WAL_SEG_SIZE == 0 && fl <= atomic_load(&wm.write_lsn),
          "a space waiter never fsyncs: flushed_lsn only at a finished segment's end, from the worker");

    CHECK(wal_log_flush(&wm, end) == MYDB_OK && atomic_load(&wm.flushed_lsn) == end,
          "one commit at the end writes and syncs the rest");
    verify_log(&wm, &s, 1, end);
    sessions_free(&s, 1);
    done(&wm);
}

/* Eight sessions insert 2.5 rings with no commit and no writer thread:
 * every write is done by a space waiter, under write_lock. */
static void test_concurrent_space_waiters(void)
{
    printf("\n[test_concurrent_space_waiters]\n");
    WalManager wm;
    CiSession  s[CI_SESSIONS];
    const uint64_t R = (uint64_t)WAL_RING_FRAMES * WAL_PAGE_SIZE;
    CHECK(fresh(&wm) && sessions_setup(s, CI_SESSIONS, &wm, (uint32_t)(R * 5 / 16), 0),
          "fresh log, 8 sessions of 5/16 of a ring each");

    CHECK(run_sessions(s, CI_SESSIONS), "8 session threads ran to the end");
    bool calls_ok = true;
    uint64_t max_end = 0;
    for (uint32_t i = 0; i < CI_SESSIONS; i++) {
        if (!s[i].ok) calls_ok = false;
        if (s[i].max_end > max_end) max_end = s[i].max_end;
    }
    uint64_t end = log_end(&wm);
    CHECK(calls_ok && max_end == end, "every call OK, its LSNs match its headers, the latest end is the log's end");
    CHECK(end > 2 * R, "more than two rings of log went through the ring");
    CHECK(atomic_load(&wm.write_lsn) + R + WAL_BLOCK_SIZE >= end && atomic_load(&wm.write_lsn) > R,
          "the space waiters wrote the ring themselves: write_lsn past one ring, within a ring of the end");

    CHECK(wal_log_flush(&wm, end) == MYDB_OK &&
          atomic_load(&wm.write_lsn) == end && atomic_load(&wm.flushed_lsn) == end,
          "a last commit writes and syncs everything");
    uint32_t tail_idx = (uint32_t)((end >> WAL_BLOCK_SHIFT) & wm.ctr_mask);
    bool zeroed = true;
    for (uint32_t i = 0; i <= wm.ctr_mask; i++)
        if (i != tail_idx && ctr(&wm, i) != 0) zeroed = false;
    CHECK(zeroed && ctr(&wm, tail_idx) == (end & (WAL_BLOCK_SIZE - 1)),
          "every counter is zero but the tail block's, which holds exactly its bytes");
    verify_log(&wm, s, CI_SESSIONS, end);
    sessions_free(s, CI_SESSIONS);
    done(&wm);
}

/* Eight sessions insert and commit every batch: commit leaders and their
 * followers on flush_lock, writes through write_lock, space waiters when
 * the ring fills. */
static void test_concurrent_commits(void)
{
    printf("\n[test_concurrent_commits]\n");
    WalManager wm;
    CiSession  s[CI_SESSIONS];
    CHECK(fresh(&wm) && sessions_setup(s, CI_SESSIONS, &wm, 1u << 19, 1), "fresh log, 8 sessions of 512 KB");

    CHECK(run_sessions(s, CI_SESSIONS), "8 session threads ran to the end");
    bool calls_ok = true;
    for (uint32_t i = 0; i < CI_SESSIONS; i++) if (!s[i].ok) calls_ok = false;
    CHECK(calls_ok, "every insert and every commit OK, flushed_lsn past each commit's end when it returned");
    uint64_t end = log_end(&wm);
    CHECK(atomic_load(&wm.flushed_lsn) == end && atomic_load(&wm.write_lsn) == end,
          "the last commits left everything written and synced");
    verify_log(&wm, s, CI_SESSIONS, end);
    sessions_free(s, CI_SESSIONS);
    done(&wm);
}

int main(void)
{
    printf("=== test_wal_flush ===\n");

    test_write_basic();
    test_write_wraps_ring();
    test_write_full_ring_and_lazy_rollover();
    test_write_splits_at_segment_end();
    test_write_splits_at_segment_end_inside_ring();
    test_write_io_error();
    test_log_flush();
    test_hole_wait_wakes_covered();
    test_space_wait_writes_ring();
    test_concurrent_space_waiters();
    test_concurrent_commits();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
