#include "wal_segment_pool.h"
#include "wal_page.h"
#include "common.h"

/* wal_page.h's LSN arithmetic assumes the segment size this pool uses. */
_Static_assert(WAL_SEG_SIZE == WAL_SEGMENT_FILE_SIZE, "WAL_SEG_SIZE must match WAL_SEGMENT_FILE_SIZE");

#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>

/* ------------------------------------------------------------------
 * Internal helpers — same shape as disk_manager.c's pwrite_all/
 * pread_all (each on-disk-I/O file in this codebase keeps its own
 * static copy rather than sharing one; ensure_dir mirrors the existing
 * copies already duplicated in engine/src/engine.c and
 * engine/src/stats_buffer.c).
 * ------------------------------------------------------------------ */

static int pwrite_all(int fd, const void *buf, size_t n, off_t offset)
{
    ssize_t written = pwrite(fd, buf, n, offset);
    return (written == (ssize_t)n) ? MYDB_OK : MYDB_ERR;
}

static int pread_all(int fd, void *buf, size_t n, off_t offset)
{
    ssize_t got = pread(fd, buf, n, offset);
    return (got == (ssize_t)n) ? MYDB_OK : MYDB_ERR;
}

/* mkdir, ignoring EEXIST. Returns 0 if the directory now exists. */
static int ensure_dir(const char *path)
{
    if (mkdir(path, 0755) == 0) return 0;
    if (errno == EEXIST) {
        struct stat st;
        if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) return 0;
    }
    return -1;
}

static int page_no_valid(uint32_t page_no)
{
    return page_no >= 1 && page_no < WAL_SEGMENT_PAGES_PER_FILE;
}

/* ------------------------------------------------------------------
 * Per-slot open-or-create.
 *
 * A segment file's page-0 slot is a full WAL_PAGE_SIZE page (not just
 * WAL_SEGMENT_HEADER_ON_DISK_SIZE bytes) — 64 meaningful bytes followed
 * by zero padding to fill the page, same "page N starts at N *
 * WAL_PAGE_SIZE" convention every other on-disk format in this codebase
 * follows (disk_manager.h's FileHeader = page 0, data starts at page 1).
 * Real WAL pages therefore start at page_no = 1.
 * ------------------------------------------------------------------ */
static int open_existing_or_create_slot(WalSegmentPool *pool, uint32_t slot_no)
{
    char path[300];
    snprintf(path, sizeof(path), "%s/wal_%u.mydb", pool->wal_dir, slot_no);

    WalSlot *slot = &pool->slots[slot_no];
    slot->slot_no = (uint8_t)slot_no;
    slot->fd = -1;

    int fd = open(path, O_RDWR);
    if (fd >= 0) {
        /* Existing segment file from a prior run — reload its header. */
        uint8_t page_buf[WAL_PAGE_SIZE];
        if (pread_all(fd, page_buf, WAL_PAGE_SIZE, 0) != MYDB_OK) {
            close(fd);
            return MYDB_ERR;
        }
        int rc = wal_segment_header_deserialize(page_buf, &slot->header);
        if (rc != MYDB_OK) {
            close(fd);
            return rc;
        }
        slot->fd = fd;
        return MYDB_OK;
    }
    if (errno != ENOENT) return MYDB_ERR;

    /* Doesn't exist yet — create, pre-allocate, write an initial
     * SEG_FREE header. posix_fallocate() (portable POSIX call) stands in
     * for the design doc's "via fallocate()" — Linux fallocate() needs
     * _GNU_SOURCE, which nothing else in this codebase defines;
     * posix_fallocate() gives the same practical guarantee (blocks
     * reserved up front, no later ENOSPC surprise) without it. */
    fd = open(path, O_RDWR | O_CREAT | O_EXCL, 0644);
    if (fd < 0) return MYDB_ERR;

    if (posix_fallocate(fd, 0, WAL_SEGMENT_FILE_SIZE) != 0) {
        close(fd);
        return MYDB_ERR;
    }

    WalSegmentHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.id.file_type = FILETYPE_WAL_SEGMENT;
    hdr.segment_no   = 0;   /* placeholder — the real value is stamped at claim time */
    hdr.partition_id = pool->partition_id;
    hdr.state        = SEG_FREE;

    uint8_t page_buf[WAL_PAGE_SIZE];
    memset(page_buf, 0, WAL_PAGE_SIZE);
    wal_segment_header_serialize(&hdr, page_buf);

    if (pwrite_all(fd, page_buf, WAL_PAGE_SIZE, 0) != MYDB_OK) {
        close(fd);
        return MYDB_ERR;
    }
    if (fdatasync(fd) < 0) {
        close(fd);
        return MYDB_ERR;
    }

    /* Read back rather than trust the local hdr — picks up the real
     * checksum wal_segment_header_serialize stamped into page_buf. */
    wal_segment_header_deserialize(page_buf, &slot->header);
    slot->fd = fd;
    return MYDB_OK;
}

/* ------------------------------------------------------------------
 * Lifecycle
 * ------------------------------------------------------------------ */

int wal_segment_pool_init(WalSegmentPool *pool, const char *wal_dir, uint32_t partition_id)
{
    if (!pool || !wal_dir) return MYDB_ERR;
    if (ensure_dir(wal_dir) != 0) return MYDB_ERR;

    memset(pool, 0, sizeof(*pool));
    snprintf(pool->wal_dir, sizeof(pool->wal_dir), "%s", wal_dir);
    pool->partition_id = partition_id;

    for (uint32_t i = 0; i < WAL_SEGMENT_POOL_SLOTS; i++) {
        int rc = open_existing_or_create_slot(pool, i);
        if (rc != MYDB_OK) {
            /* Close whatever we already opened before bailing out. */
            for (uint32_t j = 0; j < i; j++) {
                if (pool->slots[j].fd >= 0) close(pool->slots[j].fd);
            }
            return rc;
        }
    }

    /* Any slot reloaded SEG_ACTIVE has a stale on-disk data_pages by
     * definition (impl doc §8.4) — recover the true count now. */
    for (uint32_t i = 0; i < WAL_SEGMENT_POOL_SLOTS; i++) {
        if (pool->slots[i].header.state == SEG_ACTIVE) {
            uint32_t data_pages = 0;
            if (wal_segment_pool_tail_scan(pool, i, &data_pages) == MYDB_OK)
                pool->slots[i].header.data_pages = data_pages;
        }
    }

    /* Reseed the shared claim counter from whatever's on disk — see
     * wal_segment_pool.h's header comment for why this scheme (one
     * pool-level counter, assigned at claim time) stands in for the
     * design doc's Archiver-driven free-time stamping. Counts every slot
     * that was ever claimed (start_lsn != 0), FREE included, not just the
     * non-FREE ones: the segment number is part of the LSN, so it must
     * never drop back (T-010). */
    uint64_t max_used = 0;
    int      any_used = 0;
    for (uint32_t i = 0; i < WAL_SEGMENT_POOL_SLOTS; i++) {
        if (pool->slots[i].header.state != SEG_FREE ||
            pool->slots[i].header.start_lsn != 0) {
            if (!any_used || pool->slots[i].header.segment_no > max_used)
                max_used = pool->slots[i].header.segment_no;
            any_used = 1;
        }
    }
    pool->next_segment_no = any_used ? (max_used + 1) : 0;

    return MYDB_OK;
}

int wal_segment_pool_shutdown(WalSegmentPool *pool)
{
    if (!pool) return MYDB_ERR;
    for (uint32_t i = 0; i < WAL_SEGMENT_POOL_SLOTS; i++) {
        if (pool->slots[i].fd >= 0) {
            close(pool->slots[i].fd);
            pool->slots[i].fd = -1;
        }
    }
    return MYDB_OK;
}

/* ------------------------------------------------------------------
 * Claiming
 * ------------------------------------------------------------------ */

int wal_segment_pool_claim_next(WalSegmentPool *pool, uint32_t *out_slot_index)
{
    if (!pool || !out_slot_index) return MYDB_ERR;

    uint32_t target = (uint32_t)(pool->next_segment_no % WAL_SEGMENT_POOL_SLOTS);
    WalSlot *slot = &pool->slots[target];

    if (slot->header.state != SEG_FREE) return MYDB_ERR;

    /* Mutate a local copy, not slot->header: if the write below fails we
     * must leave the in-memory header exactly as it was, or memory and
     * disk disagree about this slot's state and every later decision
     * trusts the wrong one. slot->header is only committed once the
     * write has actually succeeded. */
    WalSegmentHeader next = slot->header;
    next.segment_no   = pool->next_segment_no;
    next.start_lsn    = wal_segment_first_lsn(pool->next_segment_no);   /* T-039 */
    next.end_lsn      = 0;
    next.data_pages   = 0;
    next.state        = SEG_ACTIVE;
    next.partition_id = pool->partition_id;

    uint8_t page_buf[WAL_PAGE_SIZE];
    memset(page_buf, 0, WAL_PAGE_SIZE);
    wal_segment_header_serialize(&next, page_buf);

    if (pwrite_all(slot->fd, page_buf, WAL_PAGE_SIZE, 0) != MYDB_OK) return MYDB_ERR;
    /* No fdatasync here — see the header's doc comment for why claim_next
     * never needs one. */

    wal_segment_header_deserialize(page_buf, &slot->header);

    pool->next_segment_no++;
    *out_slot_index = target;
    return MYDB_OK;
}

int wal_segment_pool_mark_done(WalSegmentPool *pool, WalWorker *worker,
                               uint32_t slot_index, uint64_t end_lsn, uint32_t data_pages,
                               uint64_t *out_ticket)
{
    if (out_ticket) *out_ticket = 0;
    if (!pool || slot_index >= WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;

    WalSlot *slot = &pool->slots[slot_index];
    if (slot->header.state != SEG_ACTIVE) return MYDB_ERR;

    /* Local copy, committed to slot->header only after the write and
     * sync both succeed — see claim_next's own note. */
    WalSegmentHeader next = slot->header;
    next.end_lsn    = end_lsn;
    next.data_pages = data_pages;
    next.state      = SEG_DONE;

    uint8_t page_buf[WAL_PAGE_SIZE];
    memset(page_buf, 0, WAL_PAGE_SIZE);
    wal_segment_header_serialize(&next, page_buf);

    if (pwrite_all(slot->fd, page_buf, WAL_PAGE_SIZE, 0) != MYDB_OK) return MYDB_ERR;
    if (worker) {
        /* out_ticket is written by async_fdatasync itself, in the same
         * call that posts -- so it is set before any later step here can
         * fail. That ordering is the point: a mark_done that posts and
         * then returns an error would otherwise leave its caller with no
         * ticket to drain with, and an undrained mailbox blocks the next
         * post forever, for the whole partition. */
        if (wal_worker_async_fdatasync(worker, slot->fd, out_ticket) != MYDB_OK) return MYDB_ERR;
    } else {
        if (fdatasync(slot->fd) < 0) return MYDB_ERR;
    }

    wal_segment_header_deserialize(page_buf, &slot->header);
    return MYDB_OK;
}

/* ------------------------------------------------------------------
 * Raw page I/O
 * ------------------------------------------------------------------ */

int wal_segment_pool_write_page(WalSegmentPool *pool, uint32_t slot_index,
                                 uint32_t page_no, const uint8_t *buf)
{
    if (!pool || !buf || slot_index >= WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;
    if (!page_no_valid(page_no)) return MYDB_ERR;

    off_t offset = (off_t)page_no * WAL_PAGE_SIZE;
    return pwrite_all(pool->slots[slot_index].fd, buf, WAL_PAGE_SIZE, offset);
}

int wal_segment_pool_read_page(WalSegmentPool *pool, uint32_t slot_index,
                                uint32_t page_no, uint8_t *out_buf)
{
    if (!pool || !out_buf || slot_index >= WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;
    if (!page_no_valid(page_no)) return MYDB_ERR;

    off_t offset = (off_t)page_no * WAL_PAGE_SIZE;
    return pread_all(pool->slots[slot_index].fd, out_buf, WAL_PAGE_SIZE, offset);
}

int wal_segment_pool_read_segment(WalSegmentPool *pool, uint32_t slot_index,
                                   uint8_t *out_buf)
{
    if (!pool || !out_buf || slot_index >= WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;
    return pread_all(pool->slots[slot_index].fd, out_buf, WAL_SEGMENT_FILE_SIZE, 0);
}

/* ------------------------------------------------------------------
 * wal_segment_pool_write — the Flusher-facing abstraction. A blind byte
 * mover: never parses or constructs WalPageHeader content (the Flusher's
 * buf already carries whatever headers/LSNs it needs, copied straight
 * from ring-buffer frames — impl doc §8.6). Only knows page/segment
 * *geometry*. slot_index/page_no/offset are the caller's own segment-
 * side cursor, purely positional — never derived from anything inside
 * buf (see wal_segment_pool.h for the full contract, including why page
 * numbers never need reconciling between the ring buffer and the
 * segment: WalPageHeader carries no page_no field at all).
 * ------------------------------------------------------------------ */

int wal_segment_pool_write(WalSegmentPool *pool, WalWorker *worker,
                            uint32_t *slot_index, uint32_t *page_no, uint32_t *offset,
                            const uint8_t *buf, size_t buf_len)
{
    if (!pool || !slot_index || !page_no || !offset || !buf) return MYDB_ERR;
    if (*slot_index >= WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;
    if (!page_no_valid(*page_no)) return MYDB_ERR;
    if (*offset >= WAL_PAGE_SIZE) return MYDB_ERR;

    /* Single exit through `out:` from here on. Once mark_done() below has
     * handed a flush to worker, EVERY return path has to drain it:
     * wal_worker_wait() is the only thing that clears the mailbox's done
     * flag, so returning early without it wedges the next
     * wal_worker_async_fdatasync() forever — for the whole partition,
     * not just this call. */
    int rc = MYDB_OK;

    /* Names the fsync mark_done() handed to worker, if this call rolled
     * over at all. Stays 0 otherwise, and wal_worker_wait() treats 0 as
     * "I posted nothing" -- which is what lets the drain at `out:` stay
     * unconditional without ever collecting the OTHER caller thread's
     * result. Both WAL paths share one WalWorker (wal_manager.h). */
    uint64_t flush_ticket = 0;

    size_t written = 0;
    while (written < buf_len) {
        uint32_t room_left = WAL_PAGE_SIZE - *offset;
        size_t   remaining = buf_len - written;
        uint32_t chunk = (remaining < (size_t)room_left) ? (uint32_t)remaining : room_left;

        off_t abs_offset = (off_t)(*page_no) * WAL_PAGE_SIZE + *offset;
        if (pwrite_all(pool->slots[*slot_index].fd, buf + written, chunk, abs_offset) != MYDB_OK) {
            rc = MYDB_ERR;
            goto out;
        }

        written += chunk;
        *offset += chunk;

        if (*offset < WAL_PAGE_SIZE) {
            /* This page-slot isn't full — every remaining byte must have
             * fit (chunk was bounded by `remaining`, not `room_left`),
             * so the loop is about to exit with the cursor left mid-page
             * for a future call to resume appending into. */
            break;
        }

        /* This page-slot just filled exactly. */
        *offset = 0;

        if (*page_no + 1 >= WAL_SEGMENT_PAGES_PER_FILE) {
            /* The segment is full too. The one non-"blind" step: read
             * the page we just filled back and work out where its data
             * ends — start_lsn + WAL_PAGE_SIZE, since every page the
             * Flusher writes is full. (It used to copy the page's own
             * end_lsn field, dropped in T-039, and then add data_len,
             * dropped after it; see wal_page.h.) A field read, not
             * content interpretation. */
            uint8_t  last_page[WAL_PAGE_SIZE];
            uint64_t seg_end_lsn = 0;
            if (wal_segment_pool_read_page(pool, *slot_index, *page_no, last_page) == MYDB_OK) {
                WalPageHeader hdr;
                if (wal_page_header_deserialize(last_page, &hdr) == MYDB_OK)
                    seg_end_lsn = hdr.start_lsn + WAL_PAGE_SIZE;
            }

            uint32_t finished_slot       = *slot_index;
            uint32_t finished_data_pages = *page_no;
            if (wal_segment_pool_mark_done(pool, worker, finished_slot, seg_end_lsn,
                                           finished_data_pages, &flush_ticket) != MYDB_OK) {
                rc = MYDB_ERR;
                goto out;
            }
            /* mark_done may have just handed a flush to worker — from
             * here on, bailing out without draining it would wedge the
             * mailbox, hence goto rather than return. */
            if (wal_segment_pool_claim_next(pool, slot_index) != MYDB_OK) {
                rc = MYDB_ERR;
                goto out;
            }
            *page_no = 1;
        } else {
            (*page_no)++;
        }
    }

    /* TEMPORARY: fdatasync every call, since there's no Flusher/group-commit
     * yet to batch many writes behind one flush (design doc §11).
     * fdatasync() flushes every dirty page of this fd, not just what this
     * call wrote, so this is correct even after a mid-call segment
     * rollover (the finalized old segment was already fdatasync'd by
     * mark_done() itself; this covers whatever landed in the current
     * *slot_index since). Segment files are posix_fallocate'd to their
     * full fixed size once at creation and never resized again, so
     * fdatasync (which skips flushing metadata not needed for correct
     * data retrieval, e.g. timestamps) is strictly cheaper than fsync
     * here with no durability difference for our data — avoids dragging
     * the filesystem journal into every WAL flush on filesystems like
     * ext4. Remove this one line once the Flusher lands and does its own
     * batched flush at the end of each flush cycle instead. */
    if (fdatasync(pool->slots[*slot_index].fd) < 0) rc = MYDB_ERR;

out:
    /* If this call rolled over, mark_done() may have handed its fsync to
     * worker instead of waiting for it (see mark_done's own doc comment)
     * — confirm it's actually durable before telling our own caller this
     * call succeeded. Cheap no-op if it didn't. Runs unconditionally,
     * including on the error paths above: draining is what keeps the
     * shared mailbox usable, and the worker's own result still counts
     * toward what we report. */
    if (worker && wal_worker_wait(worker, flush_ticket) != MYDB_OK) rc = MYDB_ERR;

    return rc;
}

/* ------------------------------------------------------------------
 * Crash-reload tail-scan
 * ------------------------------------------------------------------ */

int wal_segment_pool_tail_scan(WalSegmentPool *pool, uint32_t slot_index,
                                uint32_t *out_data_pages)
{
    if (!pool || !out_data_pages || slot_index >= WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;

    int      fd    = pool->slots[slot_index].fd;
    uint32_t count = 0;
    uint8_t  buf[WAL_PAGE_SIZE];

    for (uint32_t page_no = 1; page_no < WAL_SEGMENT_PAGES_PER_FILE; page_no++) {
        off_t offset = (off_t)page_no * WAL_PAGE_SIZE;
        if (pread_all(fd, buf, WAL_PAGE_SIZE, offset) != MYDB_OK) break;

        WalPageHeader hdr;
        /* An unwritten (still-zero, sparse) page fails file_header_
         * check_id's magic check inside deserialize — that failure is
         * exactly the stop condition, not an error to report upward. */
        if (wal_page_header_deserialize(buf, &hdr) != MYDB_OK) break;

        count++;
    }

    *out_data_pages = count;
    return MYDB_OK;
}
