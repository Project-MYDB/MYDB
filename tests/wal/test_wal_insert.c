#include <dirent.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "checksum.h"
#include "common.h"
#include "wal_flush.h"
#include "wal_frame_state.h"
#include "wal_insert.h"
#include "wal_page.h"
#include "wal_types.h"

/*
 * test_wal_insert — the V2 insert path (MYDB_WAL_DESIGN_V2.md §9, §10):
 * block counters, reserving, filling record headers, planning a piece
 * and copying it, wal_insert, and the space check where it passes. Also
 * the write step's completeness scan (wal_flush_scan, §10.5), which reads
 * these counters. The space check that has to wait runs the write step,
 * so it is tested in test_wal_flush, on segment files.
 */

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

/* Block counters (V2 §10.1-10.2, Appendix E): a flat array of 16-byte
 * counters, one per 512 B block of the ring, indexed (lsn >> 9) & ctr_mask. */
static void test_block_counter_indexing(void)
{
    printf("\n[test_block_counter_indexing]\n");

    const uint32_t frames   = 1024;                      /* 4 MB ring */
    const uint32_t ctr_mask = frames * WAL_FRAME_BLOCKS - 1;
    CHECK(WAL_BLOCK_SIZE == 512 && WAL_FRAME_BLOCKS == 8, "512 B blocks, 8 per 4 KB frame");
    CHECK(ctr_mask == 8191, "a 4 MB ring has 8192 counters, ctr_mask 8191");
    CHECK((size_t)frames * WAL_FRAME_BLOCKS * sizeof(WalBlockCounter) == 128u * 1024u,
          "their memory is 128 KB, 3.125% of the 4 MB ring");

    static WalBlockCounter c[1024 * WAL_FRAME_BLOCKS];
    CHECK(wal_block_counter(c, ctr_mask, 0)   == &c[0].done &&
          wal_block_counter(c, ctr_mask, 24)  == &c[0].done &&
          wal_block_counter(c, ctr_mask, 511) == &c[0].done,
          "LSN 0..511 (page header + first 488 data bytes) is block 0");
    CHECK(wal_block_counter(c, ctr_mask, 512) == &c[1].done, "LSN 512 starts block 1");
    CHECK(wal_block_counter(c, ctr_mask, 4096) == &c[8].done, "the next frame starts at counter 8");

    uint64_t lsn = (uint64_t)37 * 4096 + 3 * 512 + 100;  /* frame 37, block 3 */
    CHECK(wal_block_counter(c, ctr_mask, lsn) == &c[37 * 8 + 3].done,
          "frame 37, block 3 -> counter 299");

    /* The ring wraps every 4 MB of LSN: one ring length later is the same
     * counter, and a whole segment boundary lines up with counter 0. */
    uint64_t ring_bytes = (uint64_t)frames * 4096;
    CHECK(wal_block_counter(c, ctr_mask, lsn + ring_bytes) == wal_block_counter(c, ctr_mask, lsn),
          "one ring length later reuses the same counter");
    CHECK(wal_block_counter(c, ctr_mask, (uint64_t)5 << 22) == &c[0].done,
          "segment 5's first byte wraps to counter 0");
    CHECK(wal_block_counter(c, ctr_mask, ring_bytes - 1) == &c[8191].done,
          "the ring's last byte is the last counter");

    /* 16-byte stride: 4 counters per 64-byte line, so counters 0-3 share
     * the first line and 4 starts the second (2 KB of ring per line). */
    CHECK((uintptr_t)&c[4] - (uintptr_t)&c[0] == 64, "counter 4 is 64 bytes after counter 0");
}

/* A WalManager with only what the insert path touches: a zeroed 8 MB
 * ring and its counters, in memory (no segment files). */
static void ring_setup(WalManager *wm)
{
    memset(wm, 0, sizeof(*wm));
    wm->ring_buf_size   = WAL_RING_FRAMES;
    wm->ring_frame_mask = WAL_RING_FRAMES - 1;
    wm->ctr_mask        = WAL_RING_FRAMES * WAL_FRAME_BLOCKS - 1;
    wm->ring_buf    = aligned_alloc(4096, (size_t)WAL_RING_FRAMES * 4096);
    wm->fs_counters = aligned_alloc(64, (size_t)WAL_RING_FRAMES * WAL_FRAME_BLOCKS * sizeof(WalBlockCounter));
    memset(wm->ring_buf, 0, (size_t)WAL_RING_FRAMES * 4096);
    memset(wm->fs_counters, 0, (size_t)WAL_RING_FRAMES * WAL_FRAME_BLOCKS * sizeof(WalBlockCounter));
    /* The space check takes write_lock when it has to wait; these tests
     * never let it (no segment files to write to). */
    wal_gcl_init(&wm->write_lock, &wm->write_lsn);
}

static void ring_free(WalManager *wm)
{
    wal_gcl_destroy(&wm->write_lock);
    free(wm->ring_buf);
    free(wm->fs_counters);
}

static uint16_t ctr(WalManager *wm, uint32_t i) { return atomic_load(&wm->fs_counters[i].done); }

/* A batch in buf: records of the given total lengths back to back, each
 * header's total_len filled (the plan reads it), every byte otherwise a
 * pattern of its buffer offset so a copy can be checked byte for byte. */
static uint32_t make_batch(uint8_t *buf, const uint32_t *lens, int n)
{
    uint32_t off = 0;
    for (int i = 0; i < n; i++) {
        for (uint32_t j = 0; j < lens[i]; j++) buf[off + j] = (uint8_t)((off + j) * 7 + 1);
        memcpy(buf + off + offsetof(WalRecordHeader, total_len), &lens[i], 4);
        off += lens[i];
    }
    return off;
}

/* Plans and copies a whole batch, with the loop wal_insert.h shows;
 * returns the number of pieces. */
static int insert_batch(WalManager *wm, uint64_t lsn, const uint8_t *buf, uint32_t len)
{
    WalByteInsert p;
    int pieces = 0;
    uint32_t off = 0;
    while (off < len) {
        wal_insert_plan(wm, &p, lsn, buf, len, off);
        wal_insert_copy(wm, &p);
        pieces++;
        off += p.len;
        lsn += p.len + WAL_PAGE_HEADER_SIZE;
    }
    return pieces;
}

/* The loop's step, for tests that check each piece: plans the piece
 * after *p, or returns false (leaving *p) when *p was the last one. */
static bool plan_next(WalManager *wm, WalByteInsert *p, const uint8_t *buf, uint32_t len)
{
    uint32_t off = (uint32_t)(p->src - buf) + p->len;
    if (off == len) return false;
    wal_insert_plan(wm, p, p->lsn + p->len + WAL_PAGE_HEADER_SIZE, buf, len, off);
    return true;
}

/* V2 §10.6, exactly: three copiers in a fresh frame, in data bytes of the
 * frame (frame offset = 24 + data offset). H [0,100) also writes the page
 * header; A [100,700); B [700,760), still copying when A has published.
 * Checked first on the planned pieces, then on the counters. */
static void test_plan_worked_example(void)
{
    printf("\n[test_plan_worked_example]\n");
    WalManager wm;
    ring_setup(&wm);
    uint8_t h_buf[100], a_buf[600], b_buf[60];
    make_batch(h_buf, (uint32_t[]){100}, 1);
    make_batch(a_buf, (uint32_t[]){600}, 1);
    make_batch(b_buf, (uint32_t[]){60}, 1);
    WalByteInsert h, a, b;

    /* Three records, H at LSN 24 (100 B), A at 124 (600 B), B at 724 (60 B). */
    wal_insert_plan(&wm, &h, 24, h_buf, 100, 0);
    wal_insert_plan(&wm, &a, 124, a_buf, 600, 0);
    wal_insert_plan(&wm, &b, 724, b_buf, 60, 0);
    CHECK(h.has_page_hdr && h.n_ctr == 1 && h.ctr_idx[0] == 0 && h.ctr_add[0] == 124,
          "H's plan: page header, block 0 += 24 + 100 = 124");
    CHECK(!a.has_page_hdr && a.n_ctr == 2 && a.ctr_idx[0] == 0 && a.ctr_add[0] == 388 &&
          a.ctr_idx[1] == 1 && a.ctr_add[1] == 212,
          "A's plan: no header, block 0 += 388, block 1 += 212");
    CHECK(!b.has_page_hdr && b.n_ctr == 1 && b.ctr_idx[0] == 1 && b.ctr_add[0] == 60,
          "B's plan: block 1 += 60");
    CHECK(!plan_next(&wm, &h, h_buf, 100) && h.len == 100, "a one-page batch has one piece");

    wal_insert_copy(&wm, &h);
    CHECK(ctr(&wm, 0) == 124, "H publishes: block 0 = 124");
    wal_insert_copy(&wm, &a);
    CHECK(ctr(&wm, 0) == 512 && ctr(&wm, 1) == 212, "A publishes: block 0 = 512 (full), block 1 = 212");
    wal_insert_copy(&wm, &b);
    CHECK(ctr(&wm, 1) == 272, "B publishes: block 1 = 272 (= 784 - 512)");

    WalPageHeader ph;
    CHECK(wal_page_header_deserialize(wm.ring_buf, &ph) == MYDB_OK &&
          ph.start_lsn == 0 && ph.continuation_len == 0 && ph.flags == 0,
          "H wrote the page header: start_lsn 0, no continuation");
    CHECK(memcmp(wm.ring_buf + 24, h_buf, 100) == 0 && memcmp(wm.ring_buf + 124, a_buf, 600) == 0 &&
          memcmp(wm.ring_buf + 724, b_buf, 60) == 0 && wm.ring_buf[784] == 0,
          "bytes 24..783 are H, A, B; nothing after");
    ring_free(&wm);

    /* H and A publish in either order: block 0 reaches 512 only when both are done. */
    ring_setup(&wm);
    wal_insert_copy(&wm, &a);
    CHECK(ctr(&wm, 0) == 388, "A first: block 0 = 388, not full (H's bytes are a hole)");
    wal_insert_copy(&wm, &h);
    CHECK(ctr(&wm, 0) == 512, "then H: block 0 = 512");
    ring_free(&wm);
}

/* One 6000-byte record from page 1's start: a full page, then 1928 bytes
 * continuing on page 2. */
static void test_plan_record_spans_pages(void)
{
    printf("\n[test_plan_record_spans_pages]\n");
    WalManager wm;
    ring_setup(&wm);
    static uint8_t buf[6000];
    make_batch(buf, (uint32_t[]){6000}, 1);
    WalByteInsert p;
    WalPageHeader h;

    wal_insert_plan(&wm, &p, 4096 + 24, buf, 6000, 0);
    CHECK(p.frame == 1 && p.len == 4072 && p.has_page_hdr && p.n_ctr == 8,
          "piece 1: frame 1, a whole page (4072 bytes), 8 counters");
    int all = 1;
    for (int i = 0; i < 8; i++) if (p.ctr_idx[i] != (uint32_t)(8 + i) || p.ctr_add[i] != 512) all = 0;
    CHECK(all, "every one of frame 1's blocks gets 512");
    CHECK(wal_page_header_deserialize(p.page_hdr, &h) == MYDB_OK && h.start_lsn == 4096 &&
          h.continuation_len == 0 && h.flags == 0,
          "the record starts at page 1's offset 24: no continuation");
    wal_insert_copy(&wm, &p);

    CHECK(plan_next(&wm, &p, buf, 6000), "there is a second piece");
    CHECK(p.lsn == 2 * 4096 + 24 && p.frame == 2 && p.len == 1928 && p.src == buf + 4072,
          "piece 2: page 2's offset 24, the last 1928 bytes");
    CHECK(wal_page_header_deserialize(p.page_hdr, &h) == MYDB_OK && h.start_lsn == 8192 &&
          h.continuation_len == 1928 && h.flags == WAL_PAGE_INCOMING_CONTINUATION,
          "page 2 continues the record: continuation_len 1928, INCOMING");
    CHECK(p.n_ctr == 4 && p.ctr_add[0] == 512 && p.ctr_add[1] == 512 && p.ctr_add[2] == 512 &&
          p.ctr_add[3] == 24 + 1928 - 3 * 512 && p.ctr_idx[3] == 19,
          "24 + 1928 = 1952 bytes: blocks 16-18 get 512, block 19 gets 416");
    wal_insert_copy(&wm, &p);
    CHECK(!plan_next(&wm, &p, buf, 6000) && p.len == 1928, "no third piece, *p left as it was");
    ring_free(&wm);
}

/* Several records in one batch: a page that starts inside a later record
 * of the batch continues that record, not the first one, and a page that
 * starts exactly where a record starts continues nothing. */
static void test_plan_batch_continuation(void)
{
    printf("\n[test_plan_batch_continuation]\n");
    WalManager wm;
    ring_setup(&wm);
    static uint8_t buf[16384];
    WalByteInsert p;
    WalPageHeader h;

    /* Records [0,3000) [3000,5000) [5000,10000) from LSN 24. Page 1
     * starts at buffer offset 4072, inside the second record: 928 of it
     * left. Page 2 starts at 8144, inside the third: 1856 left. */
    uint32_t len = make_batch(buf, (uint32_t[]){3000, 2000, 5000}, 3);
    wal_insert_plan(&wm, &p, 24, buf, len, 0);
    CHECK(wal_page_header_deserialize(p.page_hdr, &h) == MYDB_OK && h.continuation_len == 0,
          "page 0: the batch's first record starts there");
    CHECK(plan_next(&wm, &p, buf, len) && wal_page_header_deserialize(p.page_hdr, &h) == MYDB_OK &&
          h.continuation_len == 928 && h.flags == WAL_PAGE_INCOMING_CONTINUATION,
          "page 1: continues the second record, continuation_len 928");
    CHECK(plan_next(&wm, &p, buf, len) && wal_page_header_deserialize(p.page_hdr, &h) == MYDB_OK &&
          h.continuation_len == 1856 && p.len == 1856,
          "page 2: continues the third record, continuation_len 1856 (all of the piece)");
    CHECK(!plan_next(&wm, &p, buf, len), "three pieces in all");

    /* Records [0,4072) [4072,4172): the second starts at page 1's offset 24. */
    len = make_batch(buf, (uint32_t[]){4072, 100}, 2);
    wal_insert_plan(&wm, &p, 24, buf, len, 0);
    CHECK(p.len == 4072, "page 0 holds exactly the first record");
    CHECK(plan_next(&wm, &p, buf, len) && wal_page_header_deserialize(p.page_hdr, &h) == MYDB_OK &&
          h.continuation_len == 0 && h.flags == 0 && p.len == 100,
          "page 1 starts at a record: no continuation, no flag");

    /* A batch starting mid-page, whose second record straddles page 1:
     * records [0,3000) [3000,4000) from LSN 1024 (buffer offset 0 = page
     * offset 1024). Page 1 starts at buffer offset 4096 - 1024 = 3072,
     * inside the second record: 928 left. */
    len = make_batch(buf, (uint32_t[]){3000, 1000}, 2);
    wal_insert_plan(&wm, &p, 1024, buf, len, 0);
    CHECK(!p.has_page_hdr && p.len == 3072, "a mid-page start: no header, 3072 bytes to the page end");
    CHECK(plan_next(&wm, &p, buf, len) && wal_page_header_deserialize(p.page_hdr, &h) == MYDB_OK &&
          h.continuation_len == 928 && p.len == 928,
          "page 1: continuation_len 928");
    ring_free(&wm);
}

/* A piece starting mid-page writes no header and counts only its bytes. */
static void test_plan_mid_page(void)
{
    printf("\n[test_plan_mid_page]\n");
    WalManager wm;
    ring_setup(&wm);
    uint8_t buf[300];
    make_batch(buf, (uint32_t[]){300}, 1);
    WalByteInsert p;

    /* Page 3, offset 1000. */
    wal_insert_plan(&wm, &p, 3 * 4096 + 1000, buf, 300, 0);
    CHECK(!p.has_page_hdr && p.n_ctr == 2 && p.ctr_idx[0] == 25 && p.ctr_add[0] == 24 &&
          p.ctr_idx[1] == 26 && p.ctr_add[1] == 276,
          "300 bytes from offset 1000: block 1 += 24, block 2 += 276");
    wal_insert_copy(&wm, &p);
    uint8_t zero[24] = {0};
    CHECK(memcmp(wm.ring_buf + 3 * 4096, zero, 24) == 0 && ctr(&wm, 24) == 0,
          "no page header written, block 0 untouched");
    ring_free(&wm);
}

/* The ring wraps: one ring length later is the same frame and counters. */
static void test_plan_wraps(void)
{
    printf("\n[test_plan_wraps]\n");
    WalManager wm;
    ring_setup(&wm);
    uint8_t buf[100];
    make_batch(buf, (uint32_t[]){100}, 1);
    uint64_t lap = (uint64_t)WAL_RING_FRAMES * 4096;   /* one ring lap, 8 MB */
    WalByteInsert p;

    wal_insert_plan(&wm, &p, lap + 24, buf, 100, 0);
    CHECK(p.frame == 0 && p.ctr_idx[0] == 0, "LSN 8 MB + 24 (one lap) plans frame 0, counter 0");
    wal_insert_copy(&wm, &p);
    WalPageHeader h;
    CHECK(wal_page_header_deserialize(wm.ring_buf, &h) == MYDB_OK && h.start_lsn == lap,
          "its header says start_lsn 8 MB");

    /* A batch running off the ring's last frame continues in frame 0. */
    static uint8_t big[5000];
    make_batch(big, (uint32_t[]){5000}, 1);
    wal_insert_plan(&wm, &p, lap - 4096 + 24, big, 5000, 0);
    CHECK(p.frame == WAL_RING_FRAMES - 1, "starts in the last frame");
    CHECK(plan_next(&wm, &p, big, 5000) && p.frame == 0 && p.lsn == lap + 24 && p.ctr_idx[0] == 0,
          "continues in frame 0 at LSN 8 MB + 24");
    ring_free(&wm);
}

/* The whole round trip: a multi-page batch copied into the ring reads
 * back, with the page headers stripped, as the caller's buffer; every
 * page header checks out; the counters add up to the bytes written. */
static void test_copy_round_trip(void)
{
    printf("\n[test_copy_round_trip]\n");
    WalManager wm;
    ring_setup(&wm);
    static uint8_t buf[20000], back[20000];
    uint32_t len = make_batch(buf, (uint32_t[]){44, 3000, 7000, 100, 9000}, 5);   /* 19144 */
    uint64_t lsn = 5 * 4096 + 2000;                                                 /* mid page 5 */

    int pieces = insert_batch(&wm, lsn, buf, len);
    /* 4096 - 2000 = 2096 bytes on page 5, then 4072 a page: 2096 + 4 *
     * 4072 = 18384, so pages 6-9 are full and 760 bytes land on page 10:
     * 6 pieces. */
    CHECK(pieces == 6, "19144 bytes from page 5's offset 2000 make 6 pieces");

    uint32_t got = 0, headers_ok = 1;
    for (uint32_t pg = 5; pg <= 10; pg++) {
        uint32_t from = (pg == 5) ? 2000 : WAL_PAGE_HEADER_SIZE;
        uint32_t n    = WAL_PAGE_SIZE - from;
        if (n > len - got) n = len - got;
        memcpy(back + got, wm.ring_buf + (size_t)pg * 4096 + from, n);
        got += n;
        WalPageHeader h;
        if (pg > 5 && (wal_page_header_deserialize(wm.ring_buf + (size_t)pg * 4096, &h) != MYDB_OK ||
                       h.start_lsn != (uint64_t)pg * 4096))
            headers_ok = 0;
    }
    CHECK(got == len && memcmp(back, buf, len) == 0, "the ring holds the batch's bytes, in order");
    CHECK(headers_ok, "pages 6-10 have valid headers with their own start_lsn");

    uint32_t sum = 0;
    for (uint32_t i = 0; i <= wm.ctr_mask; i++) sum += ctr(&wm, i);
    CHECK(sum == len + 5 * WAL_PAGE_HEADER_SIZE, "counters add up to the bytes + 5 page headers");
    CHECK(ctr(&wm, 5 * 8 + 3) == 512 - (2000 - 1536) && ctr(&wm, 6 * 8) == 512 && ctr(&wm, 10 * 8 + 1) == 24 + 760 - 512,
          "page 5 block 3 = 48, page 6 block 0 = 512, page 10 block 1 = 272");
    ring_free(&wm);
}

/* Records as a caller hands them over: every header field the caller
 * owns set, lsn and prev_lsn junk, body_checksum in the checksum field. */
static uint32_t make_records(uint8_t *buf, const uint32_t *lens, int n)
{
    uint32_t off = 0;
    for (int i = 0; i < n; i++) {
        uint32_t body_len = lens[i] - WAL_RECORD_HEADER_SIZE;
        uint8_t *body = buf + off + WAL_RECORD_HEADER_SIZE;
        for (uint32_t j = 0; j < body_len; j++) body[j] = (uint8_t)(j * 13 + i + 1);

        WalRecordHeader h = {0};
        h.lsn       = UINT64_MAX;
        h.prev_lsn  = UINT64_MAX;
        h.txn_id    = 42;
        h.total_len = lens[i];
        h.table_id  = 5;
        h.page_no   = 100 + (uint32_t)i;
        h.rec_type  = WAL_REC_INSERT;
        h.checksum  = body_len ? crc32c_update(CRC32C_INIT, body, body_len) : CRC32C_INIT;
        memcpy(buf + off, &h, WAL_RECORD_HEADER_SIZE);
        off += lens[i];
    }
    return off;
}

/* The record at rec is exactly what wal_record_header_serialize builds
 * from the same fields and body, with this lsn, prev_lsn and flags. */
static bool record_is(const uint8_t *rec, uint32_t total_len, uint32_t page_no,
                      uint64_t lsn, uint64_t prev_lsn, uint8_t flags)
{
    static uint8_t want[10000];
    uint32_t body_len = total_len - WAL_RECORD_HEADER_SIZE;
    WalRecordHeader h = {0};
    h.lsn       = lsn;
    h.prev_lsn  = prev_lsn;
    h.txn_id    = 42;
    h.total_len = total_len;
    h.table_id  = 5;
    h.page_no   = page_no;
    h.rec_type  = WAL_REC_INSERT;
    h.flags     = flags;
    wal_record_header_serialize(&h, body_len ? rec + WAL_RECORD_HEADER_SIZE : NULL, body_len, want);
    return memcmp(rec, want, total_len) == 0;
}

/* Reserved at byte 3000: [1072][100][5000]. The first ends exactly on
 * page 0's end, the second sits inside page 1, the third runs onto page 2. */
static void test_fill_headers(void)
{
    printf("\n[test_fill_headers]\n");
    static uint8_t buf[10000];
    const uint8_t cont = WAL_RECORD_FLAG_CONTINUES_ON_NEW_PAGE;

    uint32_t len  = make_records(buf, (uint32_t[]){1072, 100, 5000}, 3);
    uint64_t last = wal_insert_fill_headers(buf, len, 3000, 777);
    CHECK(record_is(buf, 1072, 100, 3024, 777, 0),
          "1st: LSN 3024, prev_lsn is the argument, ends on the page end so no flag");
    CHECK(record_is(buf + 1072, 100, 101, 4120, 3024, 0),
          "2nd: LSN 4120 (past page 1's header), prev_lsn 3024");
    CHECK(record_is(buf + 1172, 5000, 102, 4220, 4120, cont),
          "3rd: LSN 4220, prev_lsn 4120, continues on a new page");
    CHECK(last == 4220, "returns the last record's LSN");

    make_records(buf, (uint32_t[]){1073}, 1);
    wal_insert_fill_headers(buf, 1073, 3000, 777);
    CHECK(record_is(buf, 1073, 100, 3024, 777, cont), "one byte past the page end sets the flag");

    /* 22 bytes left on page 0: the first header itself is cut. */
    len  = make_records(buf, (uint32_t[]){44, 60}, 2);
    last = wal_insert_fill_headers(buf, len, 4050, 0);
    CHECK(record_is(buf, 44, 100, 4074, 0, cont),
          "a header cut by the page end, no body, first of its txn (prev_lsn 0)");
    CHECK(record_is(buf + 44, 60, 101, 4142, 4074, 0) && last == 4142,
          "the record after it is at LSN 4142");

    /* Only the flag is the WAL's; the caller's other bits stay. */
    make_records(buf, (uint32_t[]){100}, 1);
    buf[offsetof(WalRecordHeader, flags)] = 0x80;
    wal_insert_fill_headers(buf, 100, 4050, 0);
    CHECK(record_is(buf, 100, 100, 4074, 0, 0x80 | cont), "the caller's other flag bits are kept");

    /* A stale flag from the caller is cleared on a record that fits. */
    make_records(buf, (uint32_t[]){100}, 1);
    buf[offsetof(WalRecordHeader, flags)] = 0x80 | cont;
    wal_insert_fill_headers(buf, 100, 0, 0);
    CHECK(record_is(buf, 100, 100, 24, 0, 0x80), "a stale CONTINUES flag is cleared, the other bits kept");

    make_records(buf + 1, (uint32_t[]){333}, 1);
    last = wal_insert_fill_headers(buf + 1, 333, (uint64_t)WAL_SEG_PAGES * 4072, 24);
    CHECK(record_is(buf + 1, 333, 100, (uint64_t)WAL_SEG_SIZE + 24, 24, 0) && last == (uint64_t)WAL_SEG_SIZE + 24,
          "segment 1's first byte, from a buffer at an odd address");
}

/* Every start position in a page, across a segment end and the ring's
 * wrap: headers filled, then planned and copied. The records read back
 * from the ring are valid, and the pages marked INCOMING are exactly the
 * page starts the records run over. */
static void test_fill_then_plan_every_start(void)
{
    printf("\n[test_fill_then_plan_every_start]\n");
    WalManager wm;
    ring_setup(&wm);
    static uint8_t buf[14000], back[14000];
    const uint32_t lens[] = {44, 500, 44, 4072, 44, 9000, 100};   /* 13804 */
    const int n = 7;

    bool headers_ok = true, ring_ok = true, pages_ok = true;
    for (uint32_t start = 0; start < WAL_PAGE_USABLE; start++) {
        uint64_t byte_pos = 1022ull * WAL_PAGE_USABLE + start;
        uint64_t first    = (1022ull << WAL_PAGE_SHIFT) + WAL_PAGE_HEADER_SIZE + start;

        uint32_t len  = make_records(buf, lens, n);
        uint64_t last = wal_insert_fill_headers(buf, len, byte_pos, 999);

        /* Expected LSNs by stepping record to record, without wal_byte_to_lsn. */
        uint64_t lsn = first, prev = 999;
        uint32_t off = 0, crossed = 0;
        for (int i = 0; i < n; i++) {
            uint64_t end   = wal_lsn_advance(lsn, lens[i]);
            uint32_t pages = (uint32_t)(((end - 1) >> WAL_PAGE_SHIFT) - (lsn >> WAL_PAGE_SHIFT));
            uint8_t  flags = pages ? WAL_RECORD_FLAG_CONTINUES_ON_NEW_PAGE : 0;
            if (!record_is(buf + off, lens[i], 100 + (uint32_t)i, lsn, prev, flags)) headers_ok = false;
            crossed += pages;
            off += lens[i];
            prev = lsn;
            lsn  = wal_lsn_offset(end) ? end : end + WAL_PAGE_HEADER_SIZE;
        }
        if (last != prev) headers_ok = false;

        WalByteInsert p;
        uint32_t incoming = 0;
        lsn = first;
        off = 0;
        while (off < len) {
            wal_insert_plan(&wm, &p, lsn, buf, len, off);
            wal_insert_copy(&wm, &p);
            uint8_t *frame = wm.ring_buf + (size_t)p.frame * WAL_PAGE_SIZE;
            memcpy(back + off, frame + wal_lsn_offset(lsn), p.len);
            if (p.has_page_hdr) {
                WalPageHeader h;
                if (wal_page_header_deserialize(frame, &h) != MYDB_OK) pages_ok = false;
                else if (h.flags & WAL_PAGE_INCOMING_CONTINUATION) incoming++;
            }
            off += p.len;
            lsn += p.len + WAL_PAGE_HEADER_SIZE;
        }
        if (incoming != crossed) pages_ok = false;

        off = 0;
        for (int i = 0; i < n; i++) {
            WalRecordHeader h;
            if (wal_record_header_deserialize(back + off, lens[i] - WAL_RECORD_HEADER_SIZE, &h) != MYDB_OK)
                ring_ok = false;
            off += lens[i];
        }
        if (memcmp(back, buf, len) != 0) ring_ok = false;
    }
    CHECK(headers_ok, "all 4072 starts: lsn, prev_lsn, flag and checksum of every record");
    CHECK(ring_ok,    "all 4072 starts: every record read back from the ring has a valid CRC");
    CHECK(pages_ok,   "all 4072 starts: INCOMING pages match the records that cross a page");
    ring_free(&wm);
}

static void test_reserve(void)
{
    printf("\n[test_reserve]\n");
    WalManager wm;
    memset(&wm, 0, sizeof(wm));   /* reserve touches only current_pos */

    CHECK(wal_insert_reserve(&wm, 100) == 0 && atomic_load(&wm.current_pos) == 100,
          "a new log: returns byte 0, current_pos 100");
    CHECK(wal_insert_reserve(&wm, 5000) == 100 && atomic_load(&wm.current_pos) == 5100,
          "the next range starts where the last ended, across a page end");

    atomic_store(&wm.current_pos, 732);   /* as init resumes at LSN 756 */
    uint64_t pos = wal_insert_reserve(&wm, 76);
    CHECK(pos == 732 && wal_byte_to_lsn(pos) == 756, "after a resume at LSN 756 it reserves from there");

    const uint64_t high = (5ull << 32) + 100;
    atomic_store(&wm.current_pos, high);
    CHECK(wal_insert_reserve(&wm, 3000000000u) == high &&
          atomic_load(&wm.current_pos) == high + 3000000000ull,
          "positions past 4 GB and a 3 GB length are not truncated");

    /* Far more than the 8 MB ring with nothing written (write_lsn 0). */
    atomic_store(&wm.current_pos, 0);
    CHECK(wal_insert_reserve(&wm, 64u << 20) == 0 && atomic_load(&wm.current_pos) == 64u << 20,
          "more than the whole ring still succeeds");
}

#define RESERVE_THREADS 8
#define RESERVE_EACH    50000

typedef struct {
    uint64_t pos;
    uint32_t len;
} Range;

typedef struct {
    WalManager  *wm;
    _Atomic int *go;
    Range       *out;
    uint32_t     id;
} Reserver;

static void *reserve_many(void *arg)
{
    Reserver *r = arg;
    while (!atomic_load(r->go)) { }   /* all start together */
    for (uint32_t i = 0; i < RESERVE_EACH; i++) {
        r->out[i].len = WAL_RECORD_HEADER_SIZE + (i * 37 + r->id * 101) % 5000;
        r->out[i].pos = wal_insert_reserve(r->wm, r->out[i].len);
    }
    return NULL;
}

static int range_by_pos(const void *a, const void *b)
{
    const Range *x = a, *y = b;
    return (x->pos > y->pos) - (x->pos < y->pos);
}

/* Threads reserving at once: sorted by position, the ranges must line up
 * end to start, from the first position to current_pos. */
static void test_reserve_concurrent(void)
{
    printf("\n[test_reserve_concurrent]\n");
    WalManager wm;
    memset(&wm, 0, sizeof(wm));
    const uint64_t first = 732;
    atomic_store(&wm.current_pos, first);

    const size_t total = (size_t)RESERVE_THREADS * RESERVE_EACH;
    Range *all = malloc(total * sizeof(Range));
    if (!all) { CHECK(0, "allocated the result array"); return; }

    _Atomic int go = 0;
    pthread_t th[RESERVE_THREADS];
    Reserver  args[RESERVE_THREADS];
    int started = 0;
    for (; started < RESERVE_THREADS; started++) {
        args[started] = (Reserver){ &wm, &go, all + (size_t)started * RESERVE_EACH, (uint32_t)started };
        if (pthread_create(&th[started], NULL, reserve_many, &args[started]) != 0) break;
    }
    atomic_store(&go, 1);
    for (int t = 0; t < started; t++) pthread_join(th[t], NULL);
    CHECK(started == RESERVE_THREADS, "8 threads ran");

    if (started == RESERVE_THREADS) {
        bool rising = true;
        for (int t = 0; t < started; t++)
            for (int i = 1; i < RESERVE_EACH; i++)
                if (args[t].out[i].pos < args[t].out[i - 1].pos + args[t].out[i - 1].len) rising = false;
        CHECK(rising, "each thread's own ranges come in log order");

        qsort(all, total, sizeof(Range), range_by_pos);
        uint64_t next = first;
        bool tiled = true;
        for (size_t i = 0; i < total; i++) {
            if (all[i].pos != next) tiled = false;
            next += all[i].len;
        }
        CHECK(tiled, "400000 ranges with no overlap and no gap");
        CHECK(atomic_load(&wm.current_pos) == next, "current_pos ends at the last range's end");
    }
    free(all);
}

static void test_insert_basic(void)
{
    printf("\n[test_insert_basic]\n");
    WalManager wm;
    ring_setup(&wm);
    static uint8_t buf[21000];
    uint64_t last, end, wc = 0;   /* wc: the session's write_lsn copy */

    make_records(buf, (uint32_t[]){100}, 1);
    CHECK(wal_insert(&wm, buf, 100, 0, &wc, &last, &end) == MYDB_OK && last == 24 && end == 124,
          "a new log's first record: last LSN 24, end 124");
    CHECK(record_is(buf, 100, 100, 24, 0, 0) && memcmp(wm.ring_buf + 24, buf, 100) == 0,
          "its header is filled in the caller's buffer and it is in the ring");
    CHECK(ctr(&wm, 0) == 124 && atomic_load(&wm.current_pos) == 100,
          "block 0 = 124 (page header + record), current_pos 100");

    /* The same transaction again: two records, up to exactly page 0's end. */
    uint32_t len = make_records(buf, (uint32_t[]){1000, 2972}, 2);
    CHECK(wal_insert(&wm, buf, len, last, &wc, &last, &end) == MYDB_OK && last == 1124 && end == 4096,
          "a batch ending on the page end: last LSN 1124, end 4096 (not 4120)");
    CHECK(record_is(buf, 1000, 100, 124, 24, 0) && record_is(buf + 1000, 2972, 101, 1124, 124, 0),
          "prev_lsn chains across the two calls and inside the batch");
    bool full = true;
    for (uint32_t b = 0; b < 8; b++) if (ctr(&wm, b) != 512) full = false;
    CHECK(full && ctr(&wm, 8) == 0, "page 0's blocks all read 512, page 1 untouched");

    make_records(buf, (uint32_t[]){44}, 1);
    CHECK(wal_insert(&wm, buf, 44, last, &wc, &last, &end) == MYDB_OK && last == 4120 && end == 4164,
          "the next record opens page 1: LSN 4120, end 4164");
    WalPageHeader h;
    CHECK(wal_page_header_deserialize(wm.ring_buf + 4096, &h) == MYDB_OK &&
          h.start_lsn == 4096 && h.flags == 0 && ctr(&wm, 8) == 24 + 44,
          "and writes that page's header, no continuation");
    ring_free(&wm);

    /* One record over five pages: 4 full (16288 bytes), 3712 on the fifth. */
    ring_setup(&wm);
    wc = 0;                       /* a new WalManager: a new session copy */
    make_records(buf, (uint32_t[]){20000}, 1);
    CHECK(wal_insert(&wm, buf, 20000, 0, &wc, &last, &end) == MYDB_OK &&
          last == 24 && end == 4 * 4096 + 24 + 3712,
          "a 20000-byte record spans five pages");
    CHECK(wal_page_header_deserialize(wm.ring_buf + 4 * 4096, &h) == MYDB_OK &&
          h.continuation_len == 3712 && h.flags == WAL_PAGE_INCOMING_CONTINUATION,
          "the fifth page continues it: continuation_len 3712");
    ring_free(&wm);
}

static void test_insert_rejects(void)
{
    printf("\n[test_insert_rejects]\n");
    WalManager wm;
    ring_setup(&wm);
    atomic_store(&wm.current_pos, 732);
    uint8_t  buf[100];
    uint64_t last, end, wc = 0;
    make_records(buf, (uint32_t[]){100}, 1);

    CHECK(wal_insert(NULL, buf, 100, 0, &wc, &last, &end) == MYDB_ERR &&
          wal_insert(&wm, NULL, 100, 0, &wc, &last, &end) == MYDB_ERR &&
          wal_insert(&wm, buf, 100, 0, NULL, &last, &end) == MYDB_ERR &&
          wal_insert(&wm, buf, 100, 0, &wc, NULL, &end) == MYDB_ERR &&
          wal_insert(&wm, buf, 100, 0, &wc, &last, NULL) == MYDB_ERR,
          "a NULL argument is refused");
    CHECK(wal_insert(&wm, buf, 0, 0, &wc, &last, &end) == MYDB_ERR &&
          wal_insert(&wm, buf, WAL_RECORD_HEADER_SIZE - 1, 0, &wc, &last, &end) == MYDB_ERR,
          "a buffer shorter than one record header is refused");
    CHECK(atomic_load(&wm.current_pos) == 732, "refused calls reserve nothing");
    ring_free(&wm);
}

/* wal_insert from every start position in a page, across a segment end
 * and the ring's wrap: the ring read back, every page header, and every
 * counter. */
static void test_insert_every_start(void)
{
    printf("\n[test_insert_every_start]\n");
    WalManager wm;
    ring_setup(&wm);
    static uint8_t buf[14000], back[14000];
    const uint32_t lens[] = {44, 500, 44, 4072, 44, 9000, 100};   /* 13804 */

    bool out_ok = true, ring_ok = true, pages_ok = true, ctrs_ok = true;
    for (uint32_t start = 0; start < WAL_PAGE_USABLE; start++) {
        uint64_t byte_pos = 1022ull * WAL_PAGE_USABLE + start;
        uint64_t first    = (1022ull << WAL_PAGE_SHIFT) + WAL_PAGE_HEADER_SIZE + start;
        atomic_store(&wm.current_pos, byte_pos);
        /* Past one ring: as if a write step had written everything before
         * this insert (its page start, when it opens the page). */
        atomic_store(&wm.write_lsn, start == 0 ? first - WAL_PAGE_HEADER_SIZE : first);

        uint32_t len = make_records(buf, lens, 7);
        uint64_t last, end, wc = 0;
        if (wal_insert(&wm, buf, len, 999, &wc, &last, &end) != MYDB_OK) { out_ok = false; break; }
        if (end != wal_lsn_advance(first, len) ||
            last != wal_byte_to_lsn(byte_pos + len - 100) ||
            atomic_load(&wm.current_pos) != byte_pos + len)
            out_ok = false;

        /* Read the ring back, hopping the page headers. */
        uint64_t lsn = first;
        uint32_t off = 0;
        while (off < len) {
            uint8_t *frame = wm.ring_buf + ((lsn >> WAL_PAGE_SHIFT) & wm.ring_frame_mask) * WAL_PAGE_SIZE;
            uint32_t n = WAL_PAGE_SIZE - wal_lsn_offset(lsn);
            if (n > len - off) n = len - off;
            memcpy(back + off, frame + wal_lsn_offset(lsn), n);

            if (wal_lsn_offset(lsn) == WAL_PAGE_HEADER_SIZE) {
                uint32_t rec = 0, cont = 0;   /* what is left of the record holding off */
                for (int i = 0; i < 7; i++) {
                    if (rec < off && off < rec + lens[i]) cont = rec + lens[i] - off;
                    rec += lens[i];
                }
                if (cont > WAL_PAGE_USABLE) cont = WAL_PAGE_USABLE;
                WalPageHeader h;
                if (wal_page_header_deserialize(frame, &h) != MYDB_OK ||
                    h.start_lsn != lsn - WAL_PAGE_HEADER_SIZE || h.continuation_len != cont ||
                    h.flags != (cont ? WAL_PAGE_INCOMING_CONTINUATION : 0))
                    pages_ok = false;
            }
            off += n;
            lsn += n + WAL_PAGE_HEADER_SIZE;
        }
        if (memcmp(back, buf, len) != 0) ring_ok = false;
        off = 0;
        for (int i = 0; i < 7; i++) {
            WalRecordHeader h;
            if (wal_record_header_deserialize(back + off, lens[i] - WAL_RECORD_HEADER_SIZE, &h) != MYDB_OK)
                ring_ok = false;
            off += lens[i];
        }

        /* What was published is one LSN range, [from, end), page headers
         * included: every block's counter is its overlap with it. */
        uint64_t from = (wal_lsn_offset(first) == WAL_PAGE_HEADER_SIZE) ? first - WAL_PAGE_HEADER_SIZE : first;
        uint32_t sum = 0;
        for (uint64_t b = from & ~(uint64_t)(WAL_BLOCK_SIZE - 1); b < end; b += WAL_BLOCK_SIZE) {
            uint64_t lo = b > from ? b : from;
            uint64_t hi = b + WAL_BLOCK_SIZE < end ? b + WAL_BLOCK_SIZE : end;
            uint16_t got = atomic_exchange(wal_block_counter(wm.fs_counters, wm.ctr_mask, b), 0);
            if (got != hi - lo) ctrs_ok = false;
            sum += got;
        }
        if (sum != end - from) ctrs_ok = false;
    }
    for (uint32_t i = 0; i <= wm.ctr_mask; i++) if (ctr(&wm, i) != 0) ctrs_ok = false;

    CHECK(out_ok,   "all 4072 starts: last LSN, end LSN and current_pos");
    CHECK(ring_ok,  "all 4072 starts: the ring holds the buffer, every CRC valid");
    CHECK(pages_ok, "all 4072 starts: every page header's start_lsn, continuation_len and flag");
    CHECK(ctrs_ok,  "all 4072 starts: every counter is exactly the bytes written in its block");
    ring_free(&wm);
}

#define INSERT_WAL_DIR "/tmp/mydb_test_wal_insert"

static void remove_wal_dir(void)
{
    DIR *d = opendir(INSERT_WAL_DIR);
    if (d) {
        struct dirent *ent;
        char path[400];
        while ((ent = readdir(d)) != NULL) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
            snprintf(path, sizeof(path), "%s/%s", INSERT_WAL_DIR, ent->d_name);
            unlink(path);
        }
        closedir(d);
    }
    rmdir(INSERT_WAL_DIR);
}

/* Writes and syncs everything up to end (wal_log_flush). */
static bool write_out(WalManager *wm, uint64_t end)
{
    return wal_log_flush(wm, end) == MYDB_OK &&
           atomic_load(&wm->write_lsn) >= end && atomic_load(&wm->flushed_lsn) >= end;
}

/* The writer against the reader: insert into a real WalManager, commit
 * (write and sync), restart, and tail_scan must find exactly the end wal_insert
 * returned, then inserting goes on from there. Runs past segment 0, once
 * ending exactly on its end and once with a record across it. */
static void test_insert_restart(bool end_on_segment_end)
{
    printf("\n[test_insert_restart: %s]\n",
           end_on_segment_end ? "ends on the segment end" : "a record across the segment end");
    remove_wal_dir();
    WalManager wm;
    memset(&wm, 0, sizeof(wm));
    if (wal_manager_init(&wm, INSERT_WAL_DIR, 1) != MYDB_OK) {
        CHECK(0, "init succeeds");
        remove_wal_dir();
        return;
    }

    static uint8_t buf[70000];
    const uint64_t seg_bytes = (uint64_t)WAL_SEG_PAGES * WAL_PAGE_USABLE;   /* 4169728 */
    uint64_t last = 0, end = 0, wc = 0;
    bool     ok = true, at_seg_end = false, crossed = false, up = true;
    int      i, restarts = 0;

    for (i = 0; ok && atomic_load(&wm.current_pos) < seg_bytes + 100000; i++) {
        uint64_t pos  = atomic_load(&wm.current_pos);
        uint32_t lens[3] = { 44 + (uint32_t)(i * 131) % 1500, 44, 44 + (uint32_t)(i * 7919) % 25000 };
        int      n = 1 + i % 3;

        uint32_t to_page_end = WAL_PAGE_USABLE - (uint32_t)(pos % WAL_PAGE_USABLE);
        if (i % 5 == 0 && to_page_end >= WAL_RECORD_HEADER_SIZE) {   /* end on a page end */
            lens[0] = to_page_end;
            n = 1;
        }
        bool near_seg_end = pos < seg_bytes && seg_bytes - pos < 40000;
        if (near_seg_end) {
            uint32_t left = (uint32_t)(seg_bytes - pos);
            lens[0] = end_on_segment_end && left >= WAL_RECORD_HEADER_SIZE ? left : left + 9000;
            n = 1;
        }

        uint32_t len = make_records(buf, lens, n);
        if (wal_insert(&wm, buf, len, last, &wc, &last, &end) != MYDB_OK || !write_out(&wm, end)) {
            ok = false;
            break;
        }
        if (end == WAL_SEG_SIZE) at_seg_end = true;
        if (pos < seg_bytes && pos + len > seg_bytes) crossed = true;

        if (near_seg_end || i % 25 == 0 || i % 37 == 0) {
            wal_manager_shutdown(&wm);
            up = false;
            memset(&wm, 0, sizeof(wm));
            if (wal_manager_init(&wm, INSERT_WAL_DIR, 1) != MYDB_OK) { ok = false; break; }
            up = true;
            restarts++;
            wc = 0;   /* a new WalManager: the session's copy starts again */
            /* No record starts at offset 24 and ends there, so no end is
             * ever pulled back to a page start: resume is the end itself. */
            if (atomic_load(&wm.write_lsn) != end || atomic_load(&wm.current_pos) != pos + len) ok = false;
        }
    }

    CHECK(ok, "every restart resumes exactly at wal_insert's end, every block complete");
    CHECK(restarts >= 10, "restarted at least 10 times along the way");
    CHECK(wal_lsn_segment(end) == 1, "the log went on into segment 1 and round the ring");
    CHECK(end_on_segment_end ? (at_seg_end && !crossed) : (crossed && !at_seg_end),
          "the segment end was met the way this run means to");

    if (up) wal_manager_shutdown(&wm);
    remove_wal_dir();
}

typedef struct {
    WalManager   *wm;
    uint64_t      end;
    uint64_t     *wc;
    _Atomic bool  done;
} CheckJob;

static void *check_job(void *arg)
{
    CheckJob *j = arg;
    wal_insert_space_check(j->wm, j->end, j->wc);
    atomic_store(&j->done, true);
    return NULL;
}

/* Runs the space check on its own thread and says whether it returned
 * within 2 s, so a wrong wait fails the check instead of hanging the
 * test; then raises write_lsn so it returns, and joins. */
static bool check_returns(WalManager *wm, uint64_t end, uint64_t *wc)
{
    CheckJob  j = { .wm = wm, .end = end, .wc = wc };
    pthread_t th;
    if (pthread_create(&th, NULL, check_job, &j) != 0) return false;
    bool done = false;
    for (int i = 0; i < 2000 && !(done = atomic_load(&j.done)); i++) usleep(1000);
    if (!done) atomic_store(&wm->write_lsn, UINT64_MAX / 2);
    pthread_join(th, NULL);
    return done;
}

/* The space check where it must not wait (V2 §9.3, block rule): need =
 * the piece's end rounded up to its block end; free when need - ring
 * bytes <= write_lsn. Whether the session's copy was reloaded shows in wc. */
static void test_space_check_passes(void)
{
    printf("\n[test_space_check_passes]\n");
    WalManager wm;
    ring_setup(&wm);
    const uint64_t R = (uint64_t)WAL_RING_FRAMES * WAL_PAGE_SIZE;   /* 8 MB */
    uint64_t wc = 0;

    atomic_store(&wm.write_lsn, 1000);
    CHECK(check_returns(&wm, R, &wc) && wc == 0,
          "an end on a block end is not rounded further: R passes on the copy 0, no reload");

    CHECK(check_returns(&wm, R + 512, &wc) && wc == 1000,
          "R + 512 fails on the copy, reloads write_lsn 1000 and passes: block 0 is written");

    /* Lower write_lsn than the copy, only to see that it is not read. */
    atomic_store(&wm.write_lsn, 0);
    CHECK(check_returns(&wm, R + 1, &wc) && wc == 1000,
          "R + 1 (need R + 512) passes on the copy alone: no shared read");
    ring_free(&wm);
}

/* ------------------------------------------------------------------
 * The completeness scan (wal_flush_scan, V2 §10.5). Counters are set by
 * hand, no copiers: each case is a state the counters can be in.
 * ------------------------------------------------------------------ */

/* Zeroed counters, the scan to start at write_lsn, current_pos = pos. */
static void scan_reset(WalManager *wm, uint64_t write_lsn, uint64_t pos)
{
    memset(wm->fs_counters, 0, (size_t)(wm->ctr_mask + 1) * sizeof(WalBlockCounter));
    atomic_store(&wm->write_lsn, write_lsn);
    wm->last_ctr_idx = (uint32_t)((write_lsn >> WAL_BLOCK_SHIFT) & wm->ctr_mask);
    atomic_store(&wm->current_pos, pos);
}

static void set_ctr(WalManager *wm, uint32_t i, uint16_t v) { atomic_store(&wm->fs_counters[i].done, v); }

static void test_scan(void)
{
    printf("\n[test_scan]\n");
    WalManager wm;
    ring_setup(&wm);
    WalScanResult r;
    const uint64_t ring_bytes = (uint64_t)WAL_RING_FRAMES * WAL_PAGE_SIZE;

    /* V2 §10.6: H [0,100), A [100,700), B [700,760) in data bytes, so
     * LSN 24..124, 124..724, 724..784; current_pos 760 -> LSN 784. */
    scan_reset(&wm, 0, 760);
    set_ctr(&wm, 0, 124);                                  /* only H */
    wal_flush_scan(&wm, &r);
    CHECK(r.end_lsn == 0 && r.full_blocks == 0 && !r.tail,
          "worked example, H only: block 0 = 124 of 512 reserved, a hole: nothing to write");
    set_ctr(&wm, 0, 512); set_ctr(&wm, 1, 212);            /* A publishes */
    wal_flush_scan(&wm, &r);
    CHECK(r.end_lsn == 512 && r.full_blocks == 1 && !r.tail,
          "worked example, H and A: block 0 full, block 1 = 212 of 272 reserved, B is a hole: stop at 512");
    set_ctr(&wm, 1, 272);                                  /* B publishes */
    wal_flush_scan(&wm, &r);
    CHECK(r.end_lsn == 784 && r.full_blocks == 1 && r.tail,
          "worked example, all three: block 1 = 272 = reserved, a complete tail: up to 784");

    /* A whole frame full, then a tail in the next frame's block 0, whose
     * 124 bytes are its page header + 100 data bytes. */
    scan_reset(&wm, 0, WAL_PAGE_USABLE + 100);             /* LSN 4096 + 24 + 100 */
    for (uint32_t i = 0; i < 8; i++) set_ctr(&wm, i, 512);
    set_ctr(&wm, 8, 124);
    wal_flush_scan(&wm, &r);
    CHECK(r.end_lsn == 4220 && r.full_blocks == 8 && r.tail,
          "8 full blocks, then a tail of page header + 100 bytes: up to 4220");

    /* current_pos exactly at a page start: its LSN is 4120, but no copier
     * has written that page's header yet. */
    scan_reset(&wm, 0, WAL_PAGE_USABLE);
    for (uint32_t i = 0; i < 8; i++) set_ctr(&wm, i, 512);
    wal_flush_scan(&wm, &r);
    CHECK(r.end_lsn == 4096 && r.full_blocks == 8 && !r.tail,
          "current_pos at a page start: the next page's header is not written, stop at the page start 4096");

    /* A tail block with a hole in it. */
    scan_reset(&wm, 0, 688);                               /* LSN 712: 200 bytes into block 1 */
    set_ctr(&wm, 0, 512); set_ctr(&wm, 1, 100);
    wal_flush_scan(&wm, &r);
    CHECK(r.end_lsn == 512 && r.full_blocks == 1 && !r.tail,
          "tail block with 100 of 200 reserved bytes counted: a hole, stop at its start 512");

    /* Starting mid-block: write_lsn 124, the last write step stopped in
     * block 0, whose counter still holds those 124 bytes. */
    scan_reset(&wm, 124, 300);                             /* LSN 324 */
    set_ctr(&wm, 0, 324);
    wal_flush_scan(&wm, &r);
    CHECK(r.end_lsn == 324 && r.full_blocks == 0 && r.tail,
          "start mid-block: counter 324 = everything reserved from the block's start, up to 324");
    set_ctr(&wm, 0, 200);
    wal_flush_scan(&wm, &r);
    CHECK(r.end_lsn == 124 && !r.tail,
          "start mid-block with a hole: end stays at write_lsn 124, never below it");

    /* Wrapping at the ring's end: the last block, then counter 0 for the
     * next pass's first frame (page header + 76 bytes). */
    scan_reset(&wm, ring_bytes - 512, wal_lsn_to_byte(ring_bytes + 100));
    CHECK(wm.last_ctr_idx == wm.ctr_mask, "the scan starts at the last counter, 16383");
    set_ctr(&wm, wm.ctr_mask, 512);
    set_ctr(&wm, 0, 100);
    wal_flush_scan(&wm, &r);
    CHECK(r.end_lsn == ring_bytes + 100 && r.full_blocks == 1 && r.tail,
          "the scan wraps from counter 16383 to counter 0: up to ring end + 100");

    /* A completely full ring: every counter 512. */
    scan_reset(&wm, 0, (uint64_t)WAL_RING_FRAMES * WAL_PAGE_USABLE);   /* LSN ring end + 24 */
    for (uint32_t i = 0; i <= wm.ctr_mask; i++) set_ctr(&wm, i, 512);
    wal_flush_scan(&wm, &r);
    CHECK(r.end_lsn == ring_bytes && r.full_blocks == wm.ctr_mask + 1 && !r.tail,
          "a full ring: the scan stops after one ring, 16384 blocks, at 8 MB");

    ring_free(&wm);
}

int main(void)
{
    printf("=== test_wal_insert ===\n");

    test_block_counter_indexing();
    test_reserve();
    test_reserve_concurrent();
    test_fill_headers();
    test_fill_then_plan_every_start();
    test_plan_worked_example();
    test_plan_record_spans_pages();
    test_plan_batch_continuation();
    test_plan_mid_page();
    test_plan_wraps();
    test_copy_round_trip();
    test_insert_basic();
    test_insert_rejects();
    test_insert_every_start();
    test_insert_restart(true);
    test_insert_restart(false);
    test_space_check_passes();
    test_scan();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
