#include <dirent.h>
#include <pthread.h>
#include <sched.h>
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
#include "wal_frame_state.h"
#include "wal_insert.h"
#include "wal_page.h"
#include "wal_types.h"

/*
 * test_wal_insert — the V2 insert path (MYDB_WAL_DESIGN_V2.md §9, §10):
 * block counters, reserving, filling record headers, planning a piece
 * and copying it, wal_insert, the space check, and sessions inserting
 * concurrently against a stand-in write step.
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

/* A WalManager with only what the insert path touches: a zeroed 4 MB
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
}

static void ring_free(WalManager *wm)
{
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
    uint64_t lap = (uint64_t)WAL_RING_FRAMES * 4096;   /* 4 MB: segment 1's start */
    WalByteInsert p;

    wal_insert_plan(&wm, &p, lap + 24, buf, 100, 0);
    CHECK(p.frame == 0 && p.ctr_idx[0] == 0, "LSN 4 MB + 24 plans frame 0, counter 0");
    wal_insert_copy(&wm, &p);
    WalPageHeader h;
    CHECK(wal_page_header_deserialize(wm.ring_buf, &h) == MYDB_OK && h.start_lsn == lap,
          "its header says start_lsn 4 MB");

    /* A batch running off the ring's last frame continues in frame 0. */
    static uint8_t big[5000];
    make_batch(big, (uint32_t[]){5000}, 1);
    wal_insert_plan(&wm, &p, lap - 4096 + 24, big, 5000, 0);
    CHECK(p.frame == WAL_RING_FRAMES - 1, "starts in the last frame");
    CHECK(plan_next(&wm, &p, big, 5000) && p.frame == 0 && p.lsn == lap + 24 && p.ctr_idx[0] == 0,
          "continues in frame 0 at LSN 4 MB + 24");
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
    last = wal_insert_fill_headers(buf + 1, 333, 1024ull * 4072, 24);
    CHECK(record_is(buf + 1, 333, 100, (1ull << 22) + 24, 24, 0) && last == (1ull << 22) + 24,
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

    /* Far more than the 4 MB ring with nothing written (write_lsn 0). */
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

/* Stands in for the write step until wal_flush exists: everything up to
 * end must be complete in the counters (the tail block up to end); writes
 * those pages, moving to the next segment as it gets there, and zeroes
 * the counters of the blocks it passed. */
static bool write_out(WalManager *wm, uint64_t end)
{
    uint64_t from = atomic_load(&wm->write_lsn);

    for (uint64_t b = from & ~(uint64_t)(WAL_BLOCK_SIZE - 1); b < end; b += WAL_BLOCK_SIZE) {
        _Atomic uint16_t *c = wal_block_counter(wm->fs_counters, wm->ctr_mask, b);
        bool     whole = b + WAL_BLOCK_SIZE <= end;
        uint64_t want  = whole ? WAL_BLOCK_SIZE : end - b;
        if (atomic_load(c) != want) return false;
        if (whole) atomic_store(c, 0);
    }

    for (uint64_t pg = from & ~(uint64_t)(WAL_PAGE_SIZE - 1); pg < end; pg += WAL_PAGE_SIZE) {
        uint64_t seg = wm->pool.slots[wm->curr_slot].header.segment_no;
        if (wal_lsn_segment(pg) > seg) {
            if (wal_segment_pool_mark_done(&wm->pool, NULL, wm->curr_slot, (seg + 1) << WAL_SEG_SHIFT,
                                           WAL_SEG_PAGES, NULL) != MYDB_OK) return false;
            if (wal_segment_pool_claim_next(&wm->pool, &wm->curr_slot) != MYDB_OK) return false;
        }
        uint8_t *frame = wm->ring_buf + ((pg >> WAL_PAGE_SHIFT) & wm->ring_frame_mask) * WAL_PAGE_SIZE;
        if (wal_segment_pool_write_page(&wm->pool, wm->curr_slot, wal_lsn_page(pg) + 1, frame) != MYDB_OK)
            return false;
    }
    atomic_store(&wm->write_lsn, end);
    return true;
}

/* The writer against the reader: insert into a real WalManager, write
 * out, restart, and tail_scan must find exactly the end wal_insert
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
    const uint64_t R = (uint64_t)WAL_RING_FRAMES * WAL_PAGE_SIZE;   /* 4 MB */
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

typedef struct {
    WalManager   *wm;
    uint8_t      *buf;
    uint32_t      len;
    uint64_t      wc, last, end;
    int           rc;
    _Atomic bool  done;
} InsertJob;

static void *insert_job(void *arg)
{
    InsertJob *j = arg;
    j->rc = wal_insert(j->wm, j->buf, j->len, 0, &j->wc, &j->last, &j->end);
    atomic_store(&j->done, true);
    return NULL;
}

static bool job_done_within(InsertJob *j, int ms)
{
    for (int i = 0; i < ms && !atomic_load(&j->done); i++) usleep(1000);
    return atomic_load(&j->done);
}

static bool frame_is(const WalManager *wm, uint32_t frame, uint8_t byte)
{
    const uint8_t *f = wm->ring_buf + (size_t)frame * WAL_PAGE_SIZE;
    for (uint32_t i = 0; i < WAL_PAGE_SIZE; i++) if (f[i] != byte) return false;
    return true;
}

/* An insert that runs into lap 1's unwritten frame waits there, piece by
 * piece, until the write step (this test) has written and zeroed every
 * block its piece touches: not before, and not for the whole 2 KB line. */
static void test_space_check_waits(void)
{
    printf("\n[test_space_check_waits]\n");
    WalManager wm;
    ring_setup(&wm);
    const uint64_t R = (uint64_t)WAL_RING_FRAMES * WAL_PAGE_SIZE;

    /* Lap 1 left frame 0 full and not written: blocks 0 and 1 read 512. */
    memset(wm.ring_buf, 0xEE, WAL_PAGE_SIZE);
    atomic_store(&wm.fs_counters[0].done, 512);
    atomic_store(&wm.fs_counters[1].done, 512);

    /* One 4972-byte record from page 1023's first record byte: piece 1
     * fills page 1023 (ends at R, free), piece 2 is 900 bytes on page 1024,
     * frame 0 again, LSN [R + 24, R + 924): need = R + 1024. */
    static uint8_t buf[5000];
    make_records(buf, (uint32_t[]){4972}, 1);
    atomic_store(&wm.current_pos, 1023ull * WAL_PAGE_USABLE);

    InsertJob j = { .wm = &wm, .buf = buf, .len = 4972 };
    pthread_t th;
    if (pthread_create(&th, NULL, insert_job, &j) != 0) {
        CHECK(0, "start the inserting thread");
        ring_free(&wm);
        return;
    }

    bool last_page_full = true;
    CHECK(!job_done_within(&j, 100) && frame_is(&wm, 0, 0xEE) &&
          ctr(&wm, 0) == 512 && ctr(&wm, 1) == 512,
          "write_lsn 0: it waits before frame 0, which keeps lap 1's bytes and counters");
    for (uint32_t b = 1023 * 8; b < 1024 * 8; b++) if (ctr(&wm, b) != 512) last_page_full = false;
    CHECK(last_page_full, "while the first piece is already copied: page 1023's blocks all read 512");

    /* The write step passes block 0: zeroes it, then publishes. */
    atomic_store(&wm.fs_counters[0].done, 0);
    atomic_store(&wm.write_lsn, 512);
    CHECK(!job_done_within(&j, 100) && frame_is(&wm, 0, 0xEE),
          "write_lsn 512: still waits, its piece also touches block 1");

    /* It stops inside block 1, past the piece's own end (924) but in its
     * tail block, which it does not zero. */
    atomic_store(&wm.write_lsn, 930);
    CHECK(!job_done_within(&j, 100) && frame_is(&wm, 0, 0xEE) && ctr(&wm, 1) == 512,
          "write_lsn 930: still waits, for block 1's end (1024), not the piece's end (924)");

    atomic_store(&wm.fs_counters[1].done, 0);
    atomic_store(&wm.write_lsn, 1024);
    bool woke = job_done_within(&j, 5000);
    CHECK(woke, "write_lsn 1024: block 1 written and zeroed, the insert goes on");
    if (!woke) atomic_store(&wm.write_lsn, UINT64_MAX / 2);   /* let it finish, to join */
    pthread_join(th, NULL);

    WalPageHeader h;
    CHECK(j.rc == MYDB_OK && j.last == R - WAL_PAGE_SIZE + 24 && j.end == R + 924 && j.wc == 1024,
          "it returns its LSNs, and the session's copy is the write_lsn it waited for");
    CHECK(wal_page_header_deserialize(wm.ring_buf, &h) == MYDB_OK && h.start_lsn == R &&
          h.continuation_len == 900 && h.flags == WAL_PAGE_INCOMING_CONTINUATION &&
          memcmp(wm.ring_buf + 24, buf + 4072, 900) == 0,
          "frame 0 now holds page 1024: its header and the record's last 900 bytes");
    CHECK(ctr(&wm, 0) == 512 && ctr(&wm, 1) == 412,
          "counted from zero: block 0 = 24 + 488 = 512, block 1 = 924 - 512 = 412");
    ring_free(&wm);
}

/* ------------------------------------------------------------------
 * Concurrent inserts: sessions insert at once while a stand-in write step
 * drains the ring by the block rule, about two rings of log in all, and
 * the "disk" it wrote must be the one unbroken log the sessions meant.
 * ------------------------------------------------------------------ */
#define CI_SESSIONS        8
#define CI_BYTES_PER_SESS  (1u << 20)          /* 8 sessions: ~8 MB, two rings */
#define CI_MAX_REC         (44 + 12000)
#define CI_MAX_RECS        8000
#define CI_DISK_SIZE       (12u << 20)

typedef struct {
    WalManager   *wm;
    uint32_t      sid;
    uint64_t     *lsns;       /* record i's LSN, from its filled header */
    uint32_t     *lens;
    uint32_t      n_rec;
    uint64_t      max_end;
    bool          ok;
    _Atomic int  *finished;
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
 * 12 KB (across pages), until it has inserted CI_BYTES_PER_SESS. */
static void *ci_session(void *arg)
{
    CiSession *s   = arg;
    uint8_t   *buf = malloc(3 * CI_MAX_REC);
    uint64_t   prev = 0, wc = 0, total = 0;   /* the session's prev_lsn and write_lsn copy */
    s->ok = (buf != NULL);

    for (uint32_t k = 0; s->ok && total < CI_BYTES_PER_SESS; k++) {
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
        if (end > s->max_end) s->max_end = end;
        prev   = last;
        total += off;
    }
    free(buf);
    atomic_fetch_add(s->finished, 1);
    return NULL;
}

typedef struct {
    WalManager   *wm;
    uint8_t      *disk;       /* the "segment files": byte lsn at disk[lsn] */
    uint64_t      max_lag;    /* most the reservations got ahead of write_lsn */
    bool          ok;
    _Atomic bool  stop;
} CiWriter;

/* Stands in for the write step, by the block rule: a block that reads 512
 * is copied to disk, zeroed, and only then is write_lsn published past it
 * (release). It never zeroes the tail block, and stops at it once told
 * to. Slowed on purpose so the inserts fill the ring and wait. */
static void *ci_writer(void *arg)
{
    CiWriter   *w   = arg;
    WalManager *wm  = w->wm;
    uint64_t    R   = (uint64_t)wm->ring_buf_size << WAL_PAGE_SHIFT;
    uint64_t    pos = atomic_load(&wm->write_lsn);
    uint32_t    n   = 0;
    w->ok = true;

    for (;;) {
        _Atomic uint16_t *c = wal_block_counter(wm->fs_counters, wm->ctr_mask, pos);
        if (atomic_load_explicit(c, memory_order_acquire) == WAL_BLOCK_SIZE) {
            if (pos + WAL_BLOCK_SIZE > CI_DISK_SIZE) { w->ok = false; break; }
            memcpy(w->disk + pos, wm->ring_buf + (pos & (R - 1)), WAL_BLOCK_SIZE);
            atomic_store_explicit(c, 0, memory_order_relaxed);
            pos += WAL_BLOCK_SIZE;
            atomic_store_explicit(&wm->write_lsn, pos, memory_order_release);

            uint64_t reserved = wal_byte_to_lsn(atomic_load(&wm->current_pos));
            if (reserved > pos && reserved - pos > w->max_lag) w->max_lag = reserved - pos;
            if (++n % 8 == 0) usleep(200);
            continue;
        }
        if (atomic_load(&w->stop)) break;
        sched_yield();
    }
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

static void test_insert_concurrent(void)
{
    printf("\n[test_insert_concurrent]\n");
    WalManager wm;
    ring_setup(&wm);
    const uint64_t R = (uint64_t)WAL_RING_FRAMES * WAL_PAGE_SIZE;

    _Atomic int finished = 0;
    CiSession   s[CI_SESSIONS];
    CiWriter    w = { .wm = &wm, .disk = calloc(1, CI_DISK_SIZE) };
    uint8_t    *rec = malloc(CI_MAX_REC);
    bool        mem = w.disk && rec;
    for (uint32_t i = 0; i < CI_SESSIONS; i++) {
        s[i] = (CiSession){ .wm = &wm, .sid = i, .finished = &finished,
                            .lsns = malloc(CI_MAX_RECS * sizeof(uint64_t)),
                            .lens = malloc(CI_MAX_RECS * sizeof(uint32_t)) };
        if (!s[i].lsns || !s[i].lens) mem = false;
    }

    pthread_t wth, th[CI_SESSIONS];
    uint32_t  started = 0;
    bool      writer_up = mem && pthread_create(&wth, NULL, ci_writer, &w) == 0;
    if (writer_up)
        for (; started < CI_SESSIONS; started++)
            if (pthread_create(&th[started], NULL, ci_session, &s[started]) != 0) break;
    CHECK(writer_up && started == CI_SESSIONS, "memory, the writer and 8 session threads");
    if (!writer_up || started < CI_SESSIONS) {
        /* Nothing is checked without all of them; let what started end. */
        atomic_store(&wm.write_lsn, UINT64_MAX / 2);
        for (uint32_t i = 0; i < started; i++) pthread_join(th[i], NULL);
        if (writer_up) { atomic_store(&w.stop, true); pthread_join(wth, NULL); }
        goto out;
    }

    /* A session stuck in the space wait would hang the test: give up
     * loudly after 60 s instead. */
    for (int i = 0; i < 60000 && atomic_load(&finished) < CI_SESSIONS; i++) usleep(1000);
    if (atomic_load(&finished) < CI_SESSIONS) {
        CHECK(0, "every session finished within 60 s (none stuck waiting for space)");
        printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
        exit(1);
    }
    for (uint32_t i = 0; i < CI_SESSIONS; i++) pthread_join(th[i], NULL);
    atomic_store(&w.stop, true);
    pthread_join(wth, NULL);

    bool     calls_ok = w.ok;
    uint64_t max_end  = 0;
    for (uint32_t i = 0; i < CI_SESSIONS; i++) {
        if (!s[i].ok) calls_ok = false;
        if (s[i].max_end > max_end) max_end = s[i].max_end;
    }
    uint64_t end_lsn = wal_lsn_advance(WAL_FIRST_RECORD_LSN, atomic_load(&wm.current_pos));
    CHECK(calls_ok && max_end == end_lsn,
          "every call OK, its LSNs match its filled headers, and the latest end is the log's end");
    CHECK(end_lsn > 2 * R, "more than two rings of log went through the ring");
    CHECK(w.max_lag > R,
          "the ring filled: reservations ran more than a ring ahead of write_lsn, so inserts waited");

    /* The tail: the write step stops in the last block; take it as it is. */
    uint64_t wl      = atomic_load(&wm.write_lsn);
    bool     tail_ok = wl <= end_lsn && end_lsn - wl < WAL_BLOCK_SIZE;
    if (tail_ok && end_lsn > wl) {
        tail_ok = ctr(&wm, (uint32_t)((wl >> WAL_BLOCK_SHIFT) & wm.ctr_mask)) == end_lsn - wl;
        memcpy(w.disk + wl, wm.ring_buf + (wl & (R - 1)), end_lsn - wl);
    }
    CHECK(tail_ok, "the write step stops only in the tail block, which holds exactly the last bytes");
    bool zeroed = true;
    for (uint32_t i = 0; i <= wm.ctr_mask; i++)
        if (i != ((wl >> WAL_BLOCK_SHIFT) & wm.ctr_mask) && ctr(&wm, i) != 0) zeroed = false;
    CHECK(zeroed, "every other counter is back to zero");

    /* Walk the disk from LSN 24 by total_len, as recovery would. */
    uint32_t  pages    = (uint32_t)((end_lsn + WAL_PAGE_SIZE - 1) >> WAL_PAGE_SHIFT);
    uint16_t *exp_cont = calloc(pages, sizeof(uint16_t));
    uint32_t  next_seq[CI_SESSIONS] = {0};
    bool      walk_ok = exp_cont != NULL, owner_ok = true, body_ok = true;
    uint64_t  lsn = WAL_FIRST_RECORD_LSN;
    while (walk_ok && lsn < end_lsn) {
        if (wal_lsn_offset(lsn) == 0) lsn += WAL_PAGE_HEADER_SIZE;
        uint32_t total_len;
        disk_read(w.disk, lsn, rec, WAL_RECORD_HEADER_SIZE);
        memcpy(&total_len, rec + offsetof(WalRecordHeader, total_len), 4);
        if (total_len < WAL_RECORD_HEADER_SIZE || total_len > CI_MAX_REC) { walk_ok = false; break; }
        uint64_t next = disk_read(w.disk, lsn, rec, total_len);

        WalRecordHeader h;
        if (wal_record_header_deserialize(rec, total_len - WAL_RECORD_HEADER_SIZE, &h) != MYDB_OK) {
            walk_ok = false;
            break;
        }
        uint32_t sid = h.table_id, seq = h.page_no;
        if (sid >= CI_SESSIONS || seq != next_seq[sid] || seq >= s[sid].n_rec ||
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
    for (uint32_t i = 0; i < CI_SESSIONS; i++) if (next_seq[i] != s[i].n_rec) owner_ok = false;
    CHECK(walk_ok && lsn == end_lsn,
          "the disk is one unbroken log from LSN 24 to the end: every record found by total_len, every CRC valid");
    CHECK(owner_ok, "each session's records are all there, once, in its order, at the LSNs it was given, prev_lsn chained");
    CHECK(body_ok, "every record's body is the bytes its session wrote");

    bool pages_ok = walk_ok;
    for (uint32_t p = 0; pages_ok && p < pages; p++) {
        WalPageHeader ph;
        if (wal_page_header_deserialize(w.disk + (uint64_t)p * WAL_PAGE_SIZE, &ph) != MYDB_OK ||
            ph.start_lsn != (uint64_t)p * WAL_PAGE_SIZE || ph.continuation_len != exp_cont[p] ||
            ph.flags != (exp_cont[p] ? WAL_PAGE_INCOMING_CONTINUATION : 0))
            pages_ok = false;
    }
    CHECK(pages_ok, "every page header: start_lsn, continuation_len and flag");
    free(exp_cont);

out:
    for (uint32_t i = 0; i < CI_SESSIONS; i++) { free(s[i].lsns); free(s[i].lens); }
    free(rec);
    free(w.disk);
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
    test_space_check_waits();
    test_insert_concurrent();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
