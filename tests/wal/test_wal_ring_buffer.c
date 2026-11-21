#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "common.h"
#include "wal_types.h"
#include "wal_page.h"
#include "wal_segment_pool.h"
#include "wal_ring_buffer.h"

#define TEST_WAL_DIR "/tmp/mydb_test_wal_ring_buffer"

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

static void cleanup(void)
{
    char path[300];
    for (uint32_t i = 0; i < WAL_SEGMENT_POOL_SLOTS; i++) {
        snprintf(path, sizeof(path), "%s/wal_%u.mydb", TEST_WAL_DIR, i);
        unlink(path);
    }
    rmdir(TEST_WAL_DIR);
}

static WalRecordHeader make_header(uint64_t prev_lsn, uint64_t txn_id)
{
    WalRecordHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.prev_lsn  = prev_lsn;
    hdr.txn_id    = txn_id;
    hdr.table_id  = 5;
    hdr.page_no   = 7;
    hdr.rec_type  = WAL_REC_INSERT;
    return hdr;
}

/* The frame holding data page k, and its header. */
static uint8_t *frame_k(WalRingBuffer *rb, uint64_t k)
{
    return rb->buf + (size_t)(k % WAL_RING_BUFFER_FRAMES) * WAL_PAGE_SIZE;
}
static int page_hdr(WalRingBuffer *rb, uint64_t k, WalPageHeader *out)
{
    return wal_page_header_deserialize(frame_k(rb, k), out);
}

/* Reassembles n record bytes starting at data position lsn out of the
 * ring, hopping over page headers (and segment page 0) the way a reader
 * of the log would. */
static void read_bytes(WalRingBuffer *rb, uint64_t lsn, uint8_t *out, size_t n)
{
    uint64_t pos = lsn;
    while (n > 0) {
        uint32_t off = wal_lsn_offset(pos);
        size_t   c   = WAL_PAGE_SIZE - off;
        if (c > n) c = n;
        memcpy(out, frame_k(rb, wal_lsn_data_page(pos)) + off, c);
        out += c; n -= c; pos += c;
        if (n > 0 && wal_lsn_offset(pos) == 0) pos = wal_lsn_next_data(pos);
    }
}

/* Reads a whole record back out of the ring and validates its CRC. */
static int record_ok(WalRingBuffer *rb, uint64_t lsn, size_t total_len, WalRecordHeader *out)
{
    uint8_t *buf = malloc(total_len);
    read_bytes(rb, lsn, buf, total_len);
    int rc = wal_record_header_deserialize(buf, total_len - WAL_RECORD_HEADER_SIZE, out);
    free(buf);
    return rc == MYDB_OK && out->lsn == lsn && out->total_len == total_len;
}

/* ------------------------------------------------------------------ */

static void test_lsn_arithmetic(void)
{
    printf("\n[test_lsn_arithmetic]\n");
    CHECK(WAL_FIRST_RECORD_LSN == 4120, "first record of the log is at 4120 (page 0 + page 1's header)");
    CHECK(wal_data_page_base(0) == 4096, "data page 0 is segment 0, page 1");
    CHECK(wal_data_page_base(511) == WAL_SEG_SIZE + 4096, "data page 511 is segment 1, page 1 (page 0 skipped)");
    CHECK(wal_lsn_advance(4120, 4072) == 8192, "4072 bytes from 4120 end exactly at page 1's end");
    CHECK(wal_lsn_advance(4120, 4073) == 8192 + 24 + 1, "one more byte goes past page 2's header");
    CHECK(wal_lsn_next_data(8192) == 8216, "next data byte after a page end is past the next header");
    CHECK(wal_lsn_next_data(WAL_SEG_SIZE) == WAL_SEG_SIZE + 4120,
          "after a segment end the next data byte steps over the new segment's page 0");
    CHECK(wal_lsn_advance(wal_data_page_base(510) + 24, 4072) == WAL_SEG_SIZE,
          "the last data page of segment 0 ends exactly at segment 1's start");

    /* The worked example from the design discussion. */
    uint64_t lsn = 12323434242422237ULL;
    CHECK(wal_lsn_segment(lsn) == 5876271363ULL && wal_lsn_page(lsn) == 235 && wal_lsn_offset(lsn) == 1501,
          "12,323,434,242,422,237 is segment 5,876,271,363, page 235, offset 1,501");
    CHECK(wal_lsn_data_page(lsn) == 3002774666727ULL, "it is data page 3,002,774,666,727");
    CHECK(wal_lsn_data_page(lsn) % WAL_RING_BUFFER_FRAMES == 3559, "which lives in ring frame 3,559");
    CHECK(wal_data_ord_lsn(wal_lsn_data_ord(lsn)) == lsn, "data_ord and data_ord_lsn are inverses");
}

static void test_single_append(void)
{
    printf("\n[test_single_append]\n");
    WalRingBuffer rb;
    CHECK(wal_ring_buffer_init(&rb, WAL_FIRST_RECORD_LSN) == MYDB_OK, "init at 4120 succeeds");
    CHECK(rb.current_lsn == 4120 && rb.drained_lsn == 4096, "writer at 4120, reader at page 1's base");
    CHECK(wal_ring_buffer_init(&rb, 4096) == MYDB_ERR, "init on a header position is refused");

    WalRecordHeader hdr = make_header(0, 1);
    uint8_t body[20] = {0};
    uint64_t lsn, end;
    int rc = wal_ring_buffer_append(&rb, &hdr, body, sizeof(body), &lsn, &end);

    CHECK(rc == MYDB_OK, "append succeeds");
    CHECK(lsn == 4120, "first record gets lsn 4120");
    CHECK(end == 4120 + WAL_RECORD_HEADER_SIZE + sizeof(body), "end = lsn + total_len on one page");
    CHECK(rb.current_lsn == end, "current_lsn moves to the record's end");
    CHECK(!(hdr.flags & WAL_RECORD_FLAG_CONTINUES_ON_NEW_PAGE), "a record on one page has no continuation flag");

    WalPageHeader ph;
    CHECK(page_hdr(&rb, 0, &ph) == MYDB_OK, "the open frame's header is valid right after the append");
    CHECK(ph.start_lsn == 4096 && ph.continuation_len == 0 && ph.flags == 0,
          "header: start_lsn = page base, no continuation");

    WalRecordHeader back;
    CHECK(record_ok(&rb, lsn, 64, &back), "the record reads back and its CRC checks");

    wal_ring_buffer_shutdown(&rb);
}

static void test_lsn_skips_page_headers(void)
{
    printf("\n[test_lsn_skips_page_headers]\n");
    WalRingBuffer rb;
    wal_ring_buffer_init(&rb, WAL_FIRST_RECORD_LSN);

    /* 40 records of 100 bytes fill 4000 of page 1's 4072; the 41st spans. */
    uint64_t lsn = 0, end = 0;
    uint8_t body[56] = {0};
    for (int i = 0; i < 40; i++) {
        WalRecordHeader h = make_header(0, 1);
        wal_ring_buffer_append(&rb, &h, body, sizeof(body), &lsn, &end);
    }
    CHECK(end == 4120 + 4000, "40 x 100 bytes end at 8120");

    WalRecordHeader h = make_header(0, 1);
    memset(body, 0xAB, sizeof(body));
    wal_ring_buffer_append(&rb, &h, body, sizeof(body), &lsn, &end);
    CHECK(lsn == 8120, "the 41st record starts at 8120 (72 bytes left on page 1)");
    CHECK(end == 8192 + 24 + 28, "and ends 28 bytes into page 2, past page 2's header");
    CHECK(h.flags & WAL_RECORD_FLAG_CONTINUES_ON_NEW_PAGE, "its header carries the continuation flag");

    WalPageHeader p1, p2;
    page_hdr(&rb, 0, &p1);
    page_hdr(&rb, 1, &p2);
    CHECK(p1.continuation_len == 0 && p1.flags == 0,
          "page 1 keeps the header it was opened with (no OUT flag any more)");
    CHECK(p2.start_lsn == 8192 && p2.continuation_len == 28 &&
          p2.flags == WAL_PAGE_INCOMING_CONTINUATION,
          "page 2: start_lsn 8192, continuation_len 28, IN");

    WalRecordHeader back;
    CHECK(record_ok(&rb, lsn, 100, &back), "the split record reassembles and its CRC checks");
    CHECK(back.flags & WAL_RECORD_FLAG_CONTINUES_ON_NEW_PAGE, "the flag is inside the CRC-covered bytes");

    wal_ring_buffer_shutdown(&rb);
}

static void test_record_spanning_five_pages(void)
{
    printf("\n[test_record_spanning_five_pages]\n");
    WalRingBuffer rb;
    wal_ring_buffer_init(&rb, WAL_FIRST_RECORD_LSN);

    size_t body_len = 16384;                         /* total 16,428 bytes */
    uint8_t *body = malloc(body_len);
    for (size_t i = 0; i < body_len; i++) body[i] = (uint8_t)(i * 7);
    WalRecordHeader h = make_header(0, 9);
    uint64_t lsn, end;
    CHECK(wal_ring_buffer_append(&rb, &h, body, body_len, &lsn, &end) == MYDB_OK, "a 16 KB record appends");
    CHECK(end == wal_data_page_base(4) + 24 + (16428 - 4 * 4072), "it ends 140 bytes into the 5th page");

    WalPageHeader p;
    page_hdr(&rb, 0, &p);
    CHECK(p.flags == 0 && p.continuation_len == 0, "page 1: no flags, the record starts here");
    int mid_ok = 1;
    for (uint64_t k = 1; k <= 3; k++) {
        page_hdr(&rb, k, &p);
        mid_ok &= (p.flags == WAL_PAGE_INCOMING_CONTINUATION
                   && p.continuation_len == 4072 && p.start_lsn == wal_data_page_base(k));
    }
    CHECK(mid_ok, "pages 2-4: IN, continuation_len = 4072 (the record passes through)");
    page_hdr(&rb, 4, &p);
    CHECK(p.flags == WAL_PAGE_INCOMING_CONTINUATION && p.continuation_len == 140,
          "page 5: IN, continuation_len = 140");

    WalRecordHeader back;
    CHECK(record_ok(&rb, lsn, 16428, &back), "the 5-page record reassembles and its CRC checks");

    free(body);
    wal_ring_buffer_shutdown(&rb);
}

static void test_exact_page_end(void)
{
    printf("\n[test_exact_page_end]\n");
    WalRingBuffer rb;
    wal_ring_buffer_init(&rb, WAL_FIRST_RECORD_LSN);

    uint8_t *body = calloc(1, WAL_PAGE_USABLE);
    WalRecordHeader h = make_header(0, 1);
    uint64_t lsn, end;
    wal_ring_buffer_append(&rb, &h, body, WAL_PAGE_USABLE - WAL_RECORD_HEADER_SIZE, &lsn, &end);
    CHECK(end == 8192 && rb.current_lsn == 8192, "a record filling page 1 exactly ends on the page end");
    CHECK(!(h.flags & WAL_RECORD_FLAG_CONTINUES_ON_NEW_PAGE), "it does not continue");

    WalPageHeader p;
    page_hdr(&rb, 0, &p);
    CHECK(p.continuation_len == 0 && p.flags == 0, "page 1 is full and keeps the header it was opened with");
    CHECK(page_hdr(&rb, 1, &p) != MYDB_OK, "page 2 is not opened yet (lazy)");

    uint8_t scratch[2 * WAL_PAGE_SIZE];
    uint32_t n = 0;
    WalCursor cur;
    wal_ring_buffer_snapshot(&rb, scratch, 2, &n, &cur);
    CHECK(n == 1 && cur.lsn == 8192, "snapshot takes page 1: it is closed even though nothing follows");

    WalRecordHeader h2 = make_header(0, 1);
    uint8_t small[10] = {0};
    wal_ring_buffer_append(&rb, &h2, small, sizeof(small), &lsn, &end);
    CHECK(lsn == 8216, "the next record starts past page 2's header");
    page_hdr(&rb, 1, &p);
    CHECK(p.start_lsn == 8192 && p.continuation_len == 0 && p.flags == 0,
          "page 2 opened then, with no continuation");

    free(body);
    wal_ring_buffer_shutdown(&rb);
}

static void test_segment_boundary(void)
{
    printf("\n[test_segment_boundary]\n");
    WalRingBuffer rb;
    uint64_t start = wal_data_page_base(510) + 24;   /* segment 0, page 511 */
    CHECK(wal_ring_buffer_init(&rb, start) == MYDB_OK, "init on segment 0's last page");

    size_t body_len = 5000 - WAL_RECORD_HEADER_SIZE;
    uint8_t *body = malloc(body_len);
    memset(body, 0x5A, body_len);
    WalRecordHeader h = make_header(0, 3);
    uint64_t lsn, end;
    wal_ring_buffer_append(&rb, &h, body, body_len, &lsn, &end);
    CHECK(lsn == start, "the record starts on page 511");
    CHECK(end == WAL_SEG_SIZE + 4096 + 24 + (5000 - 4072),
          "it ends in segment 1, page 1 — page 0 (the segment header) was stepped over");

    WalPageHeader p;
    CHECK(page_hdr(&rb, 511, &p) == MYDB_OK && p.start_lsn == WAL_SEG_SIZE + 4096 &&
          p.continuation_len == 928 && p.flags == WAL_PAGE_INCOMING_CONTINUATION,
          "segment 1 page 1 (data page 511, frame 511): IN, continuation_len 928");

    WalRecordHeader back;
    CHECK(record_ok(&rb, lsn, 5000, &back), "the record across two segments reassembles");

    free(body);
    wal_ring_buffer_shutdown(&rb);
}

static void test_ring_wrap(void)
{
    printf("\n[test_ring_wrap]\n");
    WalRingBuffer rb;
    uint64_t k = 4100;
    wal_ring_buffer_init(&rb, wal_data_page_base(k) + 24);

    WalRecordHeader h = make_header(0, 1);
    uint8_t body[16];
    memset(body, 0x77, sizeof(body));
    uint64_t lsn, end;
    wal_ring_buffer_append(&rb, &h, body, sizeof(body), &lsn, &end);

    WalPageHeader p;
    wal_page_header_deserialize(rb.buf + 4 * WAL_PAGE_SIZE, &p);
    CHECK(p.start_lsn == wal_data_page_base(4100), "data page 4100 lives in physical frame 4 (4100 % 4096)");
    CHECK(rb.buf[4 * WAL_PAGE_SIZE + 24 + WAL_RECORD_HEADER_SIZE] == 0x77, "and its record bytes are there");

    wal_ring_buffer_shutdown(&rb);
}

static void test_full_ring(void)
{
    printf("\n[test_full_ring]\n");
    WalRingBuffer rb;
    wal_ring_buffer_init(&rb, WAL_FIRST_RECORD_LSN);

    /* One-page records: record i fills data page i exactly. */
    size_t body_len = WAL_PAGE_USABLE - WAL_RECORD_HEADER_SIZE;
    uint8_t *body = calloc(1, body_len);
    uint64_t lsn, end;
    int rc = MYDB_OK, count = 0;
    while (count < WAL_RING_BUFFER_FRAMES + 10) {
        WalRecordHeader h = make_header(0, 1);
        rc = wal_ring_buffer_append(&rb, &h, body, body_len, &lsn, &end);
        if (rc != MYDB_OK) break;
        count++;
    }
    CHECK(rc == MYDB_ERR_FULL, "append returns FULL once every frame holds an un-drained page");
    CHECK(count == WAL_RING_BUFFER_FRAMES, "exactly 4096 one-page records fit");
    uint64_t before = rb.current_lsn;

    WalRecordHeader h = make_header(0, 1);
    CHECK(wal_ring_buffer_append(&rb, &h, body, body_len, &lsn, &end) == MYDB_ERR_FULL &&
          rb.current_lsn == before, "a refused append writes nothing");

    uint8_t *scratch = malloc(WAL_RING_BUFFER_SIZE);
    uint32_t n = 0;
    WalCursor cur;
    wal_ring_buffer_snapshot(&rb, scratch, 8, &n, &cur);
    CHECK(n == 8 && rb.drained_lsn == wal_data_page_base(8), "draining 8 pages moves drained_lsn by 8 pages");
    CHECK(wal_ring_buffer_append_wait(&rb, body_len + WAL_RECORD_HEADER_SIZE) == MYDB_OK,
          "append_wait returns at once now that there is room");
    CHECK(wal_ring_buffer_append(&rb, &h, body, body_len, &lsn, &end) == MYDB_OK,
          "and the append goes through");
    CHECK(lsn == wal_data_page_base(4096) + 24, "into data page 4096, which reuses frame 0");

    free(scratch);
    free(body);
    wal_ring_buffer_shutdown(&rb);
}

static void test_end_to_end_through_pool(void)
{
    printf("\n[test_end_to_end_through_pool]\n");
    cleanup();

    WalSegmentPool pool;
    CHECK(wal_segment_pool_init(&pool, TEST_WAL_DIR, 1) == MYDB_OK, "segment pool init succeeds");
    uint32_t seg_slot, seg_page = 1, seg_offset = 0;
    CHECK(wal_segment_pool_claim_next(&pool, &seg_slot) == MYDB_OK, "segment claim succeeds");

    WalRingBuffer rb;
    wal_ring_buffer_init(&rb, wal_segment_first_lsn(pool.slots[seg_slot].header.segment_no));

    for (int i = 0; i < 200; i++) {
        WalRecordHeader hdr = make_header(0, 1);
        uint8_t body[80];
        memset(body, (uint8_t)i, sizeof(body));
        uint64_t lsn, end;
        if (wal_ring_buffer_append(&rb, &hdr, body, sizeof(body), &lsn, &end) != MYDB_OK) {
            CHECK(0, "append in the loop");
            break;
        }
    }
    /* 200 x 124 = 24,800 bytes = 6 full pages + part of a 7th. */
    uint8_t *scratch = malloc(16 * WAL_PAGE_SIZE);
    uint32_t n = 0;
    WalCursor cur;
    CHECK(wal_ring_buffer_snapshot(&rb, scratch, 16, &n, &cur) == MYDB_OK && n == 6,
          "snapshot copies the 6 closed pages, not the open 7th");
    CHECK(cur.lsn == wal_data_page_base(5) + WAL_PAGE_SIZE, "and reports the end of the 6th page");

    int ok = 1;
    for (uint32_t i = 0; i < n; i++)
        ok &= (wal_segment_pool_write(&pool, NULL, &seg_slot, &seg_page, &seg_offset,
                                      scratch + (size_t)i * WAL_PAGE_SIZE, WAL_PAGE_SIZE) == MYDB_OK);
    CHECK(ok, "the pages write to the segment");

    uint8_t from_disk[WAL_PAGE_SIZE];
    CHECK(wal_segment_pool_read_page(&pool, seg_slot, 1, from_disk) == MYDB_OK &&
          memcmp(from_disk, scratch, WAL_PAGE_SIZE) == 0,
          "segment page 1 on disk is byte-identical to the ring's first page");
    WalPageHeader p;
    CHECK(wal_page_header_deserialize(from_disk, &p) == MYDB_OK && p.start_lsn == 4096,
          "and its start_lsn is its own position in the log");

    free(scratch);
    wal_segment_pool_shutdown(&pool);
    wal_ring_buffer_shutdown(&rb);
    cleanup();
}

/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_wal_ring_buffer ===\n");

    test_lsn_arithmetic();
    test_single_append();
    test_lsn_skips_page_headers();
    test_record_spanning_five_pages();
    test_exact_page_end();
    test_segment_boundary();
    test_ring_wrap();
    test_full_ring();
    test_end_to_end_through_pool();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
