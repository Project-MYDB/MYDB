#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "common.h"
#include "wal_page.h"
#include "wal_segment.h"
#include "wal_segment_pool.h"
#include "wal_types.h"

#define TEST_WAL_DIR "/tmp/mydb_test_wal_pool"

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

/* Remove every wal_<i>.mydb under TEST_WAL_DIR, then the directory
 * itself. Safe to call whether or not the directory exists yet. */
static void cleanup(void)
{
    char path[300];
    for (uint32_t i = 0; i < WAL_SEGMENT_POOL_SLOTS; i++) {
        snprintf(path, sizeof(path), "%s/wal_%u.mydb", TEST_WAL_DIR, i);
        unlink(path);
    }
    rmdir(TEST_WAL_DIR);
}

/* ------------------------------------------------------------------ */

static void test_fresh_init_creates_all_slots(void)
{
    printf("\n[test_fresh_init_creates_all_slots]\n");
    cleanup();

    WalSegmentPool pool;
    int rc = wal_segment_pool_init(&pool, TEST_WAL_DIR, 7);
    CHECK(rc == MYDB_OK, "fresh init succeeds");

    int all_free = 1, all_right_size = 1;
    for (uint32_t i = 0; i < WAL_SEGMENT_POOL_SLOTS; i++) {
        char path[300];
        snprintf(path, sizeof(path), "%s/wal_%u.mydb", TEST_WAL_DIR, i);
        struct stat st;
        if (stat(path, &st) != 0 || st.st_size != WAL_SEGMENT_FILE_SIZE)
            all_right_size = 0;
        if (pool.slots[i].header.state != SEG_FREE)
            all_free = 0;
    }
    CHECK(all_right_size, "every slot file is exactly WAL_SEGMENT_FILE_SIZE bytes");
    CHECK(all_free, "every slot reloads as SEG_FREE");
    CHECK(pool.next_segment_no == 0, "next_segment_no starts at 0 on a fresh pool");

    wal_segment_pool_shutdown(&pool);
    cleanup();
}

static void test_claim_next_round_robins(void)
{
    printf("\n[test_claim_next_round_robins]\n");
    cleanup();

    WalSegmentPool pool;
    CHECK(wal_segment_pool_init(&pool, TEST_WAL_DIR, 1) == MYDB_OK, "init succeeds");

    int ok = 1;
    for (uint32_t i = 0; i < WAL_SEGMENT_POOL_SLOTS; i++) {
        uint32_t slot_index;
        if (wal_segment_pool_claim_next(&pool, &slot_index) != MYDB_OK) { ok = 0; break; }
        if (slot_index != i) { ok = 0; break; }
        if (pool.slots[slot_index].header.segment_no != i) { ok = 0; break; }
        if (pool.slots[slot_index].header.state != SEG_ACTIVE) { ok = 0; break; }
        if (pool.slots[slot_index].header.start_lsn != wal_segment_first_lsn(i)) { ok = 0; break; }
    }
    CHECK(ok, "10 successive claims assign slot_index == segment_no == 0..9 in order, "
              "each stamped with start_lsn = its first record's LSN");

    uint32_t eleventh;
    int rc = wal_segment_pool_claim_next(&pool, &eleventh);
    CHECK(rc == MYDB_ERR, "an 11th claim fails: every slot is used and none has been freed yet");

    wal_segment_pool_shutdown(&pool);
    cleanup();
}

static void test_page_round_trip_and_bounds(void)
{
    printf("\n[test_page_round_trip_and_bounds]\n");
    cleanup();

    WalSegmentPool pool;
    CHECK(wal_segment_pool_init(&pool, TEST_WAL_DIR, 1) == MYDB_OK, "init succeeds");

    uint32_t slot_index;
    CHECK(wal_segment_pool_claim_next(&pool, &slot_index) == MYDB_OK, "claim succeeds");

    WalPageHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.id.file_type = FILETYPE_WAL_PAGE;
    hdr.start_lsn = 99;
    hdr.continuation_len = 123;   /* (was data_len, dropped from the page header) */

    uint8_t page_buf[WAL_PAGE_SIZE];
    memset(page_buf, 0, WAL_PAGE_SIZE);
    wal_page_header_serialize(&hdr, page_buf);

    CHECK(wal_segment_pool_write_page(&pool, slot_index, 1, page_buf) == MYDB_OK,
          "write_page(page_no=1) succeeds");

    uint8_t read_buf[WAL_PAGE_SIZE];
    CHECK(wal_segment_pool_read_page(&pool, slot_index, 1, read_buf) == MYDB_OK,
          "read_page(page_no=1) succeeds");

    WalPageHeader out;
    int rc = wal_page_header_deserialize(read_buf, &out);
    CHECK(rc == MYDB_OK, "the page read back deserializes cleanly");
    CHECK(out.start_lsn == 99, "start_lsn round-trips through the pool");

    CHECK(wal_segment_pool_write_page(&pool, slot_index, 0, page_buf) == MYDB_ERR,
          "page_no=0 (the header's own slot) is rejected");
    CHECK(wal_segment_pool_write_page(&pool, slot_index, WAL_SEGMENT_PAGES_PER_FILE, page_buf) == MYDB_ERR,
          "page_no == WAL_SEGMENT_PAGES_PER_FILE is out of range and rejected");

    wal_segment_pool_shutdown(&pool);
    cleanup();
}

static void test_clean_reload_preserves_state(void)
{
    printf("\n[test_clean_reload_preserves_state]\n");
    cleanup();

    WalSegmentPool pool;
    CHECK(wal_segment_pool_init(&pool, TEST_WAL_DIR, 1) == MYDB_OK, "first init succeeds");

    uint32_t slot_index;
    wal_segment_pool_claim_next(&pool, &slot_index);
    wal_segment_pool_claim_next(&pool, &slot_index);
    wal_segment_pool_claim_next(&pool, &slot_index);
    wal_segment_pool_shutdown(&pool);

    WalSegmentPool reloaded;
    CHECK(wal_segment_pool_init(&reloaded, TEST_WAL_DIR, 1) == MYDB_OK, "reload succeeds");
    CHECK(reloaded.next_segment_no == 3, "next_segment_no survives a clean reload (3 claims made)");
    CHECK(reloaded.slots[0].header.state == SEG_ACTIVE &&
          reloaded.slots[1].header.state == SEG_ACTIVE &&
          reloaded.slots[2].header.state == SEG_ACTIVE,
          "the 3 previously-claimed slots reload as SEG_ACTIVE");
    CHECK(reloaded.slots[3].header.state == SEG_FREE, "an untouched slot reloads as SEG_FREE");

    wal_segment_pool_shutdown(&reloaded);
    cleanup();
}

/* tail_scan finds the end of the log at start-up. It picks the active
 * segment (the ACTIVE slot with the highest segment_no) and reports
 * NOT_FOUND for a brand-new log. (It used to count valid pages into
 * data_pages.) */
static void test_tail_scan_picks_the_active_segment(void)
{
    printf("\n[test_tail_scan_picks_the_active_segment]\n");
    cleanup();

    WalSegmentPool pool;
    CHECK(wal_segment_pool_init(&pool, TEST_WAL_DIR, 1) == MYDB_OK, "init succeeds");

    uint32_t slot = 99;
    uint64_t end  = 0;
    CHECK(wal_segment_pool_tail_scan(&pool, &slot, &end) == MYDB_ERR_NOT_FOUND && slot == 99,
          "a brand-new log has no ACTIVE segment: NOT_FOUND, slot untouched");

    /* Segments 0 and 1 both ACTIVE: what the old init, which claimed a
     * fresh segment on every start, could leave behind. */
    uint32_t s0, s1;
    CHECK(wal_segment_pool_claim_next(&pool, &s0) == MYDB_OK &&
          wal_segment_pool_claim_next(&pool, &s1) == MYDB_OK, "claim segments 0 and 1");
    CHECK(wal_segment_pool_tail_scan(&pool, &slot, &end) == MYDB_OK && slot == s0 && end == 0,
          "segment 1 is the active one, so the walk starts at segment 0: unwritten, it ends at LSN 0, in slot 0");

    /* The same after a crash: reload sees both still ACTIVE. */
    wal_segment_pool_shutdown(&pool);
    WalSegmentPool reloaded;
    CHECK(wal_segment_pool_init(&reloaded, TEST_WAL_DIR, 1) == MYDB_OK, "post-crash reload succeeds");
    slot = 99;
    CHECK(wal_segment_pool_tail_scan(&reloaded, &slot, &end) == MYDB_OK && slot == s0 && end == 0,
          "the same after reload");

    wal_segment_pool_shutdown(&reloaded);
    cleanup();
}

#include "wal_test_log.h"

/* Fresh pool with segment 0 claimed; returns its slot. */
static uint32_t ts_setup(WalSegmentPool *pool)
{
    cleanup();
    uint32_t slot = 0;
    wal_segment_pool_init(pool, TEST_WAL_DIR, 1);
    wal_segment_pool_claim_next(pool, &slot);
    return slot;
}

static uint64_t ts_end(WalSegmentPool *pool, uint32_t *slot)
{
    uint64_t end = UINT64_MAX;
    if (wal_segment_pool_tail_scan(pool, slot, &end) != MYDB_OK) return UINT64_MAX;
    return end;
}

/* V2 resume 4b-2a: tail_scan walks the active segment's records and
 * reports just past the last valid one. */
static void test_tail_scan_finds_the_end(void)
{
    printf("\n[test_tail_scan_finds_the_end]\n");
    WalSegmentPool pool;
    TestLog tl;
    uint32_t slot, s0;

    /* Nothing written: the segment's own start. */
    s0 = ts_setup(&pool);
    CHECK(ts_end(&pool, &slot) == 0 && slot == s0, "an unwritten segment 0 ends at LSN 0");
    wal_segment_pool_shutdown(&pool);

    /* Three small records. */
    s0 = ts_setup(&pool);
    tl_begin(&tl, 0, 0);
    tl_record(&tl, 100, 0x11); tl_record(&tl, 200, 0x22); tl_record(&tl, 300, 0x33);
    tl_write(&tl, &pool, s0);
    CHECK(ts_end(&pool, &slot) == tl.pos && tl.pos == 24 + 3 * 44 + 600,
          "three records: the end is just past the third (LSN 756)");
    wal_segment_pool_shutdown(&pool);

    /* A record spanning three pages, then a small one. */
    s0 = ts_setup(&pool);
    tl_begin(&tl, 0, 0);
    tl_record(&tl, 100, 0x11);
    uint64_t big = tl_record(&tl, 9000, 0x22);
    tl_record(&tl, 50, 0x33);
    tl_write(&tl, &pool, s0);
    CHECK(ts_end(&pool, &slot) == tl.pos && wal_lsn_page(big) == 0 && tl.pos > 2 * 4096,
          "a 9 KB record spanning three pages, then one more: the end is past the last");
    wal_segment_pool_shutdown(&pool);

    /* A record that fills page 0 exactly: the end is the page end, 4096. */
    s0 = ts_setup(&pool);
    tl_begin(&tl, 0, 0);
    tl_record(&tl, WAL_PAGE_USABLE - WAL_RECORD_HEADER_SIZE, 0x11);
    tl_write(&tl, &pool, s0);
    CHECK(tl.pos == 4096 && ts_end(&pool, &slot) == 4096,
          "a record filling page 0 exactly ends at the page end, 4096 (page 1 unwritten)");
    uint64_t next = tl_record(&tl, 10, 0x22);
    tl_write(&tl, &pool, s0);
    CHECK(next == 4096 + 24 && ts_end(&pool, &slot) == tl.pos,
          "the next record starts past page 1's header and the end follows it");
    wal_segment_pool_shutdown(&pool);

    /* A record header split across a page end: 20 bytes on page 0, 24 on page 1. */
    s0 = ts_setup(&pool);
    tl_begin(&tl, 0, 0);
    tl_record(&tl, WAL_PAGE_USABLE - 20 - WAL_RECORD_HEADER_SIZE, 0x11);
    uint64_t split = tl_record(&tl, 64, 0x22);
    tl_write(&tl, &pool, s0);
    CHECK(wal_lsn_offset(split) == 4096 - 20 && ts_end(&pool, &slot) == tl.pos,
          "a record whose header is split across the page end is read whole");
    wal_segment_pool_shutdown(&pool);

    free(tl.img);
}

/* Each check that ends the walk, on an otherwise valid log. */
static void test_tail_scan_stops_at_bad_records(void)
{
    printf("\n[test_tail_scan_stops_at_bad_records]\n");
    WalSegmentPool pool;
    TestLog tl;
    uint32_t slot, s0;

    /* Torn tail: one body byte of the last record flipped. */
    s0 = ts_setup(&pool);
    tl_begin(&tl, 0, 0);
    tl_record(&tl, 100, 0x11);
    uint64_t last = tl_record(&tl, 300, 0x22);
    tl.img[last + 44 + 150] ^= 0x01;
    tl_write(&tl, &pool, s0);
    CHECK(ts_end(&pool, &slot) == last, "a failed CRC ends the log at that record's start");
    wal_segment_pool_shutdown(&pool);
    free(tl.img);

    /* A stale page in the middle of a record: the record continues onto
     * page 1, and page 1 on disk is left from an earlier pass through the
     * slot. Its data is made identical to the real one and it carries the
     * INCOMING flag, so the CRC and the flag pass: only its start_lsn
     * (segment 10's, not segment 0's) gives it away. */
    s0 = ts_setup(&pool);
    tl_begin(&tl, 0, 0);
    tl_record(&tl, 100, 0x11);
    uint64_t spans = tl_record(&tl, 6000, 0x22);   /* continues onto page 1 */
    WalPageHeader ph1;
    wal_page_header_deserialize(tl.img + 4096, &ph1);
    ph1.start_lsn += 10ull << WAL_SEG_SHIFT;
    wal_page_header_serialize(&ph1, tl.img + 4096);
    tl_write(&tl, &pool, s0);
    CHECK(ts_end(&pool, &slot) == spans,
          "a record continuing onto a stale page (wrong start_lsn) ends the log at its start");
    wal_segment_pool_shutdown(&pool);
    free(tl.img);

    /* Stale record bytes inside a current page: the ring is not zeroed, so
     * a partly filled page goes to disk with bytes from one ring length
     * earlier after the end (V2 §8.2). Right after the last record sits a
     * whole, valid older record, CRC included, written for another
     * position: only its lsn field gives it away. */
    s0 = ts_setup(&pool);
    tl_begin(&tl, 0, 0);
    tl_record(&tl, 100, 0x11);
    uint64_t end = tl.pos;
    {
        uint8_t body[100], rec[WAL_RECORD_HEADER_SIZE + 100];
        memset(body, 0x22, sizeof(body));
        WalRecordHeader h;
        memset(&h, 0, sizeof(h));
        h.lsn = end + (10ull << WAL_SEG_SHIFT);   /* written for another position */
        h.txn_id = 1; h.total_len = sizeof(rec); h.rec_type = WAL_REC_INSERT;
        wal_record_header_serialize(&h, body, sizeof(body), rec);
        memcpy(tl.img + end, rec, sizeof(rec));
        tl.pos = end + sizeof(rec);
    }
    tl_write(&tl, &pool, s0);
    CHECK(ts_end(&pool, &slot) == end,
          "a valid older record (good CRC, lsn of another position) ends the log before it");
    wal_segment_pool_shutdown(&pool);
    free(tl.img);

    /* A continuation page without the INCOMING flag. */
    s0 = ts_setup(&pool);
    tl_begin(&tl, 0, 0);
    tl_record(&tl, 100, 0x11);
    uint64_t span = tl_record(&tl, 6000, 0x22);
    tl_page_header(&tl, 4096, 0);   /* page 1 rewritten as if a record started there */
    tl_write(&tl, &pool, s0);
    CHECK(ts_end(&pool, &slot) == span,
          "a record that continues onto a page without the INCOMING flag ends the log");
    wal_segment_pool_shutdown(&pool);
    free(tl.img);
}

/* V2 resume 4b-2b: the walk starts in the segment before the active one
 * and follows records across the boundary; the end is the first record
 * that does not check out, in whichever segment it is. */
static void test_tail_scan_across_segments(void)
{
    printf("\n[test_tail_scan_across_segments]\n");
    WalSegmentPool pool;
    TestLog tl;
    uint32_t slot, s0, s1;
    const uint64_t seg1 = 1ull << WAL_SEG_SHIFT;

    /* A record crossing from segment 0 into segment 1, both written,
     * then records in segment 1: the end is in segment 1. */
    s0 = ts_setup(&pool);
    wal_segment_pool_claim_next(&pool, &s1);
    tl_begin(&tl, 0, 0);
    tl_fill_segment_0(&tl, 1000);                    /* 1000 record bytes left in segment 0 */
    uint64_t cross = tl_record(&tl, 3000, 0x22);     /* 1000 in segment 0, 2044 in segment 1 */
    tl_record(&tl, 200, 0x33);
    tl_write2(&tl, &pool, s0, s1);
    CHECK(wal_lsn_segment(cross) == 0 && wal_lsn_segment(tl.pos) == 1,
          "laid out: a record from segment 0 into segment 1, then one more");
    CHECK(ts_end(&pool, &slot) == tl.pos && slot == s1,
          "the end is past the last record, in segment 1's slot");

    /* The same, but segment 1's page 0 never reached disk: the crossing
     * record is incomplete, so the end is its start, in segment 0. */
    uint8_t zero[WAL_PAGE_SIZE];
    memset(zero, 0, sizeof(zero));
    wal_segment_pool_write_page(&pool, s1, 1, zero);
    CHECK(ts_end(&pool, &slot) == cross && slot == s0,
          "segment 1 page 0 unwritten: the end is the crossing record's start, in segment 0");
    wal_segment_pool_shutdown(&pool);
    free(tl.img);

    /* A torn tail in segment 0 while segment 1 has valid data (its pages
     * reached disk first): the end is the torn record's start, in
     * segment 0, and segment 1's records are not counted. */
    s0 = ts_setup(&pool);
    wal_segment_pool_claim_next(&pool, &s1);
    tl_begin(&tl, 0, 0);
    tl_fill_segment_0(&tl, 1000);
    uint64_t torn = tl_record(&tl, 500, 0x44);   /* wholly in segment 0 */
    tl_record(&tl, 3000, 0x22);
    tl_record(&tl, 200, 0x33);
    tl.img[torn + 44 + 100] ^= 0x01;
    tl_write2(&tl, &pool, s0, s1);
    CHECK(ts_end(&pool, &slot) == torn && slot == s0,
          "a torn record in segment 0 ends the log there, though segment 1 has data");
    wal_segment_pool_shutdown(&pool);
    free(tl.img);

    /* Segment 1 is active and unwritten, segment 0 is complete and its
     * last record ends exactly at the boundary: the end is segment 1's
     * start, in segment 1. */
    s0 = ts_setup(&pool);
    wal_segment_pool_claim_next(&pool, &s1);
    tl_begin(&tl, 0, 0);
    tl_fill_segment_0(&tl, 0);
    tl_write2(&tl, &pool, s0, s1);
    CHECK(tl.pos == seg1 && ts_end(&pool, &slot) == seg1 && slot == s1,
          "segment 0 full to the boundary, segment 1 empty: the end is 4 MB, in segment 1");
    wal_segment_pool_shutdown(&pool);
    free(tl.img);

    /* The previous segment is gone from every slot (archived and freed,
     * which no code does yet, so slot 0 is marked FREE in memory, where
     * tail_scan reads it). The walk then starts at segment 1, past the
     * 500 bytes at its start that continue a record from segment 0. */
    s0 = ts_setup(&pool);
    wal_segment_pool_claim_next(&pool, &s1);
    pool.slots[s0].header.state = SEG_FREE;
    tl_begin(&tl, 1, 500);
    uint64_t first = tl_record(&tl, 100, 0x11);
    tl_record(&tl, 200, 0x22);
    tl_write(&tl, &pool, s1);
    CHECK(first == seg1 + 24 + 500 && ts_end(&pool, &slot) == tl.pos && slot == s1,
          "segment 0 gone: the walk starts after segment 1's 500 continued bytes");
    wal_segment_pool_shutdown(&pool);
    free(tl.img);

    /* Only segment 0, filled exactly to its end: no next segment yet, so
     * the end (4 MB) stays with segment 0's slot. */
    s0 = ts_setup(&pool);
    tl_begin(&tl, 0, 0);
    tl_fill_segment_0(&tl, 0);
    tl_write(&tl, &pool, s0);
    CHECK(ts_end(&pool, &slot) == seg1 && slot == s0,
          "the active segment full to its end: the end is 4 MB, still in its slot");
    wal_segment_pool_shutdown(&pool);
    free(tl.img);
    cleanup();
}

/* Builds one complete WAL_PAGE_SIZE page buffer (real header + data),
 * the way the Flusher would before handing it to wal_segment_pool_write
 * — segment_write itself never constructs headers, so tests that need
 * a real one build it by hand via the existing wal_page_* functions. */
static void build_page(uint8_t *out, uint64_t start_lsn, uint8_t fill)
{
    WalPageHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.id.file_type = FILETYPE_WAL_PAGE;
    hdr.start_lsn = start_lsn;   /* the page's own position in the log (T-039) */
    /* (end_lsn, and later data_len, were set here; both are gone) */
    memset(out + WAL_PAGE_HEADER_SIZE, fill, WAL_PAGE_USABLE);
    wal_page_header_serialize(&hdr, out);
}

static void test_write_at_explicit_offset(void)
{
    printf("\n[test_write_at_explicit_offset]\n");
    cleanup();

    WalSegmentPool pool;
    CHECK(wal_segment_pool_init(&pool, TEST_WAL_DIR, 1) == MYDB_OK, "init succeeds");

    uint32_t slot_index;
    CHECK(wal_segment_pool_claim_next(&pool, &slot_index) == MYDB_OK, "claim succeeds");
    uint32_t page_no = 1, offset = 0;

    /* Two raw chunks, written in two separate calls, back to back — pure
     * position tracking, no header semantics involved (segment_write is
     * a blind byte mover; it never reads or builds a WalPageHeader for
     * this). */
    uint8_t chunk1[100];
    memset(chunk1, 0xAA, sizeof(chunk1));
    int rc = wal_segment_pool_write(&pool, NULL, &slot_index, &page_no, &offset, chunk1, sizeof(chunk1));
    CHECK(rc == MYDB_OK, "first raw write succeeds");
    CHECK(page_no == 1 && offset == 100, "cursor advances to offset 100, same page");

    uint8_t chunk2[50];
    memset(chunk2, 0xBB, sizeof(chunk2));
    rc = wal_segment_pool_write(&pool, NULL, &slot_index, &page_no, &offset, chunk2, sizeof(chunk2));
    CHECK(rc == MYDB_OK, "second raw write (at the caller-supplied offset 100) succeeds");
    CHECK(page_no == 1 && offset == 150, "cursor advances to offset 150, still same page");

    uint8_t raw[WAL_PAGE_SIZE];
    wal_segment_pool_read_page(&pool, slot_index, 1, raw);
    int content_ok = 1;
    for (int i = 0; i < 100; i++) if (raw[i] != 0xAA) content_ok = 0;
    for (int i = 0; i < 50; i++)  if (raw[100 + i] != 0xBB) content_ok = 0;
    CHECK(content_ok, "both chunks landed at exactly the byte offsets the caller specified, in order");

    wal_segment_pool_shutdown(&pool);
    cleanup();
}

static void test_write_spans_multiple_pages(void)
{
    printf("\n[test_write_spans_multiple_pages]\n");
    cleanup();

    WalSegmentPool pool;
    CHECK(wal_segment_pool_init(&pool, TEST_WAL_DIR, 1) == MYDB_OK, "init succeeds");

    uint32_t slot_index;
    wal_segment_pool_claim_next(&pool, &slot_index);
    uint32_t page_no = 1, offset = 0;

    /* One call, more than 2 whole WAL_PAGE_SIZE page-slots' worth of raw
     * bytes — page splitting is purely by WAL_PAGE_SIZE byte count, with
     * no awareness of header/usable-space boundaries at all. */
    size_t big_len = (size_t)WAL_PAGE_SIZE * 2 + 500;
    uint8_t *big = malloc(big_len);
    for (size_t i = 0; i < big_len; i++) big[i] = (uint8_t)(i & 0xFF);

    int rc = wal_segment_pool_write(&pool, NULL, &slot_index, &page_no, &offset, big, big_len);
    CHECK(rc == MYDB_OK, "one big write spanning 3 page-slots succeeds");
    CHECK(page_no == 3 && offset == 500,
          "cursor lands on page 3, offset 500 (pages 1,2 filled exactly, 3 partially)");

    uint8_t raw1[WAL_PAGE_SIZE], raw2[WAL_PAGE_SIZE], raw3[WAL_PAGE_SIZE];
    wal_segment_pool_read_page(&pool, slot_index, 1, raw1);
    wal_segment_pool_read_page(&pool, slot_index, 2, raw2);
    wal_segment_pool_read_page(&pool, slot_index, 3, raw3);

    int content_ok =
        memcmp(raw1, big, WAL_PAGE_SIZE) == 0 &&
        memcmp(raw2, big + WAL_PAGE_SIZE, WAL_PAGE_SIZE) == 0 &&
        memcmp(raw3, big + 2 * WAL_PAGE_SIZE, 500) == 0;
    CHECK(content_ok, "bytes reassembled across all 3 page-slots match the original buffer exactly");

    free(big);
    wal_segment_pool_shutdown(&pool);
    cleanup();
}

static void test_write_rolls_to_new_segment(void)
{
    printf("\n[test_write_rolls_to_new_segment]\n");
    cleanup();

    WalSegmentPool pool;
    CHECK(wal_segment_pool_init(&pool, TEST_WAL_DIR, 1) == MYDB_OK, "init succeeds");

    uint32_t slot_index;
    wal_segment_pool_claim_next(&pool, &slot_index);
    uint32_t page_no = 1, offset = 0;
    uint32_t first_slot = slot_index;

    /* Build all 1024 log pages as real, header-carrying pages, each
     * stamped with its own position in segment 0 — plus one more real page
     * that should land in the rolled-over segment. This is the one
     * scenario that needs real headers: it's testing that
     * wal_segment_pool_write works out the segment's end from its true
     * last page when it auto-finalizes (start_lsn + WAL_PAGE_SIZE; it used
     * to read that page's end_lsn field, dropped in T-039, and then add
     * data_len, dropped after it). The last page used to hold 4000 bytes
     * so the end was not just "the page end"; every page is full now.
     * Under V2 rule 6 the header page is outside the LSN, so file page
     * i + 1 holds log page i, at LSN i * WAL_PAGE_SIZE (it was (i + 1) *
     * WAL_PAGE_SIZE, 511 pages, while the header page counted). */
    uint32_t last_page_count = WAL_SEGMENT_PAGES_PER_FILE - 1;
    size_t total_len = (size_t)WAL_PAGE_SIZE * (last_page_count + 1);
    uint8_t *big = malloc(total_len);
    for (uint32_t i = 0; i < last_page_count; i++)
        build_page(big + (size_t)i * WAL_PAGE_SIZE, (uint64_t)i * WAL_PAGE_SIZE, 0x5A);
    build_page(big + (size_t)last_page_count * WAL_PAGE_SIZE, WAL_SEG_SIZE, 0x5B);   /* segment 1, page 0 */

    int rc = wal_segment_pool_write(&pool, NULL, &slot_index, &page_no, &offset, big, total_len);
    CHECK(rc == MYDB_OK, "a write that fills the whole segment and spills one page over succeeds");
    CHECK(slot_index != first_slot, "cursor's slot_index changed — it rolled to a new segment");
    CHECK(page_no == 2 && offset == 0,
          "the new segment's cursor sits right after the one spilled-over page");

    CHECK(pool.slots[first_slot].header.state == SEG_DONE,
          "the filled segment was finalized to SEG_DONE automatically");
    CHECK(pool.slots[first_slot].header.data_pages == last_page_count,
          "the finalized segment's data_pages reflects all 1024 pages used");
    CHECK(pool.slots[first_slot].header.end_lsn == 1023ull * WAL_PAGE_SIZE + WAL_PAGE_SIZE &&
          pool.slots[first_slot].header.end_lsn == WAL_SEG_SIZE,
          "the finalized segment's end_lsn = its last page's start_lsn + WAL_PAGE_SIZE (segment 1's start)");
    CHECK(pool.slots[slot_index].header.state == SEG_ACTIVE,
          "the new segment is active and holds the one spilled-over page");

    uint8_t spilled[WAL_PAGE_SIZE];
    wal_segment_pool_read_page(&pool, slot_index, 1, spilled);
    CHECK(memcmp(spilled, big + (size_t)last_page_count * WAL_PAGE_SIZE, WAL_PAGE_SIZE) == 0,
          "the spilled-over page's content landed correctly in the new segment");

    free(big);
    wal_segment_pool_shutdown(&pool);
    cleanup();
}

/* T-010: a slot that was claimed once and later freed keeps segment_no
 * and start_lsn in its FREE header, and init counts it when reseeding
 * next_segment_no — so the counter never drops back, even when every
 * slot is FREE. (No free path exists yet; the FREE header is written by
 * hand here, the way the future free path must write it.) */
static void test_reseed_counts_freed_slots(void)
{
    printf("\n[test_reseed_counts_freed_slots]\n");
    cleanup();

    WalSegmentPool pool;
    wal_segment_pool_init(&pool, TEST_WAL_DIR, 1);
    wal_segment_pool_shutdown(&pool);

    WalSegmentHeader h;
    memset(&h, 0, sizeof(h));
    h.id.file_type = FILETYPE_WAL_SEGMENT;
    h.segment_no   = 27;                          /* lives in slot 27 % 10 = 7 */
    h.start_lsn    = wal_segment_first_lsn(27);
    h.partition_id = 1;
    h.state        = SEG_FREE;
    uint8_t page[WAL_PAGE_SIZE];
    memset(page, 0, sizeof(page));
    wal_segment_header_serialize(&h, page);
    char path[300];
    snprintf(path, sizeof(path), "%s/wal_7.mydb", TEST_WAL_DIR);
    int fd = open(path, O_WRONLY);
    CHECK(fd >= 0 && pwrite(fd, page, WAL_PAGE_SIZE, 0) == WAL_PAGE_SIZE, "a freed segment 27's header is on disk");
    if (fd >= 0) close(fd);

    WalSegmentPool reloaded;
    CHECK(wal_segment_pool_init(&reloaded, TEST_WAL_DIR, 1) == MYDB_OK, "reload succeeds");
    CHECK(reloaded.slots[7].header.state == SEG_FREE, "every slot is FREE");
    CHECK(reloaded.next_segment_no == 28, "next_segment_no continues after 27 instead of restarting at 0");

    wal_segment_pool_shutdown(&reloaded);
    cleanup();
}

/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_wal_segment_pool ===\n");

    test_fresh_init_creates_all_slots();
    test_claim_next_round_robins();
    test_page_round_trip_and_bounds();
    test_clean_reload_preserves_state();
    test_tail_scan_picks_the_active_segment();
    test_tail_scan_finds_the_end();
    test_tail_scan_stops_at_bad_records();
    test_tail_scan_across_segments();
    test_write_at_explicit_offset();
    test_write_spans_multiple_pages();
    test_write_rolls_to_new_segment();
    test_reseed_counts_freed_slots();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
