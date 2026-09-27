#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#include "common.h"
#include "large_wal/large_wal_page.h"
#include "large_wal/large_wal_segment.h"
#include "large_wal/large_wal_segment_pool.h"

#define TEST_WAL_DIR "/tmp/mydb_test_large_wal_pool"

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

/* Remove every large_wal_<i>.mydb under TEST_WAL_DIR, then the directory
 * itself. Safe to call whether or not the directory exists yet. */
static void cleanup(void)
{
    char path[300];
    for (uint32_t i = 0; i < LARGE_WAL_SEGMENT_POOL_SLOTS; i++) {
        snprintf(path, sizeof(path), "%s/large_wal_%u.mydb", TEST_WAL_DIR, i);
        unlink(path);
    }
    rmdir(TEST_WAL_DIR);
}

/* ------------------------------------------------------------------ */

static void test_fresh_init_creates_all_slots(void)
{
    printf("\n[test_fresh_init_creates_all_slots]\n");
    cleanup();

    LargeWalSegmentPool pool;
    int rc = large_wal_segment_pool_init(&pool, TEST_WAL_DIR, 7, NULL);
    CHECK(rc == MYDB_OK, "fresh init succeeds");

    int all_free = 1, all_right_size = 1;
    for (uint32_t i = 0; i < LARGE_WAL_SEGMENT_POOL_SLOTS; i++) {
        char path[300];
        snprintf(path, sizeof(path), "%s/large_wal_%u.mydb", TEST_WAL_DIR, i);
        struct stat st;
        if (stat(path, &st) != 0 || st.st_size != LARGE_WAL_SEGMENT_FILE_SIZE)
            all_right_size = 0;
        if (pool.slots[i].header.state != LSEG_FREE)
            all_free = 0;
    }
    CHECK(all_right_size, "every slot file is exactly LARGE_WAL_SEGMENT_FILE_SIZE bytes");
    CHECK(all_free, "every slot reloads as LSEG_FREE");
    CHECK(pool.next_segment_no == 0, "next_segment_no starts at 0 on a fresh pool");

    large_wal_segment_pool_shutdown(&pool);
    cleanup();
}

static void test_claim_next_round_robins(void)
{
    printf("\n[test_claim_next_round_robins]\n");
    cleanup();

    LargeWalSegmentPool pool;
    CHECK(large_wal_segment_pool_init(&pool, TEST_WAL_DIR, 1, NULL) == MYDB_OK, "init succeeds");

    int ok = 1;
    for (uint32_t i = 0; i < LARGE_WAL_SEGMENT_POOL_SLOTS; i++) {
        uint32_t slot_index;
        if (large_wal_segment_pool_claim_next(&pool, &slot_index) != MYDB_OK) { ok = 0; break; }
        if (slot_index != i) { ok = 0; break; }
        if (pool.slots[slot_index].header.segment_no != i) { ok = 0; break; }
        if (pool.slots[slot_index].header.state != LSEG_ACTIVE) { ok = 0; break; }
    }
    CHECK(ok, "4 successive claims assign slot_index == segment_no == 0..3 in order");

    uint32_t fifth;
    int rc = large_wal_segment_pool_claim_next(&pool, &fifth);
    CHECK(rc == MYDB_ERR, "a 5th claim fails: every slot is used and none has been freed yet");

    large_wal_segment_pool_shutdown(&pool);
    cleanup();
}

static void test_page_round_trip_and_bounds(void)
{
    printf("\n[test_page_round_trip_and_bounds]\n");
    cleanup();

    LargeWalSegmentPool pool;
    CHECK(large_wal_segment_pool_init(&pool, TEST_WAL_DIR, 1, NULL) == MYDB_OK, "init succeeds");

    uint32_t slot_index;
    CHECK(large_wal_segment_pool_claim_next(&pool, &slot_index) == MYDB_OK, "claim succeeds");

    WalPageHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.id.file_type = FILETYPE_LARGE_WAL_PAGE;
    hdr.start_lsn = 99;
    hdr.end_lsn = 99;
    hdr.data_len = 123;

    uint8_t page_buf[PAGE_SIZE];
    memset(page_buf, 0, PAGE_SIZE);
    large_wal_page_header_serialize(&hdr, page_buf);

    CHECK(large_wal_segment_pool_write_page(&pool, slot_index, 1, page_buf) == MYDB_OK,
          "write_page(page_no=1) succeeds");

    uint8_t read_buf[PAGE_SIZE];
    CHECK(large_wal_segment_pool_read_page(&pool, slot_index, 1, read_buf) == MYDB_OK,
          "read_page(page_no=1) succeeds");

    WalPageHeader out;
    int rc = large_wal_page_header_deserialize(read_buf, &out);
    CHECK(rc == MYDB_OK, "the page read back deserializes cleanly");
    CHECK(out.start_lsn == 99, "start_lsn round-trips through the pool");

    CHECK(large_wal_segment_pool_write_page(&pool, slot_index, 0, page_buf) == MYDB_ERR,
          "page_no=0 (the header's own slot) is rejected");
    CHECK(large_wal_segment_pool_write_page(&pool, slot_index, LARGE_WAL_SEGMENT_PAGES_PER_FILE, page_buf) == MYDB_ERR,
          "page_no == LARGE_WAL_SEGMENT_PAGES_PER_FILE is out of range and rejected");

    large_wal_segment_pool_shutdown(&pool);
    cleanup();
}

static void test_clean_reload_preserves_state(void)
{
    printf("\n[test_clean_reload_preserves_state]\n");
    cleanup();

    LargeWalSegmentPool pool;
    CHECK(large_wal_segment_pool_init(&pool, TEST_WAL_DIR, 1, NULL) == MYDB_OK, "first init succeeds");

    uint32_t slot_index;
    large_wal_segment_pool_claim_next(&pool, &slot_index);
    large_wal_segment_pool_claim_next(&pool, &slot_index);
    large_wal_segment_pool_claim_next(&pool, &slot_index);
    large_wal_segment_pool_shutdown(&pool);

    LargeWalSegmentPool reloaded;
    CHECK(large_wal_segment_pool_init(&reloaded, TEST_WAL_DIR, 1, NULL) == MYDB_OK, "reload succeeds");
    CHECK(reloaded.next_segment_no == 3, "next_segment_no survives a clean reload (3 claims made)");
    CHECK(reloaded.slots[0].header.state == LSEG_ACTIVE &&
          reloaded.slots[1].header.state == LSEG_ACTIVE &&
          reloaded.slots[2].header.state == LSEG_ACTIVE,
          "the 3 previously-claimed slots reload as LSEG_ACTIVE");
    CHECK(reloaded.slots[3].header.state == LSEG_FREE, "an untouched slot reloads as LSEG_FREE");

    large_wal_segment_pool_shutdown(&reloaded);
    cleanup();
}

static void test_crash_reload_tail_scan(void)
{
    printf("\n[test_crash_reload_tail_scan]\n");
    cleanup();

    LargeWalSegmentPool pool;
    CHECK(large_wal_segment_pool_init(&pool, TEST_WAL_DIR, 1, NULL) == MYDB_OK, "first init succeeds");

    uint32_t slot_index;
    CHECK(large_wal_segment_pool_claim_next(&pool, &slot_index) == MYDB_OK, "claim succeeds");

    WalPageHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.id.file_type = FILETYPE_LARGE_WAL_PAGE;

    uint8_t page_buf[PAGE_SIZE];
    for (uint32_t page_no = 1; page_no <= 2; page_no++) {
        memset(page_buf, 0, PAGE_SIZE);
        hdr.start_lsn = page_no;
        hdr.end_lsn   = page_no;
        large_wal_page_header_serialize(&hdr, page_buf);
        large_wal_segment_pool_write_page(&pool, slot_index, page_no, page_buf);
    }

    /* Simulate a crash: shut down without ever transitioning the slot
     * out of LSEG_ACTIVE (no LSEG_DONE header rewrite happens). */
    large_wal_segment_pool_shutdown(&pool);

    LargeWalSegmentPool reloaded;
    CHECK(large_wal_segment_pool_init(&reloaded, TEST_WAL_DIR, 1, NULL) == MYDB_OK, "post-crash reload succeeds");
    CHECK(reloaded.slots[slot_index].header.state == LSEG_ACTIVE,
          "the crashed slot reloads as LSEG_ACTIVE (never got to rewrite its header)");
    CHECK(reloaded.slots[slot_index].header.data_pages == 2,
          "init's automatic tail-scan recovers data_pages == 2 from the two pages actually written");

    uint32_t rescanned = 0;
    CHECK(large_wal_segment_pool_tail_scan(&reloaded, slot_index, &rescanned) == MYDB_OK &&
          rescanned == 2,
          "calling tail_scan directly agrees: 2 valid pages found");

    large_wal_segment_pool_shutdown(&reloaded);
    cleanup();
}

/* Builds one complete PAGE_SIZE page buffer (real header + content), the
 * way the LARGE_WAL Writer would before handing it to
 * large_wal_segment_pool_write — write() itself never constructs
 * headers, so tests that need a real one build it by hand. */
static void build_page(uint8_t *out, uint64_t content_lsn, uint8_t fill)
{
    WalPageHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.id.file_type = FILETYPE_LARGE_WAL_PAGE;
    hdr.start_lsn = content_lsn;
    hdr.end_lsn   = content_lsn;
    hdr.data_len = PAGE_SIZE - LARGE_WAL_PAGE_HEADER_ON_DISK_SIZE;
    memset(out + LARGE_WAL_PAGE_HEADER_ON_DISK_SIZE, fill, hdr.data_len);
    large_wal_page_header_serialize(&hdr, out);
}

static void test_write_at_explicit_offset(void)
{
    printf("\n[test_write_at_explicit_offset]\n");
    cleanup();

    LargeWalSegmentPool pool;
    CHECK(large_wal_segment_pool_init(&pool, TEST_WAL_DIR, 1, NULL) == MYDB_OK, "init succeeds");

    uint32_t slot_index;
    CHECK(large_wal_segment_pool_claim_next(&pool, &slot_index) == MYDB_OK, "claim succeeds");
    uint32_t page_no = 1, offset = 0;

    /* Two raw chunks, written in two separate calls, back to back — pure
     * position tracking, no header semantics involved (write() is a
     * blind byte mover). */
    uint8_t chunk1[100];
    memset(chunk1, 0xAA, sizeof(chunk1));
    int rc = large_wal_segment_pool_write(&pool, NULL, &slot_index, &page_no, &offset, chunk1, sizeof(chunk1));
    CHECK(rc == MYDB_OK, "first raw write succeeds");
    CHECK(page_no == 1 && offset == 100, "cursor advances to offset 100, same page");

    uint8_t chunk2[50];
    memset(chunk2, 0xBB, sizeof(chunk2));
    rc = large_wal_segment_pool_write(&pool, NULL, &slot_index, &page_no, &offset, chunk2, sizeof(chunk2));
    CHECK(rc == MYDB_OK, "second raw write (at the caller-supplied offset 100) succeeds");
    CHECK(page_no == 1 && offset == 150, "cursor advances to offset 150, still same page");

    uint8_t raw[PAGE_SIZE];
    large_wal_segment_pool_read_page(&pool, slot_index, 1, raw);
    int content_ok = 1;
    for (int i = 0; i < 100; i++) if (raw[i] != 0xAA) content_ok = 0;
    for (int i = 0; i < 50; i++)  if (raw[100 + i] != 0xBB) content_ok = 0;
    CHECK(content_ok, "both chunks landed at exactly the byte offsets the caller specified, in order");

    large_wal_segment_pool_shutdown(&pool);
    cleanup();
}

static void test_write_spans_multiple_pages(void)
{
    printf("\n[test_write_spans_multiple_pages]\n");
    cleanup();

    LargeWalSegmentPool pool;
    CHECK(large_wal_segment_pool_init(&pool, TEST_WAL_DIR, 1, NULL) == MYDB_OK, "init succeeds");

    uint32_t slot_index;
    large_wal_segment_pool_claim_next(&pool, &slot_index);
    uint32_t page_no = 1, offset = 0;

    /* One call, more than 2 whole PAGE_SIZE page-slots' worth of raw
     * bytes — page splitting is purely by PAGE_SIZE byte count. */
    size_t big_len = (size_t)PAGE_SIZE * 2 + 500;
    uint8_t *big = malloc(big_len);
    for (size_t i = 0; i < big_len; i++) big[i] = (uint8_t)(i & 0xFF);

    int rc = large_wal_segment_pool_write(&pool, NULL, &slot_index, &page_no, &offset, big, big_len);
    CHECK(rc == MYDB_OK, "one big write spanning 3 page-slots succeeds");
    CHECK(page_no == 3 && offset == 500,
          "cursor lands on page 3, offset 500 (pages 1,2 filled exactly, 3 partially)");

    uint8_t raw1[PAGE_SIZE], raw2[PAGE_SIZE], raw3[PAGE_SIZE];
    large_wal_segment_pool_read_page(&pool, slot_index, 1, raw1);
    large_wal_segment_pool_read_page(&pool, slot_index, 2, raw2);
    large_wal_segment_pool_read_page(&pool, slot_index, 3, raw3);

    int content_ok =
        memcmp(raw1, big, PAGE_SIZE) == 0 &&
        memcmp(raw2, big + PAGE_SIZE, PAGE_SIZE) == 0 &&
        memcmp(raw3, big + 2 * PAGE_SIZE, 500) == 0;
    CHECK(content_ok, "bytes reassembled across all 3 page-slots match the original buffer exactly");

    free(big);
    large_wal_segment_pool_shutdown(&pool);
    cleanup();
}

static void test_write_rolls_to_new_segment(void)
{
    printf("\n[test_write_rolls_to_new_segment]\n");
    cleanup();

    LargeWalSegmentPool pool;
    CHECK(large_wal_segment_pool_init(&pool, TEST_WAL_DIR, 1, NULL) == MYDB_OK, "init succeeds");

    uint32_t slot_index;
    large_wal_segment_pool_claim_next(&pool, &slot_index);
    uint32_t page_no = 1, offset = 0;
    uint32_t first_slot = slot_index;

    /* Build all 127 usable pages as real, header-carrying pages — the
     * last one stamped with a distinguishable LSN — plus one more real
     * page that should land in the rolled-over segment. This is the one
     * scenario that needs real headers: it's testing that write()
     * correctly reads end_lsn back from the segment's true last page
     * when it auto-finalizes. */
    uint32_t last_page_count = LARGE_WAL_SEGMENT_PAGES_PER_FILE - 1;
    size_t total_len = (size_t)PAGE_SIZE * (last_page_count + 1);
    uint8_t *big = malloc(total_len);
    for (uint32_t i = 0; i < last_page_count; i++)
        build_page(big + (size_t)i * PAGE_SIZE, 100 + i, 0x5A);
    build_page(big + (size_t)last_page_count * PAGE_SIZE, 12345, 0x5B);

    int rc = large_wal_segment_pool_write(&pool, NULL, &slot_index, &page_no, &offset, big, total_len);
    CHECK(rc == MYDB_OK, "a write that fills the whole segment and spills one page over succeeds");
    CHECK(slot_index != first_slot, "cursor's slot_index changed — it rolled to a new segment");
    CHECK(page_no == 2 && offset == 0,
          "the new segment's cursor sits right after the one spilled-over page");

    CHECK(pool.slots[first_slot].header.state == LSEG_DONE,
          "the filled segment was finalized to LSEG_DONE automatically");
    CHECK(pool.slots[first_slot].header.data_pages == last_page_count,
          "the finalized segment's data_pages reflects all 127 pages used");
    CHECK(pool.slots[first_slot].header.end_lsn == 100 + last_page_count - 1,
          "the finalized segment's end_lsn was read back from the true last page's own end_lsn field");
    CHECK(pool.slots[slot_index].header.state == LSEG_ACTIVE,
          "the new segment is active and holds the one spilled-over page");

    uint8_t spilled[PAGE_SIZE];
    large_wal_segment_pool_read_page(&pool, slot_index, 1, spilled);
    CHECK(memcmp(spilled, big + (size_t)last_page_count * PAGE_SIZE, PAGE_SIZE) == 0,
          "the spilled-over page's content landed correctly in the new segment");

    free(big);
    large_wal_segment_pool_shutdown(&pool);
    cleanup();
}

static void test_read_segment_whole_file(void)
{
    printf("\n[test_read_segment_whole_file]\n");
    cleanup();

    LargeWalSegmentPool pool;
    CHECK(large_wal_segment_pool_init(&pool, TEST_WAL_DIR, 1, NULL) == MYDB_OK, "init succeeds");

    uint32_t slot_index;
    large_wal_segment_pool_claim_next(&pool, &slot_index);

    uint8_t page_buf[PAGE_SIZE];
    build_page(page_buf, 777, 0xCC);
    large_wal_segment_pool_write_page(&pool, slot_index, 1, page_buf);

    uint8_t *whole = malloc(LARGE_WAL_SEGMENT_FILE_SIZE);
    CHECK(large_wal_segment_pool_read_segment(&pool, slot_index, whole) == MYDB_OK,
          "read_segment succeeds");
    CHECK(memcmp(whole + PAGE_SIZE, page_buf, PAGE_SIZE) == 0,
          "the whole-file read includes the page written at page_no=1, at the right offset");

    LargeWalSegmentHeader hdr_from_whole_read;
    CHECK(large_wal_segment_header_deserialize(whole, &hdr_from_whole_read) == MYDB_OK,
          "the segment header (page 0) is included and valid in the whole-file read too");

    free(whole);
    large_wal_segment_pool_shutdown(&pool);
    cleanup();
}

static void test_free_slot_zeroes_content(void)
{
    printf("\n[test_free_slot_zeroes_content]\n");
    cleanup();

    LargeWalSegmentPool pool;
    CHECK(large_wal_segment_pool_init(&pool, TEST_WAL_DIR, 1, NULL) == MYDB_OK, "init succeeds");

    uint32_t slot_index;
    large_wal_segment_pool_claim_next(&pool, &slot_index);

    uint8_t page_buf[PAGE_SIZE];
    for (uint32_t p = 1; p <= 5; p++) {
        build_page(page_buf, 500 + p, 0xE1);
        large_wal_segment_pool_write_page(&pool, slot_index, p, page_buf);
    }

    large_wal_segment_pool_mark_done(&pool, NULL, slot_index, 505, 5, NULL);
    CHECK(large_wal_segment_pool_free_slot(&pool, slot_index) == MYDB_OK, "free_slot succeeds");
    CHECK(pool.slots[slot_index].header.state == LSEG_FREE, "the slot is LSEG_FREE");

    /* Every former content page must now read back as zeros — which is
     * exactly what makes an unwritten page fail deserialize, and so what
     * lets tail_scan find the real tail of the NEXT generation. */
    uint8_t read_buf[PAGE_SIZE];
    int all_zero = 1, all_reject = 1;
    for (uint32_t p = 1; p <= 5; p++) {
        large_wal_segment_pool_read_page(&pool, slot_index, p, read_buf);
        for (uint32_t i = 0; i < PAGE_SIZE; i++)
            if (read_buf[i] != 0) { all_zero = 0; break; }

        WalPageHeader hdr;
        if (large_wal_page_header_deserialize(read_buf, &hdr) == MYDB_OK) all_reject = 0;
    }
    CHECK(all_zero,   "every content page written before the free reads back as zeros");
    CHECK(all_reject, "and none of them deserializes any more");

    large_wal_segment_pool_shutdown(&pool);
    cleanup();
}

/* The bug this zeroing exists to kill, driven end to end.
 *
 * A segment only rolls over when full, so a slot's previous generation
 * has essentially always written every page — while the generation that
 * crashes is mid-fill. Stale pages carry valid magic/version/file_type
 * and a valid CRC, so without zeroing tail_scan walks straight past the
 * live tail and reports the OLD generation's count. */
static void test_reused_slot_does_not_inherit_stale_pages(void)
{
    printf("\n[test_reused_slot_does_not_inherit_stale_pages]\n");
    cleanup();

    LargeWalSegmentPool pool;
    CHECK(large_wal_segment_pool_init(&pool, TEST_WAL_DIR, 1, NULL) == MYDB_OK, "init succeeds");

    /* Generation 1: claim slot 0, write 20 pages, finish it, release it. */
    uint32_t slot_index;
    large_wal_segment_pool_claim_next(&pool, &slot_index);
    uint32_t first_slot = slot_index;

    uint8_t page_buf[PAGE_SIZE];
    for (uint32_t p = 1; p <= 20; p++) {
        build_page(page_buf, 1000 + p, 0xA1);
        large_wal_segment_pool_write_page(&pool, first_slot, p, page_buf);
    }
    large_wal_segment_pool_mark_done(&pool, NULL, first_slot, 1020, 20, NULL);
    large_wal_segment_pool_free_slot(&pool, first_slot);

    /* Round-robin over 4 slots brings us back to the same file. */
    uint32_t reused;
    for (int i = 0; i < LARGE_WAL_SEGMENT_POOL_SLOTS; i++)
        large_wal_segment_pool_claim_next(&pool, &reused);
    CHECK(reused == first_slot, "round-robin returns to the same physical slot");

    /* Generation 2: only 3 pages, then "crash" — no mark_done, so the
     * on-disk data_pages stays 0 and reload must reconstruct it. */
    for (uint32_t p = 1; p <= 3; p++) {
        build_page(page_buf, 2000 + p, 0xB2);
        large_wal_segment_pool_write_page(&pool, reused, p, page_buf);
    }
    large_wal_segment_pool_shutdown(&pool);

    LargeWalSegmentPool reloaded;
    CHECK(large_wal_segment_pool_init(&reloaded, TEST_WAL_DIR, 1, NULL) == MYDB_OK,
          "post-crash reload succeeds");
    CHECK(reloaded.slots[reused].header.state == LSEG_ACTIVE,
          "the crashed slot reloads as LSEG_ACTIVE");
    CHECK(reloaded.slots[reused].header.data_pages == 3,
          "tail_scan recovers 3 — the NEW generation's count, not the old 20 "
          "(without free_slot's zeroing this reports 20)");

    uint32_t rescanned = 0;
    CHECK(large_wal_segment_pool_tail_scan(&reloaded, reused, &rescanned) == MYDB_OK &&
          rescanned == 3,
          "a direct tail_scan agrees: the stale pages beyond the tail are gone");

    large_wal_segment_pool_shutdown(&reloaded);
    cleanup();
}

/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_large_wal_segment_pool ===\n");

    test_fresh_init_creates_all_slots();
    test_claim_next_round_robins();
    test_page_round_trip_and_bounds();
    test_clean_reload_preserves_state();
    test_crash_reload_tail_scan();
    test_write_at_explicit_offset();
    test_write_spans_multiple_pages();
    test_write_rolls_to_new_segment();
    test_read_segment_whole_file();
    test_free_slot_zeroes_content();
    test_reused_slot_does_not_inherit_stale_pages();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
