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

static void test_crash_reload_tail_scan(void)
{
    printf("\n[test_crash_reload_tail_scan]\n");
    cleanup();

    WalSegmentPool pool;
    CHECK(wal_segment_pool_init(&pool, TEST_WAL_DIR, 1) == MYDB_OK, "first init succeeds");

    uint32_t slot_index;
    CHECK(wal_segment_pool_claim_next(&pool, &slot_index) == MYDB_OK, "claim succeeds");

    WalPageHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.id.file_type = FILETYPE_WAL_PAGE;

    uint8_t page_buf[WAL_PAGE_SIZE];
    for (uint32_t page_no = 1; page_no <= 2; page_no++) {
        memset(page_buf, 0, WAL_PAGE_SIZE);
        hdr.start_lsn = page_no;
        wal_page_header_serialize(&hdr, page_buf);
        wal_segment_pool_write_page(&pool, slot_index, page_no, page_buf);
    }

    /* Simulate a crash: shut down without ever transitioning the slot
     * out of SEG_ACTIVE (no SEG_DONE header rewrite happens). */
    wal_segment_pool_shutdown(&pool);

    WalSegmentPool reloaded;
    CHECK(wal_segment_pool_init(&reloaded, TEST_WAL_DIR, 1) == MYDB_OK, "post-crash reload succeeds");
    CHECK(reloaded.slots[slot_index].header.state == SEG_ACTIVE,
          "the crashed slot reloads as SEG_ACTIVE (never got to rewrite its header)");
    CHECK(reloaded.slots[slot_index].header.data_pages == 2,
          "init's automatic tail-scan recovers data_pages == 2 from the two pages actually written");

    uint32_t rescanned = 0;
    CHECK(wal_segment_pool_tail_scan(&reloaded, slot_index, &rescanned) == MYDB_OK &&
          rescanned == 2,
          "calling tail_scan directly agrees: 2 valid pages found");

    wal_segment_pool_shutdown(&reloaded);
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

    /* Build all 511 usable pages as real, header-carrying pages, each
     * stamped with its own position in segment 0 — plus one more real page
     * that should land in the rolled-over segment. This is the one
     * scenario that needs real headers: it's testing that
     * wal_segment_pool_write works out the segment's end from its true
     * last page when it auto-finalizes (start_lsn + WAL_PAGE_SIZE; it used
     * to read that page's end_lsn field, dropped in T-039, and then add
     * data_len, dropped after it). The last page used to hold 4000 bytes
     * so the end was not just "the page end"; every page is full now. */
    uint32_t last_page_count = WAL_SEGMENT_PAGES_PER_FILE - 1;
    size_t total_len = (size_t)WAL_PAGE_SIZE * (last_page_count + 1);
    uint8_t *big = malloc(total_len);
    for (uint32_t i = 0; i < last_page_count; i++)
        build_page(big + (size_t)i * WAL_PAGE_SIZE, (uint64_t)(i + 1) * WAL_PAGE_SIZE, 0x5A);
    build_page(big + (size_t)last_page_count * WAL_PAGE_SIZE, WAL_SEG_SIZE + WAL_PAGE_SIZE, 0x5B);

    int rc = wal_segment_pool_write(&pool, NULL, &slot_index, &page_no, &offset, big, total_len);
    CHECK(rc == MYDB_OK, "a write that fills the whole segment and spills one page over succeeds");
    CHECK(slot_index != first_slot, "cursor's slot_index changed — it rolled to a new segment");
    CHECK(page_no == 2 && offset == 0,
          "the new segment's cursor sits right after the one spilled-over page");

    CHECK(pool.slots[first_slot].header.state == SEG_DONE,
          "the filled segment was finalized to SEG_DONE automatically");
    CHECK(pool.slots[first_slot].header.data_pages == last_page_count,
          "the finalized segment's data_pages reflects all 511 pages used");
    CHECK(pool.slots[first_slot].header.end_lsn == 511ull * WAL_PAGE_SIZE + WAL_PAGE_SIZE &&
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
    test_crash_reload_tail_scan();
    test_write_at_explicit_offset();
    test_write_spans_multiple_pages();
    test_write_rolls_to_new_segment();
    test_reseed_counts_freed_slots();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
