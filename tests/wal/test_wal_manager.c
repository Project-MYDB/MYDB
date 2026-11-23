#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <stddef.h>
#include <stdint.h>

#include "common.h"
#include "wal_manager.h"
#include "wal_test_log.h"

#define TEST_WAL_DIR "/tmp/mydb_test_wal_manager"

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

static void test_init_and_shutdown_bring_up_all_subpieces(void)
{
    printf("\n[test_init_and_shutdown_bring_up_all_subpieces]\n");
    cleanup();

    WalManager wm;
    memset(&wm, 0, sizeof(wm));
    CHECK(wal_manager_init(&wm, TEST_WAL_DIR, 1) == MYDB_OK,
          "a single init() call brings up the worker + pool and claims a segment");
    CHECK(wm.worker.mb.started == 1, "the worker thread was started as part of init()");
    CHECK(wm.curr_slot < WAL_SEGMENT_POOL_SLOTS &&
          wm.pool.slots[wm.curr_slot].header.state == SEG_ACTIVE,
          "init() claimed an active segment into curr_slot");
    uint64_t seg = wm.pool.slots[wm.curr_slot].header.segment_no;
    CHECK(wm.pool.slots[wm.curr_slot].header.start_lsn == wal_segment_first_lsn(seg),
          "its start_lsn is that segment's first record LSN");
    CHECK(seg == 0 && wal_segment_first_lsn(seg) == 24,
          "on a fresh directory that is segment 0, first record at LSN 24 (V2 rule 6)");

    CHECK(wal_manager_shutdown(&wm) == MYDB_OK, "shutdown() succeeds cleanly");
    CHECK(wm.worker.mb.started == 0, "the worker thread was stopped by shutdown()");

    cleanup();
}

/* V2 §5: the insert path's fields. Ring and counters allocated with the
 * right alignment, counters zero, masks from the frame count, the two
 * hot atomics each on their own cache line, positions seeded from the
 * claimed segment. */
static void test_insert_fields(void)
{
    printf("\n[test_insert_fields]\n");
    cleanup();

    WalManager wm;
    memset(&wm, 0, sizeof(wm));
    CHECK(wal_manager_init(&wm, TEST_WAL_DIR, 7) == MYDB_OK, "init succeeds");

    CHECK(wm.ring_buf_size == 1024 && wm.ring_frame_mask == 1023 && wm.ctr_mask == 8191,
          "a 4 MB ring: 1024 frames, frame mask 1023, ctr_mask 8191");
    CHECK(wm.partition_id == 7, "partition_id stored");
    CHECK(wm.ring_buf && ((uintptr_t)wm.ring_buf % 4096) == 0, "ring_buf is 4096-aligned");
    CHECK(wm.fs_counters && ((uintptr_t)wm.fs_counters % 64) == 0,
          "fs_counters is 64-aligned (4 counters per cache line holds)");

    int all_zero = 1;
    for (uint32_t i = 0; i <= wm.ctr_mask; i++)
        if (atomic_load(&wm.fs_counters[i].done) != 0) { all_zero = 0; break; }
    CHECK(all_zero, "all 8192 counters start at zero");

    CHECK(offsetof(WalManager, current_pos) % 64 == 0 &&
          offsetof(WalManager, write_lsn) % 64 == 0 &&
          offsetof(WalManager, write_lsn) - offsetof(WalManager, current_pos) >= 64 &&
          offsetof(WalManager, current_pos) >= 64,
          "current_pos and write_lsn each start their own cache line, apart from the read-only fields");

    /* Fresh directory: segment 0, first record at LSN 24 = record byte 0. */
    CHECK(atomic_load(&wm.current_pos) == 0, "current_pos starts at record byte 0 (LSN 24)");
    CHECK(atomic_load(&wm.write_lsn) == 0, "write_lsn starts at the segment's start, LSN 0");

    CHECK(wal_manager_shutdown(&wm) == MYDB_OK && wm.ring_buf == NULL && wm.fs_counters == NULL,
          "shutdown frees the ring and counters");

    /* Same directory again: segment 0 is still ACTIVE on disk, so this
     * is not a brand-new log. init resumes it (it used to claim segment 1
     * here): segment 0 is unwritten, so the end is LSN 0, as before. */
    memset(&wm, 0, sizeof(wm));
    CHECK(wal_manager_init(&wm, TEST_WAL_DIR, 7) == MYDB_OK &&
          wm.pool.slots[wm.curr_slot].header.segment_no == 0,
          "a second init in the same directory resumes segment 0");
    CHECK(atomic_load(&wm.current_pos) == 0 && atomic_load(&wm.write_lsn) == 0,
          "and resumes at LSN 0: nothing was written");
    CHECK(wm.pool.slots[1].header.state == SEG_FREE, "slot 1 is still FREE: nothing was claimed");
    wal_manager_shutdown(&wm);
    cleanup();
}

/* Writes a hand-built log into a fresh directory, segment 0 claimed. */
static WalSegmentPool resume_pool;
static uint32_t resume_setup(void)
{
    cleanup();
    uint32_t s0 = 0;
    wal_segment_pool_init(&resume_pool, TEST_WAL_DIR, 7);
    wal_segment_pool_claim_next(&resume_pool, &s0);
    return s0;
}

static int counters_all_zero_but(WalManager *wm, uint32_t idx)
{
    for (uint32_t i = 0; i <= wm->ctr_mask; i++)
        if (i != idx && atomic_load(&wm->fs_counters[i].done) != 0) return 0;
    return 1;
}

/* V2 resume 4b-3 (V2 §10.7): init resumes an existing log at its end. */
static void test_resume_positions(void)
{
    printf("\n[test_resume_positions]\n");
    WalManager wm;
    TestLog tl;
    uint32_t s0;

    /* Three records, the end mid-page at LSN 756. */
    s0 = resume_setup();
    tl_begin(&tl, 0, 0);
    tl_record(&tl, 100, 0x11); tl_record(&tl, 200, 0x22); tl_record(&tl, 300, 0x33);
    tl_write(&tl, &resume_pool, s0);
    wal_segment_pool_shutdown(&resume_pool);

    memset(&wm, 0, sizeof(wm));
    CHECK(wal_manager_init(&wm, TEST_WAL_DIR, 7) == MYDB_OK && wm.curr_slot == s0,
          "init resumes the log in segment 0's slot");
    CHECK(atomic_load(&wm.write_lsn) == 756 && atomic_load(&wm.current_pos) == 756 - 24,
          "write_lsn = the end (756), current_pos = 732 record bytes before it");
    CHECK(memcmp(wm.ring_buf, tl.img, 756) == 0, "ring frame 0 holds the page's 756 bytes from disk");
    int tail_zero = 1;
    for (uint32_t i = 756; i < 4096; i++) if (wm.ring_buf[i]) { tail_zero = 0; break; }
    CHECK(tail_zero, "and zeroes after them");
    CHECK(atomic_load(&wm.fs_counters[1].done) == 756 - 512 && counters_all_zero_but(&wm, 1),
          "the end's block (block 1) is seeded with its 244 bytes, every other counter zero");
    CHECK(wm.last_ctr_idx == 1, "the write step's scan starts at block 1");
    wal_manager_shutdown(&wm);
    free(tl.img);

    /* A record filling page 0 exactly: the end is the page end, 4096. */
    s0 = resume_setup();
    tl_begin(&tl, 0, 0);
    tl_record(&tl, WAL_PAGE_USABLE - WAL_RECORD_HEADER_SIZE, 0x11);
    tl_write(&tl, &resume_pool, s0);
    wal_segment_pool_shutdown(&resume_pool);
    memset(&wm, 0, sizeof(wm));
    CHECK(wal_manager_init(&wm, TEST_WAL_DIR, 7) == MYDB_OK &&
          atomic_load(&wm.write_lsn) == 4096 && atomic_load(&wm.current_pos) == 4072,
          "an end on a page end: write_lsn 4096, current_pos 4072");
    CHECK(wm.last_ctr_idx == 8 && counters_all_zero_but(&wm, UINT32_MAX) && wm.ring_buf[0] == 0,
          "nothing copied or seeded; the scan starts at frame 1's block 0 (counter 8)");
    wal_manager_shutdown(&wm);
    free(tl.img);

    /* Page 0's header written but no record: the end is LSN 24, resumed as
     * the page's start so the next copier writes the header itself. */
    s0 = resume_setup();
    tl_begin(&tl, 0, 0);
    tl_page_header(&tl, 0, 0);
    tl.pos = 24;
    tl_write(&tl, &resume_pool, s0);
    wal_segment_pool_shutdown(&resume_pool);
    memset(&wm, 0, sizeof(wm));
    CHECK(wal_manager_init(&wm, TEST_WAL_DIR, 7) == MYDB_OK &&
          atomic_load(&wm.write_lsn) == 0 && atomic_load(&wm.current_pos) == 0,
          "an end at offset 24 resumes at the page start: write_lsn 0, current_pos 0");
    CHECK(wm.last_ctr_idx == 0 && counters_all_zero_but(&wm, UINT32_MAX),
          "no counter seeded: the next copier counts the header's 24 bytes");
    wal_manager_shutdown(&wm);
    free(tl.img);
    cleanup();
}

/* The log is cut at its end: valid records the crashed run wrote after a
 * torn one are zeroed, so they can never pass for log after the next
 * crash; a second start finds the same end. */
static void test_resume_cuts_the_log(void)
{
    printf("\n[test_resume_cuts_the_log]\n");
    WalManager wm;
    TestLog tl;
    uint32_t s0 = resume_setup();

    tl_begin(&tl, 0, 0);
    tl_record(&tl, 100, 0x11);
    uint64_t torn = tl_record(&tl, 5000, 0x22);   /* torn: one body byte flipped */
    uint64_t after = tl_record(&tl, 100, 0x33);   /* valid, written after it */
    tl.img[torn + 44 + 10] ^= 0x01;
    tl_write(&tl, &resume_pool, s0);
    wal_segment_pool_shutdown(&resume_pool);

    memset(&wm, 0, sizeof(wm));
    CHECK(wal_manager_init(&wm, TEST_WAL_DIR, 7) == MYDB_OK && atomic_load(&wm.write_lsn) == torn,
          "the end is the torn record's start");
    uint8_t page[WAL_PAGE_SIZE];
    wal_segment_pool_read_page(&wm.pool, s0, wal_lsn_page(after) + 1, page);
    int zero = 1;
    for (uint32_t i = 0; i < WAL_PAGE_SIZE; i++) if (page[i]) { zero = 0; break; }
    CHECK(zero, "the valid record written after it is zeroed on disk (its whole page)");
    wal_segment_pool_read_page(&wm.pool, s0, wal_lsn_page(torn) + 1, page);
    CHECK(memcmp(page, tl.img, wal_lsn_offset(torn)) == 0 && page[wal_lsn_offset(torn)] == 0,
          "the end's page keeps its bytes before the end, zeroes from it");
    wal_manager_shutdown(&wm);

    memset(&wm, 0, sizeof(wm));
    CHECK(wal_manager_init(&wm, TEST_WAL_DIR, 7) == MYDB_OK && atomic_load(&wm.write_lsn) == torn &&
          wm.curr_slot == s0,
          "a second start finds the same end in the same slot");
    wal_manager_shutdown(&wm);
    free(tl.img);
    cleanup();
}

/* The end lies in the previous segment: a record crossing into segment 1
 * whose tail there never reached disk. Segment 0, finished (SEG_DONE),
 * becomes ACTIVE again and segment 1 is discarded, its number reused. */
static void test_resume_in_the_previous_segment(void)
{
    printf("\n[test_resume_in_the_previous_segment]\n");
    WalManager wm;
    TestLog tl;
    uint32_t s0 = resume_setup(), s1;
    wal_segment_pool_claim_next(&resume_pool, &s1);

    tl_begin(&tl, 0, 0);
    tl_fill_segment_0(&tl, 1000);
    uint64_t cross = tl_record(&tl, 3000, 0x22);
    tl_write2(&tl, &resume_pool, s0, s1);
    uint8_t zero[WAL_PAGE_SIZE];
    memset(zero, 0, sizeof(zero));
    wal_segment_pool_write_page(&resume_pool, s1, 1, zero);          /* its tail never landed */
    TestLog later;                                                   /* but a later page of it did */
    tl_begin(&later, 1, 0);
    later.pos = (1ull << 22) + 4096;
    tl_record(&later, 100, 0x44);
    wal_segment_pool_write_page(&resume_pool, s1, 2, later.img + 4096);
    wal_segment_pool_mark_done(&resume_pool, NULL, s0, 1ull << 22, 1024, NULL);
    wal_segment_pool_shutdown(&resume_pool);

    memset(&wm, 0, sizeof(wm));
    CHECK(wal_manager_init(&wm, TEST_WAL_DIR, 7) == MYDB_OK && wm.curr_slot == s0 &&
          atomic_load(&wm.write_lsn) == cross,
          "the end is the crossing record's start, in segment 0's slot");
    CHECK(wm.pool.slots[s0].header.state == SEG_ACTIVE, "segment 0 is ACTIVE again (it was DONE)");
    CHECK(wm.pool.slots[s1].header.state == SEG_FREE && wm.pool.slots[s1].header.segment_no == 0 &&
          wm.pool.slots[s1].header.start_lsn == 0,
          "segment 1 is discarded: FREE, its number and start cleared");
    CHECK(wm.pool.next_segment_no == 1, "the next segment claimed is segment 1 again");
    uint8_t page[WAL_PAGE_SIZE];
    wal_segment_pool_read_page(&wm.pool, s1, 2, page);
    CHECK(memcmp(page, zero, WAL_PAGE_SIZE) == 0,
          "segment 1's later page, valid for that position, is zeroed with it");
    free(later.img);
    wal_manager_shutdown(&wm);
    free(tl.img);
    cleanup();
}

int main(void)
{
    printf("=== test_wal_manager ===\n");

    test_init_and_shutdown_bring_up_all_subpieces();
    test_insert_fields();
    test_resume_positions();
    test_resume_cuts_the_log();
    test_resume_in_the_previous_segment();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
