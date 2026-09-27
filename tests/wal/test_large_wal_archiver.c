#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>

#include "common.h"
#include "large_wal/large_wal_page.h"
#include "large_wal/large_wal_segment.h"
#include "large_wal/large_wal_segment_pool.h"
#include "large_wal/large_wal_registry.h"
#include "large_wal/large_wal_index.h"
#include "large_wal/large_wal_archiver.h"

#define TEST_WAL_DIR "/tmp/mydb_test_large_wal_archiver"

/* reg=NULL on init throughout this file: these tests drive copy_out and
 * try_free directly, with no thread, so they want none of init's
 * holding-area scan -- the registry entries they care about are the
 * ones build_done_segment makes by hand. The scan is exercised by
 * test_large_wal_concurrency's restart test instead. */

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

/* Removes everything under TEST_WAL_DIR (segment-pool slots, archival
 * copies whose segment_no varies run to run, and the index file), then
 * the directory itself. Safe whether or not the directory exists yet. */
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

/* Builds one complete PAGE_SIZE page buffer (real header + content) —
 * same helper test_large_wal_segment_pool.c uses. */
static void build_page(uint8_t *out, uint64_t content_lsn, uint8_t page_index, uint8_t fill, uint16_t data_len)
{
    WalPageHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.id.file_type = FILETYPE_LARGE_WAL_PAGE;
    hdr.start_lsn    = content_lsn;
    hdr.end_lsn      = content_lsn;
    hdr.data_len     = data_len;
    /* page_index is gone from the shared WalPageHeader; a non-zero one
     * meant "this page continues a record started earlier", which is now
     * exactly what the INCOMING_CONTINUATION flag says. */
    hdr.flags        = page_index > 0 ? WAL_PAGE_FLAG_INCOMING_CONTINUATION : 0;
    memset(out, 0, PAGE_SIZE);
    memset(out + LARGE_WAL_PAGE_HEADER_ON_DISK_SIZE, fill, data_len);
    large_wal_page_header_serialize(&hdr, out);
}

/* Resolves segment_no -> fd via the registry directly and reassembles
 * page_count pages starting at start_page_no — the same logic
 * large_wal_get (large_wal_api.c) uses, kept small and duplicated here
 * on purpose so this test stays decoupled from the manager/api layer,
 * the same way test_partition.c/test_schema_file.c never route through
 * pm_api.c to verify their own layer's behavior. */
static int verify_content(LargeWalRegistry *reg, uint64_t segment_no,
                           uint32_t start_page_no, uint8_t page_count,
                           uint8_t *out_buf, uint32_t *out_len)
{
    int fd;
    if (large_wal_registry_lookup(reg, segment_no, &fd) != MYDB_OK) return MYDB_ERR;

    uint32_t written = 0;
    for (uint8_t p = 0; p < page_count; p++) {
        uint32_t page_no = start_page_no + p;
        uint8_t  page_buf[PAGE_SIZE];
        if (pread(fd, page_buf, PAGE_SIZE, (off_t)page_no * PAGE_SIZE) != PAGE_SIZE) return MYDB_ERR;

        WalPageHeader hdr;
        if (large_wal_page_header_deserialize(page_buf, &hdr) != MYDB_OK) return MYDB_ERR;

        memcpy(out_buf + written, page_buf + LARGE_WAL_PAGE_HEADER_ON_DISK_SIZE, hdr.data_len);
        written += hdr.data_len;
    }
    *out_len = written;
    return MYDB_OK;
}

/* Common fixture: init pool + registry + archiver + index in the same
 * wal_dir, claim a segment, write a synthetic 2-page record
 * (content_lsn=500), mark_done it. Registers the claimed segment in reg
 * (owns_fd=0, mirroring what large_wal_writer would do at claim time)
 * so copy_out has something to repoint. */
static void build_done_segment(LargeWalSegmentPool *pool, LargeWalRegistry *reg,
                                uint32_t *out_slot_index, uint64_t *out_segment_no,
                                uint64_t *out_end_lsn, uint16_t *out_len_a, uint16_t *out_len_b)
{
    uint32_t slot_index;
    CHECK(large_wal_segment_pool_claim_next(pool, &slot_index) == MYDB_OK, "claim_next succeeds");

    uint64_t segment_no = pool->slots[slot_index].header.segment_no;
    CHECK(large_wal_registry_register(reg, segment_no, pool->slots[slot_index].fd, /*owns_fd=*/0) == MYDB_OK,
          "registering the freshly claimed segment succeeds");

    uint16_t len_a = 6000, len_b = 7000;
    uint8_t page_a[PAGE_SIZE], page_b[PAGE_SIZE];
    build_page(page_a, 500, 0, 0xAA, len_a);
    build_page(page_b, 500, 1, 0xBB, len_b);

    CHECK(large_wal_segment_pool_write_page(pool, slot_index, 1, page_a) == MYDB_OK, "write page 1");
    CHECK(large_wal_segment_pool_write_page(pool, slot_index, 2, page_b) == MYDB_OK, "write page 2");

    CHECK(large_wal_segment_pool_mark_done(pool, NULL, slot_index, 500, 2, NULL) == MYDB_OK, "mark_done succeeds");

    *out_slot_index = slot_index;
    *out_segment_no = segment_no;
    *out_end_lsn    = 500;
    *out_len_a      = len_a;
    *out_len_b      = len_b;
}

/* ------------------------------------------------------------------ */

static void test_copy_out_frees_slot_and_content_readable(void)
{
    printf("\n[test_copy_out_frees_slot_and_content_readable]\n");
    cleanup();

    LargeWalSegmentPool pool;
    LargeWalRegistry      reg;
    LargeWalArchiver        arc;
    CHECK(large_wal_registry_init(&reg) == MYDB_OK, "registry init succeeds");
    CHECK(large_wal_segment_pool_init(&pool, TEST_WAL_DIR, 1, &reg) == MYDB_OK, "pool init succeeds");
    CHECK(large_wal_archiver_init(&arc, TEST_WAL_DIR, /*reg=*/NULL) == MYDB_OK, "archiver init succeeds");

    uint32_t slot_index; uint64_t segment_no, end_lsn; uint16_t len_a, len_b;
    build_done_segment(&pool, &reg, &slot_index, &segment_no, &end_lsn, &len_a, &len_b);

    CHECK(large_wal_archiver_copy_out(&arc, &pool, &reg, slot_index) == MYDB_OK, "copy_out succeeds");
    CHECK(pool.slots[slot_index].header.state == LSEG_FREE,
          "the rotation slot was freed back to LSEG_FREE");

    char archival_path[400];
    snprintf(archival_path, sizeof(archival_path), "%s/large_wal_archival_%llu.mydb",
             TEST_WAL_DIR, (unsigned long long)segment_no);
    struct stat st;
    CHECK(stat(archival_path, &st) == 0 && st.st_size == LARGE_WAL_SEGMENT_FILE_SIZE,
          "the holding-area copy exists on disk at the full segment file size");

    uint8_t out_buf[2 * PAGE_SIZE];
    uint32_t out_len = 0;
    CHECK(verify_content(&reg, segment_no, 1, 2, out_buf, &out_len) == MYDB_OK,
          "content resolves via the registry after copy_out");
    CHECK(out_len == (uint32_t)len_a + len_b, "the resolved length matches");

    int bytes_ok = 1;
    for (uint16_t i = 0; i < len_a; i++) if (out_buf[i] != 0xAA) bytes_ok = 0;
    for (uint16_t i = 0; i < len_b; i++) if (out_buf[len_a + i] != 0xBB) bytes_ok = 0;
    CHECK(bytes_ok, "the reassembled bytes match the two original pages exactly, headers stripped");

    large_wal_registry_shutdown(&reg);
    large_wal_segment_pool_shutdown(&pool);
    cleanup();
}

static void test_try_free_gated_on_both_conditions(void)
{
    printf("\n[test_try_free_gated_on_both_conditions]\n");
    cleanup();

    LargeWalSegmentPool pool;
    LargeWalRegistry      reg;
    LargeWalArchiver        arc;
    LargeWalIndex             idx;
    large_wal_registry_init(&reg);
    large_wal_segment_pool_init(&pool, TEST_WAL_DIR, 1, &reg);
    large_wal_archiver_init(&arc, TEST_WAL_DIR, /*reg=*/NULL);
    large_wal_index_open(&idx, TEST_WAL_DIR);

    uint32_t slot_index; uint64_t segment_no, end_lsn; uint16_t len_a, len_b;
    build_done_segment(&pool, &reg, &slot_index, &segment_no, &end_lsn, &len_a, &len_b);
    large_wal_archiver_copy_out(&arc, &pool, &reg, slot_index);

    LargeWalIndexEntry e;
    memset(&e, 0, sizeof(e));
    e.content_lsn = 500; e.rec_type = WAL_REC_LARGE_REF; e.segment_no = segment_no;
    e.start_page_no = 1; e.page_count = 2; e.total_size = (uint32_t)len_a + len_b;
    large_wal_index_insert(&idx, &e);

    char archival_path[400];
    snprintf(archival_path, sizeof(archival_path), "%s/large_wal_archival_%llu.mydb",
             TEST_WAL_DIR, (unsigned long long)segment_no);

    int freed;
    CHECK(large_wal_archiver_try_free(&arc, &reg, &idx, segment_no, end_lsn,
                                       /*checkpoint_lsn=*/end_lsn, /*gate_b_cleared=*/1, &freed) == MYDB_OK,
          "try_free with checkpoint_lsn == end_lsn (Gate A fails: not strictly greater) succeeds");
    CHECK(freed == 0, "not freed: Gate A didn't clear");

    CHECK(large_wal_archiver_try_free(&arc, &reg, &idx, segment_no, end_lsn,
                                       /*checkpoint_lsn=*/end_lsn + 100, /*gate_b_cleared=*/0, &freed) == MYDB_OK,
          "try_free with Gate A clear but Gate B not cleared succeeds");
    CHECK(freed == 0, "not freed: Gate B didn't clear");

    struct stat st;
    CHECK(stat(archival_path, &st) == 0, "the holding-area file still exists — neither partial attempt freed it");

    LargeWalIndexEntry out;
    CHECK(large_wal_index_lookup(&idx, 500, &out) == MYDB_OK, "the index entry still resolves");

    CHECK(large_wal_archiver_try_free(&arc, &reg, &idx, segment_no, end_lsn,
                                       /*checkpoint_lsn=*/end_lsn + 100, /*gate_b_cleared=*/1, &freed) == MYDB_OK,
          "try_free with both gates clear succeeds");
    CHECK(freed == 1, "freed: both gates cleared");

    CHECK(stat(archival_path, &st) != 0, "the holding-area file was actually unlinked");
    CHECK(large_wal_index_lookup(&idx, 500, &out) == MYDB_ERR_NOT_FOUND,
          "the index entry was pruned by delete_by_segment");

    int fd;
    CHECK(large_wal_registry_lookup(&reg, segment_no, &fd) == MYDB_ERR_NOT_FOUND,
          "the registry entry for the freed segment is gone");

    large_wal_index_close(&idx);
    large_wal_registry_shutdown(&reg);
    large_wal_segment_pool_shutdown(&pool);
    cleanup();
}

/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_large_wal_archiver ===\n");

    test_copy_out_frees_slot_and_content_readable();
    test_try_free_gated_on_both_conditions();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
