#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>

#include "common.h"
#include "normal_wal/wal_types.h"
#include "large_wal/large_wal_manager.h"
#include "large_wal/large_wal_api.h"

#define TEST_WAL_DIR "/tmp/mydb_test_large_wal_manager"

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

/* Builds one real, serialized WAL record — submits are record-aware, so
 * the api layer needs genuine records, not raw filler. Returns total_len. */
static uint32_t build_record(uint8_t *out, uint64_t lsn, uint8_t rec_type,
                              uint32_t body_len, uint8_t fill)
{
    uint8_t *body = malloc(body_len ? body_len : 1);
    memset(body, fill, body_len);

    WalRecordHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.lsn       = lsn;
    hdr.total_len = WAL_RECORD_HEADER_SIZE + body_len;
    hdr.rec_type  = rec_type;

    wal_record_header_serialize(&hdr, body, body_len, out);
    free(body);
    return WAL_RECORD_HEADER_SIZE + body_len;
}

static void test_init_and_shutdown_bring_up_all_subpieces(void)
{
    printf("\n[test_init_and_shutdown_bring_up_all_subpieces]\n");
    cleanup();

    LargeWalManager mgr;
    CHECK(large_wal_manager_init(&mgr, TEST_WAL_DIR, 1, NULL) == MYDB_OK,
          "a single init() call brings up pool/registry/index/state/archiver/writer");
    CHECK(mgr.partition_id == 1, "mgr's own partition_id is set");
    CHECK(strcmp(mgr.wal_dir, TEST_WAL_DIR) == 0, "mgr's own wal_dir is set");
    CHECK(mgr.lw_writer.mb.started == 1, "the writer thread was started as part of init()");
    CHECK(mgr.lw_pool.slots[mgr.lw_writer.cur_slot_index].header.state == LSEG_ACTIVE,
          "init() claimed an active segment for the writer to use");

    CHECK(large_wal_manager_shutdown(&mgr) == MYDB_OK, "shutdown() succeeds cleanly");
    CHECK(mgr.lw_writer.mb.started == 0, "the writer thread was stopped by shutdown()");

    cleanup();
}

static void test_write_and_get_round_trip_through_api(void)
{
    printf("\n[test_write_and_get_round_trip_through_api]\n");
    cleanup();

    LargeWalManager mgr;
    CHECK(large_wal_manager_init(&mgr, TEST_WAL_DIR, 1, NULL) == MYDB_OK, "manager init succeeds");

    /* A multi-record batch, end to end through the api layer — the
     * proof that a genuinely non-zero LargeWalIndexEntry.offset is
     * honoured all the way from packing to large_wal_get(). */
    uint8_t blob[4096];
    uint32_t l1 = build_record(blob,      42, WAL_REC_SCHEMA_UPDATE, 900, 0x7A);
    uint32_t l2 = build_record(blob + l1, 43, WAL_REC_SCHEMA_UPDATE, 700, 0x7B);

    LargeWalIndexEntry entries[4];
    uint32_t count = 0;
    CHECK(large_wal_write(&mgr, blob, l1 + l2, entries, 4, &count) == MYDB_OK,
          "large_wal_write accepts a two-record batch through the api layer");
    CHECK(count == 2, "both records reported");
    CHECK(entries[0].content_lsn == 42 && entries[1].content_lsn == 43,
          "LSNs came from the records' own headers");
    CHECK(entries[0].start_page_no == entries[1].start_page_no && entries[1].offset == l1,
          "both share a page, and the second carries a real non-zero offset");

    uint8_t out_buf[4096];
    uint32_t out_len = 0;
    CHECK(large_wal_get(&mgr, 42, out_buf, &out_len) == MYDB_OK &&
          out_len == l1 && memcmp(out_buf, blob, l1) == 0,
          "large_wal_get resolves the first record exactly");
    CHECK(large_wal_get(&mgr, 43, out_buf, &out_len) == MYDB_OK &&
          out_len == l2 && memcmp(out_buf, blob + l1, l2) == 0,
          "large_wal_get resolves the second record from mid-page, honouring its offset");

    CHECK(large_wal_get(&mgr, 9999, out_buf, &out_len) == MYDB_ERR_NOT_FOUND,
          "large_wal_get on an unindexed content_lsn misses cleanly");

    large_wal_manager_shutdown(&mgr);
    cleanup();
}

/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_large_wal_manager ===\n");

    test_init_and_shutdown_bring_up_all_subpieces();
    test_write_and_get_round_trip_through_api();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
