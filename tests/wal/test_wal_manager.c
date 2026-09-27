#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>

#include "common.h"
#include "normal_wal/wal_types.h"
#include "wal_manager.h"
#include "large_wal/large_wal_api.h"
#include "large_wal/large_wal_buffer.h"

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

/* Builds one real, serialized WAL record — submits are record-aware, so
 * these need genuine records, not raw filler. Returns total_len. */
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

static void test_init_and_shutdown_wire_the_shared_worker(void)
{
    printf("\n[test_init_and_shutdown_wire_the_shared_worker]\n");
    cleanup();

    WalManager wm;
    CHECK(wal_manager_init(&wm, TEST_WAL_DIR, 1) == MYDB_OK,
          "a single init() call brings up the shared worker + both sub-managers");
    CHECK(wm.worker.mb.started == 1, "the shared worker thread was started");
    CHECK(wm.nwm.flusher.worker == &wm.worker,
          "normal_wal's Flusher was handed a pointer to the same shared worker");
    CHECK(wm.lwm.lw_writer.worker == &wm.worker,
          "large_wal's writer was handed a pointer to the same shared worker");

    CHECK(wal_manager_shutdown(&wm) == MYDB_OK, "shutdown() succeeds cleanly");
    CHECK(wm.worker.mb.started == 0, "the shared worker was stopped by shutdown()");

    cleanup();
}

static void test_large_wal_rollover_with_the_real_shared_worker(void)
{
    printf("\n[test_large_wal_rollover_with_the_real_shared_worker]\n");
    cleanup();

    WalManager wm;
    CHECK(wal_manager_init(&wm, TEST_WAL_DIR, 1) == MYDB_OK, "manager init succeeds");

    uint8_t seed[512];
    uint32_t seed_len = build_record(seed, 500, WAL_REC_SCHEMA_UPDATE, 100, 0x11);
    LargeWalIndexEntry seed_entry; uint32_t seed_count = 0;
    CHECK(large_wal_write(&wm.lwm, seed, seed_len, &seed_entry, 1, &seed_count) == MYDB_OK,
          "seed record written through the real api, real worker attached");

    uint32_t old_slot = wm.lwm.lw_writer.cur_slot_index;

    /* Force "nearly full" the same way test_large_wal_writer.c's own
     * rollover test does, but this time with a real (not NULL) worker
     * actually wired in — exercising mark_done()'s async-offload path
     * and write()'s wal_worker_wait() for real, not just the mechanics
     * with worker == NULL. */
    wm.lwm.lw_writer.cur_page_no = LARGE_WAL_SEGMENT_PAGES_PER_FILE - 1;
    wm.lwm.lw_writer.cur_offset  = 0;

    uint32_t body = LARGE_WAL_PAGE_USABLE;   /* header+body spills past one page */
    uint8_t *big = malloc(WAL_RECORD_HEADER_SIZE + body);
    uint32_t big_len = build_record(big, 600, WAL_REC_SCHEMA_UPDATE, body, 0x22);

    LargeWalIndexEntry entry; uint32_t count = 0;
    CHECK(large_wal_write(&wm.lwm, big, big_len, &entry, 1, &count) == MYDB_OK,
          "a rollover-triggering write succeeds with the real shared worker doing the old segment's fsync");

    CHECK(wm.lwm.lw_pool.slots[old_slot].header.state == LSEG_DONE,
          "the old segment reached LSEG_DONE (its fsync was confirmed via wal_worker_wait before this call returned)");
    CHECK(entry.segment_no != wm.lwm.lw_pool.slots[old_slot].header.segment_no,
          "the record landed in a genuinely new segment");

    uint8_t *out_buf = malloc(big_len);
    uint32_t out_len = 0;
    CHECK(large_wal_get(&wm.lwm, 600, out_buf, &out_len) == MYDB_OK,
          "the rolled-over record resolves correctly");
    CHECK(out_len == big_len, "the resolved length matches");
    /* The body must match byte-for-byte, but NOT the whole record: this
     * one spans pages, so the writer patched its header's flags byte and
     * recomputed the checksum. Validating the reassembled record is the
     * right check — an exact memcmp against the pre-patch bytes would be
     * asserting the patch never happened. */
    CHECK(memcmp(out_buf + WAL_RECORD_HEADER_SIZE, big + WAL_RECORD_HEADER_SIZE, body) == 0,
          "the body reads back byte-for-byte after a real-worker-assisted rollover");
    WalRecordHeader rh;
    CHECK(wal_record_header_deserialize(out_buf, body, &rh) == MYDB_OK &&
          (rh.flags & WAL_RECORD_FLAG_CONTINUES_ON_NEW_PAGE) != 0,
          "the reassembled record still validates, carrying CONTINUES_ON_NEW_PAGE");

    free(out_buf); free(big);

    wal_manager_shutdown(&wm);
    cleanup();
}

int main(void)
{
    printf("=== test_wal_manager ===\n");

    test_init_and_shutdown_wire_the_shared_worker();
    test_large_wal_rollover_with_the_real_shared_worker();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
