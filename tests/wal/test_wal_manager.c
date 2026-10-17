#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>

#include "common.h"
#include "wal_manager.h"

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
          "a single init() call brings up the worker + ring buffer + pool + Flusher");
    CHECK(wm.worker.mb.started == 1, "the worker thread was started as part of init()");
    CHECK(wm.flusher.started == 1, "the Flusher thread was started as part of init()");
    CHECK(wm.flusher.worker == &wm.worker, "the Flusher was handed the manager's own worker");
    CHECK(wm.pool.slots[wm.flusher.seg_slot_index].header.state == SEG_ACTIVE,
          "init() claimed an active segment for the Flusher to use");
    uint64_t seg = wm.pool.slots[wm.flusher.seg_slot_index].header.segment_no;
    CHECK(wm.rb.current_lsn == wal_segment_first_lsn(seg),
          "the ring starts at the claimed segment's first record LSN");
    CHECK(wal_flusher_flushed_lsn(&wm.flusher) == wm.rb.current_lsn,
          "and flush_lsn starts at that same position");

    CHECK(wal_manager_shutdown(&wm) == MYDB_OK, "shutdown() succeeds cleanly");
    CHECK(wm.flusher.started == 0, "the Flusher thread was stopped by shutdown()");
    CHECK(wm.worker.mb.started == 0, "the worker thread was stopped by shutdown()");

    cleanup();
}

int main(void)
{
    printf("=== test_wal_manager ===\n");

    test_init_and_shutdown_bring_up_all_subpieces();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
