#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "common.h"
#include "wal_page.h"
#include "wal_worker.h"

#define TEST_WAL_DIR "/tmp/mydb_test_wal_worker"
#define SEG(n) ((uint64_t)(n) << WAL_SEG_SHIFT)

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

/* A pool with segments 0..3 claimed, and a flush_lock over flushed. */
typedef struct {
    WalSegmentPool     pool;
    WalGroupCommitLock flush_lock;
    _Atomic uint64_t   flushed;
} Rig;

static void cleanup(void)
{
    char path[300];
    for (uint32_t i = 0; i < WAL_SEGMENT_POOL_SLOTS; i++) {
        snprintf(path, sizeof(path), "%s/wal_%u.mydb", TEST_WAL_DIR, i);
        unlink(path);
    }
    rmdir(TEST_WAL_DIR);
}

static int rig_up(Rig *r)
{
    cleanup();
    atomic_init(&r->flushed, 0);
    if (wal_segment_pool_init(&r->pool, TEST_WAL_DIR, 1) != MYDB_OK) return MYDB_ERR;
    for (int i = 0; i < 4; i++) {
        uint32_t slot;
        if (wal_segment_pool_claim_next(&r->pool, &slot) != MYDB_OK) return MYDB_ERR;
    }
    return wal_gcl_init(&r->flush_lock, &r->flushed);
}

static void rig_down(Rig *r)
{
    wal_gcl_destroy(&r->flush_lock);
    wal_segment_pool_shutdown(&r->pool);
    cleanup();
}

/* Polls *v until it reaches want, for up to 2 s. */
static int reaches(_Atomic uint64_t *v, uint64_t want)
{
    struct timespec ts = { 0, 1000000 };
    for (int i = 0; i < 2000; i++) {
        if (atomic_load(v) >= want) return 1;
        nanosleep(&ts, NULL);
    }
    return atomic_load(v) >= want;
}

static void test_start_stop(void)
{
    printf("\n[test_start_stop]\n");
    Rig r;
    CHECK(rig_up(&r) == MYDB_OK, "rig set up");

    WalWorker w;
    CHECK(wal_worker_start(&w, &r.pool, &r.flush_lock, 0) == MYDB_OK, "start succeeds");
    CHECK(w.started, "started is set");
    CHECK(wal_worker_stop(&w) == MYDB_OK, "stop succeeds");
    CHECK(!w.started, "started is cleared");
    CHECK(wal_worker_stop(&w) == MYDB_OK, "a second stop is a no-op");

    rig_down(&r);
}

static void test_sync_publishes_flushed(void)
{
    printf("\n[test_sync_publishes_flushed]\n");
    Rig r;
    rig_up(&r);

    WalWorker w;
    wal_worker_start(&w, &r.pool, &r.flush_lock, 0);

    wal_worker_request_sync(&w, SEG(1));
    CHECK(reaches(&w.synced_lsn, SEG(1)), "synced_lsn reaches the end of segment 0");
    CHECK(reaches(&r.flushed, SEG(1)), "with flush_lock free, flushed_lsn follows");

    wal_worker_request_sync(&w, SEG(3));
    CHECK(reaches(&w.synced_lsn, SEG(3)), "a request two segments ahead syncs both");
    CHECK(reaches(&r.flushed, SEG(3)), "flushed_lsn follows again");

    wal_worker_request_sync(&w, SEG(2));
    CHECK(wal_worker_stop(&w) == MYDB_OK, "a request behind synced_lsn is harmless");
    CHECK(atomic_load(&w.synced_lsn) == SEG(3), "synced_lsn did not move");
    CHECK(atomic_load(&r.flushed) == SEG(3), "flushed_lsn did not move");

    rig_down(&r);
}

static void test_lock_held_skips_flushed(void)
{
    printf("\n[test_lock_held_skips_flushed]\n");
    Rig r;
    rig_up(&r);

    WalWorker w;
    wal_worker_start(&w, &r.pool, &r.flush_lock, 0);

    /* The test plays a commit leader holding flush_lock. */
    CHECK(wal_gcl_acquire(&r.flush_lock, SEG(4)) == WAL_GCL_ACQUIRED, "test holds flush_lock");

    wal_worker_request_sync(&w, SEG(2));
    CHECK(reaches(&w.synced_lsn, SEG(2)), "the worker syncs without the lock");
    usleep(20000);
    CHECK(atomic_load(&r.flushed) == 0, "it leaves flushed_lsn to the lock holder");

    wal_gcl_release(&r.flush_lock, SEG(1));
    CHECK(atomic_load(&r.flushed) == SEG(1), "the holder's release publishes");

    wal_worker_request_sync(&w, SEG(3));
    CHECK(reaches(&r.flushed, SEG(3)), "with the lock free again, the worker publishes");

    wal_worker_stop(&w);
    rig_down(&r);
}

static void test_stop_serves_request(void)
{
    printf("\n[test_stop_serves_request]\n");
    Rig r;
    rig_up(&r);

    WalWorker w;
    wal_worker_start(&w, &r.pool, &r.flush_lock, 0);
    wal_worker_request_sync(&w, SEG(4));
    CHECK(wal_worker_stop(&w) == MYDB_OK, "stop right after a request succeeds");
    CHECK(atomic_load(&w.synced_lsn) == SEG(4), "the request was served before exit");
    CHECK(atomic_load(&r.flushed) == SEG(4), "and published");

    rig_down(&r);
}

static void test_failed_sync(void)
{
    printf("\n[test_failed_sync]\n");
    Rig r;
    rig_up(&r);

    int saved = r.pool.slots[1].fd;
    r.pool.slots[1].fd = -1;   /* segment 1's fdatasync fails */

    WalWorker w;
    wal_worker_start(&w, &r.pool, &r.flush_lock, 0);
    wal_worker_request_sync(&w, SEG(3));

    int seen = 0;
    for (int i = 0; i < 2000 && !seen; i++) {
        seen = atomic_load(&w.io_error);
        if (!seen) usleep(1000);
    }
    CHECK(seen, "io_error is set");
    CHECK(atomic_load(&w.synced_lsn) == SEG(1), "synced_lsn stops before the failed segment");
    CHECK(atomic_load(&r.flushed) == 0, "nothing is published after a failure");
    CHECK(wal_worker_stop(&w) == MYDB_ERR, "stop reports the failure");

    r.pool.slots[1].fd = saved;
    rig_down(&r);
}

int main(void)
{
    printf("=== test_wal_worker ===\n");

    test_start_stop();
    test_sync_publishes_flushed();
    test_lock_held_skips_flushed();
    test_stop_serves_request();
    test_failed_sync();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
