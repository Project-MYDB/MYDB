#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "common.h"
#include "wal_types.h"
#include "wal_page.h"
#include "wal_segment_pool.h"
#include "wal_ring_buffer.h"
#include "wal_flusher.h"
#include "wal_manager.h"

#define TEST_WAL_DIR "/tmp/mydb_test_wal_flusher"

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

static WalRecordHeader make_header(void)
{
    WalRecordHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.table_id = 5;
    hdr.page_no  = 7;
    hdr.rec_type = WAL_REC_INSERT;
    return hdr;
}

/* Appends one record big enough to run past the end of whatever page is
 * open, so every earlier record sits on a closed page — this phase's
 * Flusher only drains closed frames (copying the open one early is the
 * Flusher piece's job). (It used to force a fresh frame under the old
 * no-spanning rule; now it simply spans.) */
static void force_frame_close(WalRingBuffer *rb)
{
    WalRecordHeader hdr = make_header();
    static uint8_t body[WAL_PAGE_USABLE];
    memset(body, 0xEE, sizeof(body));
    uint64_t lsn, end;
    wal_ring_buffer_append(rb, &hdr, body, sizeof(body), &lsn, &end);
}

/* Validates everything the Flusher wrote, segment by segment in segment
 * order: every page header checksums and carries its own position as
 * start_lsn; the data parts of the pages, joined, form one stream of
 * records that each deserialize (checksum-valid) and whose lsn equals the
 * position they were read from. Records may span pages and segments, so
 * they can't be checked page by page any more. Every page the Flusher
 * writes is full, so each one adds WAL_PAGE_USABLE bytes to the stream
 * (pages used to say how much they held in data_len, which is gone).
 * Stops at the first page that was never written; a record cut off there
 * is the unflushed tail. Returns the number of records verified, or -1
 * on the first failure. */
static long verify_log(WalSegmentPool *pool)
{
    size_t   cap = 32u * 1024 * 1024, len = 0;
    uint8_t *stream = malloc(cap);
    uint64_t first_ord = 0;
    int      have_first = 0;

    for (uint64_t seg = 0; ; seg++) {
        int slot = -1;
        for (int i = 0; i < WAL_SEGMENT_POOL_SLOTS; i++)
            if (pool->slots[i].header.start_lsn != 0 && pool->slots[i].header.segment_no == seg)
                slot = i;
        if (slot < 0) break;

        int ended = 0;
        for (uint32_t p = 1; p < WAL_SEGMENT_PAGES_PER_FILE; p++) {
            uint8_t buf[WAL_PAGE_SIZE];
            WalPageHeader ph;
            if (wal_segment_pool_read_page(pool, (uint32_t)slot, p, buf) != MYDB_OK ||
                wal_page_header_deserialize(buf, &ph) != MYDB_OK) { ended = 1; break; }
            if (ph.start_lsn != seg * WAL_SEG_SIZE + (uint64_t)p * WAL_PAGE_SIZE) { free(stream); return -1; }
            if (!have_first) {
                first_ord  = wal_lsn_data_ord(ph.start_lsn + WAL_PAGE_HEADER_SIZE + ph.continuation_len);
                memcpy(stream, buf + WAL_PAGE_HEADER_SIZE + ph.continuation_len,
                       WAL_PAGE_USABLE - ph.continuation_len);
                len = WAL_PAGE_USABLE - ph.continuation_len;
                have_first = 1;
            } else {
                memcpy(stream + len, buf + WAL_PAGE_HEADER_SIZE, WAL_PAGE_USABLE);
                len += WAL_PAGE_USABLE;
            }
        }
        if (ended) break;
    }

    long   records = 0;
    size_t off = 0;
    while (off + WAL_RECORD_HEADER_SIZE <= len) {
        uint32_t total_len;
        memcpy(&total_len, stream + off + 16, 4);   /* total_len field offset within WalRecordHeader */
        if (total_len < WAL_RECORD_HEADER_SIZE || off + total_len > len) break;   /* tail not flushed */
        WalRecordHeader rh;
        if (wal_record_header_deserialize(stream + off, total_len - WAL_RECORD_HEADER_SIZE, &rh) != MYDB_OK ||
            rh.lsn != wal_data_ord_lsn(first_ord + off)) {
            free(stream);
            return -1;
        }
        records++;
        off += total_len;
    }
    free(stream);
    return records;
}

/* ------------------------------------------------------------------ */

static void test_demand_signal(void)
{
    printf("\n[test_demand_signal]\n");
    cleanup();

    WalSegmentPool pool;
    CHECK(wal_segment_pool_init(&pool, TEST_WAL_DIR, 1) == MYDB_OK, "segment pool init succeeds");
    uint32_t slot;
    CHECK(wal_segment_pool_claim_next(&pool, &slot) == MYDB_OK, "claim succeeds");
    WalRingBuffer rb;
    wal_ring_buffer_init(&rb, wal_segment_first_lsn(pool.slots[slot].header.segment_no));

    WalFlusher flusher;
    CHECK(wal_flusher_start(&flusher, &rb, &pool, slot, NULL) == MYDB_OK, "flusher starts");
    CHECK(wal_flusher_flushed_lsn(&flusher) == WAL_FIRST_RECORD_LSN, "flush_lsn starts at the ring's start");

    uint64_t last_end = 0;
    for (int i = 0; i < 10; i++) {
        WalRecordHeader hdr = make_header();
        uint8_t body[50] = {0};
        uint64_t lsn, end;
        wal_ring_buffer_append(&rb, &hdr, body, sizeof(body), &lsn, &end);
        last_end = end;
    }
    force_frame_close(&rb);

    wal_flusher_signal(&flusher);
    CHECK(wal_flusher_wait_flushed(&flusher, last_end) == MYDB_OK,
          "wait_flushed returns once the demand-signaled flusher catches up");
    uint64_t fl = wal_flusher_flushed_lsn(&flusher);
    CHECK(fl >= last_end && fl % WAL_PAGE_SIZE == 0, "flush_lsn covers the record and sits on a page end");

    wal_flusher_stop(&flusher);
    CHECK(verify_log(&pool) == 10, "the 10 records on disk deserialize, checksum, and sit at their LSNs");

    wal_segment_pool_shutdown(&pool);
    wal_ring_buffer_shutdown(&rb);
    cleanup();
}

static void test_periodic_timeout(void)
{
    printf("\n[test_periodic_timeout]\n");
    cleanup();

    WalSegmentPool pool;
    wal_segment_pool_init(&pool, TEST_WAL_DIR, 1);
    uint32_t slot;
    wal_segment_pool_claim_next(&pool, &slot);
    WalRingBuffer rb;
    wal_ring_buffer_init(&rb, wal_segment_first_lsn(pool.slots[slot].header.segment_no));

    WalFlusher flusher;
    CHECK(wal_flusher_start(&flusher, &rb, &pool, slot, NULL) == MYDB_OK, "flusher starts");

    /* Append 144-byte records (no signal at all) until one crosses out of
     * page 1 — track the end of the last record wholly inside page 1. */
    uint64_t page1_last_end = 0;
    int crossed = 0;
    for (int i = 0; i < 40 && !crossed; i++) {
        WalRecordHeader hdr = make_header();
        uint8_t body[100] = {0};
        uint64_t lsn, end;
        wal_ring_buffer_append(&rb, &hdr, body, sizeof(body), &lsn, &end);
        if (end <= 8192) page1_last_end = end;
        else crossed = 1;
    }
    CHECK(crossed, "enough records were appended to close page 1");

    /* No wal_flusher_signal call here — this test is specifically about
     * the periodic WAL_FLUSHER_PERIODIC_MS timeout path. */
    struct timespec wait_ts = { 0, 200 * 1000 * 1000L };   /* 200ms, 10x the interval */
    nanosleep(&wait_ts, NULL);

    CHECK(wal_flusher_flushed_lsn(&flusher) >= page1_last_end,
          "the periodic timeout drained page 1 with no explicit signal");

    wal_flusher_stop(&flusher);
    wal_segment_pool_shutdown(&pool);
    wal_ring_buffer_shutdown(&rb);
    cleanup();
}

#define APPENDER_THREADS 4
#define APPENDS_PER_THREAD 50

typedef struct {
    WalRingBuffer *rb;
    WalManager    *wm;        /* when set, append through wal_manager_append */
    int            count;
    size_t         body_len;
    uint64_t       max_end;
    int            failures;
} AppenderArg;

static void *appender_thread(void *arg)
{
    AppenderArg *a = (AppenderArg *)arg;
    uint8_t *body = malloc(a->body_len);
    for (int i = 0; i < a->count; i++) {
        WalRecordHeader hdr = make_header();
        memset(body, (uint8_t)i, a->body_len);
        uint64_t lsn, end;
        int rc = a->wm ? wal_manager_append(a->wm, &hdr, body, a->body_len, &lsn, &end)
                       : wal_ring_buffer_append(a->rb, &hdr, body, a->body_len, &lsn, &end);
        if (rc == MYDB_OK) { if (end > a->max_end) a->max_end = end; }
        else a->failures++;
    }
    free(body);
    return NULL;
}

static void test_concurrent_appenders(void)
{
    printf("\n[test_concurrent_appenders]\n");
    cleanup();

    WalSegmentPool pool;
    wal_segment_pool_init(&pool, TEST_WAL_DIR, 1);
    uint32_t slot;
    wal_segment_pool_claim_next(&pool, &slot);
    WalRingBuffer rb;
    wal_ring_buffer_init(&rb, wal_segment_first_lsn(pool.slots[slot].header.segment_no));

    WalFlusher flusher;
    CHECK(wal_flusher_start(&flusher, &rb, &pool, slot, NULL) == MYDB_OK, "flusher starts");

    pthread_t threads[APPENDER_THREADS];
    AppenderArg args[APPENDER_THREADS];
    memset(args, 0, sizeof(args));
    for (int i = 0; i < APPENDER_THREADS; i++) {
        args[i].rb = &rb;
        args[i].count = APPENDS_PER_THREAD;
        args[i].body_len = 100;
        pthread_create(&threads[i], NULL, appender_thread, &args[i]);
    }
    for (int i = 0; i < APPENDER_THREADS; i++)
        pthread_join(threads[i], NULL);

    uint64_t total_bytes = (uint64_t)APPENDER_THREADS * APPENDS_PER_THREAD
                         * (WAL_RECORD_HEADER_SIZE + 100);
    CHECK(rb.current_lsn == wal_lsn_advance(WAL_FIRST_RECORD_LSN, total_bytes),
          "current_lsn reflects every concurrent append exactly once (no lost/duplicated records)");

    uint64_t max_end = 0;
    for (int i = 0; i < APPENDER_THREADS; i++)
        if (args[i].max_end > max_end) max_end = args[i].max_end;

    force_frame_close(&rb);   /* so the page holding the last record is drainable */
    wal_flusher_signal(&flusher);
    CHECK(wal_flusher_wait_flushed(&flusher, max_end) == MYDB_OK,
          "wait_flushed returns after concurrent appends settle");

    wal_flusher_stop(&flusher);
    CHECK(verify_log(&pool) == APPENDER_THREADS * APPENDS_PER_THREAD,
          "every record survives concurrent appends + real threaded draining intact — "
          "this is the correctness property the lock/snapshot design exists for");

    wal_segment_pool_shutdown(&pool);
    wal_ring_buffer_shutdown(&rb);
    cleanup();
}

/* More than the whole ring through wal_manager_append, with the Flusher
 * draining at the same time: appenders that hit a full ring wake it, wait
 * for space and carry on — nothing lost, nothing refused, and the log on
 * disk runs across several segments. (Whether a given run actually hits
 * FULL depends on timing; test_manager_append_waits_when_full below
 * forces it.) */
static void test_full_ring_backpressure(void)
{
    printf("\n[test_full_ring_backpressure]\n");
    cleanup();

    WalManager wm;
    memset(&wm, 0, sizeof(wm));
    CHECK(wal_manager_init(&wm, TEST_WAL_DIR, 1) == MYDB_OK, "manager init");

    enum { PER_THREAD = 1100 };
    const size_t body = 4000;   /* 4 threads x 1100 x 4044 B = ~17.8 MB > the 16 MB ring */
    pthread_t threads[APPENDER_THREADS];
    AppenderArg args[APPENDER_THREADS];
    memset(args, 0, sizeof(args));
    for (int i = 0; i < APPENDER_THREADS; i++) {
        args[i].wm = &wm;
        args[i].count = PER_THREAD;
        args[i].body_len = body;
        pthread_create(&threads[i], NULL, appender_thread, &args[i]);
    }
    uint64_t max_end = 0;
    int failures = 0;
    for (int i = 0; i < APPENDER_THREADS; i++) {
        pthread_join(threads[i], NULL);
        failures += args[i].failures;
        if (args[i].max_end > max_end) max_end = args[i].max_end;
    }
    CHECK(failures == 0, "no append failed: a full ring made them wait, not fail");

    uint64_t total = (uint64_t)APPENDER_THREADS * PER_THREAD * (WAL_RECORD_HEADER_SIZE + body);
    CHECK(wm.rb.current_lsn == wal_lsn_advance(WAL_FIRST_RECORD_LSN, total),
          "every byte is accounted for in current_lsn");

    force_frame_close(&wm.rb);
    CHECK(wal_manager_wait_flushed(&wm, max_end) == MYDB_OK, "wait_flushed covers the last record");
    wal_flusher_stop(&wm.flusher);
    CHECK(verify_log(&wm.pool) == APPENDER_THREADS * PER_THREAD,
          "all 4,400 records are on disk across segments, each valid and at its LSN");

    wal_manager_shutdown(&wm);
    cleanup();
}

typedef struct {
    WalManager *wm;
    volatile int done;
    int rc;
} BlockedArg;

static void *blocked_appender(void *arg)
{
    BlockedArg *b = (BlockedArg *)arg;
    WalRecordHeader hdr = make_header();
    uint8_t body[100] = {0};
    uint64_t lsn, end;
    b->rc = wal_manager_append(b->wm, &hdr, body, sizeof(body), &lsn, &end);
    __atomic_store_n(&b->done, 1, __ATOMIC_SEQ_CST);
    return NULL;
}

/* Deterministic version of the above: with the Flusher stopped, fill the
 * ring until append says FULL, then show wal_manager_append blocks (not
 * fails) and resumes as soon as a snapshot frees frames. */
static void test_manager_append_waits_when_full(void)
{
    printf("\n[test_manager_append_waits_when_full]\n");
    cleanup();

    WalManager wm;
    memset(&wm, 0, sizeof(wm));
    wal_manager_init(&wm, TEST_WAL_DIR, 1);
    wal_flusher_stop(&wm.flusher);   /* nobody drains now */

    size_t body_len = WAL_PAGE_USABLE - WAL_RECORD_HEADER_SIZE;
    uint8_t *page_body = calloc(1, body_len);
    uint64_t lsn, end;
    int rc;
    do {
        WalRecordHeader h = make_header();
        rc = wal_ring_buffer_append(&wm.rb, &h, page_body, body_len, &lsn, &end);
    } while (rc == MYDB_OK);
    CHECK(rc == MYDB_ERR_FULL, "the ring fills up with the Flusher stopped");

    BlockedArg b = { &wm, 0, -99 };
    pthread_t t;
    pthread_create(&t, NULL, blocked_appender, &b);
    struct timespec ts = { 0, 100 * 1000 * 1000L };
    nanosleep(&ts, NULL);
    CHECK(!__atomic_load_n(&b.done, __ATOMIC_SEQ_CST), "wal_manager_append is waiting, not failing");

    uint8_t *scratch = malloc(WAL_PAGE_SIZE);
    uint32_t n = 0;
    WalCursor cur;
    wal_ring_buffer_snapshot(&wm.rb, scratch, 1, &n, &cur);   /* free one frame */
    pthread_join(t, NULL);
    CHECK(n == 1 && b.done && b.rc == MYDB_OK, "one drained frame lets it through, and it succeeds");

    free(scratch);
    free(page_body);
    wal_manager_shutdown(&wm);
    cleanup();
}

static void test_stop_join(void)
{
    printf("\n[test_stop_join]\n");
    cleanup();

    WalSegmentPool pool;
    wal_segment_pool_init(&pool, TEST_WAL_DIR, 1);
    uint32_t slot;
    wal_segment_pool_claim_next(&pool, &slot);
    WalRingBuffer rb;
    wal_ring_buffer_init(&rb, wal_segment_first_lsn(pool.slots[slot].header.segment_no));

    WalFlusher flusher;
    wal_flusher_start(&flusher, &rb, &pool, slot, NULL);

    int rc = wal_flusher_stop(&flusher);
    CHECK(rc == MYDB_OK, "stop returns promptly, well under a full periodic cycle's worth of waiting");
    CHECK(flusher.started == 0, "started flag cleared after stop");

    int rc2 = wal_flusher_stop(&flusher);
    CHECK(rc2 == MYDB_OK, "a second stop() call is a harmless no-op, not a crash");

    wal_segment_pool_shutdown(&pool);
    wal_ring_buffer_shutdown(&rb);
    cleanup();
}

static void test_start_requires_aligned_ring(void)
{
    printf("\n[test_start_requires_aligned_ring]\n");
    cleanup();

    WalSegmentPool pool;
    wal_segment_pool_init(&pool, TEST_WAL_DIR, 1);
    uint32_t slot;
    wal_segment_pool_claim_next(&pool, &slot);
    WalRingBuffer rb;
    wal_ring_buffer_init(&rb, wal_segment_first_lsn(pool.slots[slot].header.segment_no) + 100);

    WalFlusher flusher;
    CHECK(wal_flusher_start(&flusher, &rb, &pool, slot, NULL) == MYDB_ERR,
          "a ring that does not start at the segment's first record is refused (no resume yet)");

    wal_segment_pool_shutdown(&pool);
    wal_ring_buffer_shutdown(&rb);
    cleanup();
}

/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_wal_flusher ===\n");

    test_demand_signal();
    test_periodic_timeout();
    test_concurrent_appenders();
    test_full_ring_backpressure();
    test_manager_append_waits_when_full();
    test_stop_join();
    test_start_requires_aligned_ring();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
