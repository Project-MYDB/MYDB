/*
 * test_large_wal_concurrency — exercises large_wal's segment locking.
 *
 * Every other large_wal test drives one thread. That was honest while
 * LargeWalWriter was the only thing touching these structures, but it
 * means the locks added for the archiver have nothing proving them.
 * This file used to FABRICATE the concurrency the LARGE_WAL Archiver
 * thread would later supply for real, running its own hand-rolled
 * driver. That thread now exists (large_wal_archiver.c) and
 * large_wal_manager_init starts it, so these are the real threads:
 * readers racing the writer, readers racing the archiver relocating and
 * freeing segments underneath them, and the writer racing the archiver
 * over the same rotation slot.
 *
 * That distinction matters for what a clean detector run is worth. With
 * a hand-rolled driver, a detector could only vouch for the paths the
 * test itself chose to walk; the writer-vs-archiver lock ordering had
 * to be argued from reading the code, because nothing called copy_out.
 * Now the real thread is the one being watched.
 *
 * IMPORTANT: passing here is necessary but nowhere near sufficient. A
 * data race usually produces correct output until the day it doesn't, so
 * the actual verification is running this under ThreadSanitizer:
 *
 *     cmake -B build-tsan -DMYDB_SANITIZE_THREAD=ON && \
 *         cmake --build build-tsan && \
 *         MYDB_CONCURRENCY_SCALE=20 ./build-tsan/tests/test_large_wal_concurrency
 *
 * Where TSan's runtime isn't installed, valgrind's Helgrind catches the
 * same race and lock-order-inversion classes on the ordinary build:
 *
 *     MYDB_CONCURRENCY_SCALE=40 valgrind --tool=helgrind \
 *         --history-level=none ./build/tests/test_large_wal_concurrency
 *
 * A clean detector run is the pass condition. A green run without one
 * only says nothing crashed this time.
 *
 * MYDB_CONCURRENCY_SCALE divides the workload. Both tools instrument
 * every single lock operation, so the default workload would take hours
 * under them — and piling up iterations is not what a detector needs
 * anyway. It reports a race the first time two threads reach the same
 * unguarded byte, so covering each path a handful of times is worth as
 * much as covering it a million times.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <stdatomic.h>
#include <dirent.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "common.h"
#include "normal_wal/wal_types.h"
#include "large_wal/large_wal_manager.h"
#include "large_wal/large_wal_api.h"

#define TEST_WAL_DIR "/tmp/mydb_test_large_wal_concurrency"

#define N_READERS      4

/* Two record sizes, because the two tests want opposite things.
 *
 * SMALL packs several records onto one 16KB page -- that shared-page
 * case is what makes a writer rewriting a partly-filled tail page able
 * to tear a reader's record, so test 1 uses it.
 *
 * LARGE is ~19 pages, so 6 fit in a 127-page segment. Test 2 needs
 * segments to actually FILL, because a slot only reaches LSEG_DONE on
 * rollover and the archiver has nothing to do until one does. At the
 * small size a scaled-down run never fills even one segment, so the
 * archiver assertions would pass without the archiver having run. */
#define SMALL_BODY     3000
#define LARGE_BODY     300000
#define MAX_BODY       LARGE_BODY
#define MAX_RECORD_LEN (WAL_RECORD_HEADER_SIZE + MAX_BODY)

/* Floor on test 2's record count, applied AFTER scale_down. 6 records
 * fill a segment and 24 fill the whole 4-slot pool, so this has to be
 * comfortably ABOVE 24 -- at exactly 24 the pool is merely used once
 * and nothing is ever freed and re-claimed, which is the entire point.
 * 40 rolls ~7 segments, so slots go round the ring twice over.
 *
 * Without this floor a detector run (MYDB_CONCURRENCY_SCALE=25) drops
 * to a handful of records, never fills a single segment, and the
 * archiver assertions pass without the archiver having done anything. */
#define MIN_ROLLOVER_RECORDS 40

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

/* Divisor from MYDB_CONCURRENCY_SCALE (default 1) — see the file header
 * for why a detector run wants a smaller workload, not a bigger one.
 * Clamped so a stray value can never scale a count to zero. */
static uint32_t scale_down(uint32_t n)
{
    const char *s   = getenv("MYDB_CONCURRENCY_SCALE");
    long        div = s ? strtol(s, NULL, 10) : 1;
    if (div < 1)   div = 1;
    if (div > 500) div = 500;

    uint32_t out = (uint32_t)(n / (uint32_t)div);
    return out ? out : 1;
}

/* Readers otherwise spin flat out, which is what we want on a normal run
 * — it maximises the chance of landing inside a writer's pwrite or an
 * archiver's free. Under a detector it backfires: every thread is
 * serialised onto one core, so four spinning readers starve the writer
 * and the run never finishes. Sleeping between reads keeps the readers
 * alive across the writer's whole duration, which is what actually
 * matters, at a fraction of the instrumented work. */
static useconds_t reader_throttle_us(void)
{
    return (scale_down(1000) == 1000) ? 0 : 200;
}

/* ------------------------------------------------------------------
 * Fixture
 * ------------------------------------------------------------------ */

static void cleanup_dir(void)
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

/* One real serialized record whose body is entirely `fill`. The fill
 * byte is what lets a reader verify it got THIS record's bytes and not a
 * neighbour's, which is the whole point when records pack tightly and
 * share pages. */
static uint32_t build_record(uint8_t *out, uint64_t lsn, uint8_t fill, uint32_t body_len)
{
    uint8_t *body = malloc(body_len);
    memset(body, fill, body_len);

    uint32_t record_len = WAL_RECORD_HEADER_SIZE + body_len;

    WalRecordHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.lsn       = lsn;
    hdr.total_len = record_len;
    hdr.rec_type  = WAL_REC_INSERT;

    wal_record_header_serialize(&hdr, body, body_len, out);
    free(body);
    return record_len;
}

/* ------------------------------------------------------------------
 * Published-entry table — what the writer has landed and readers may
 * therefore ask for. Its own mutex: this is test scaffolding racing
 * alongside the subsystem, not part of what's under test.
 * ------------------------------------------------------------------ */

typedef struct {
    LargeWalIndexEntry entry;
    uint8_t            fill;
} Published;

typedef struct {
    Published      *items;
    uint32_t        count;
    uint32_t        capacity;
    pthread_mutex_t lock;
} PubTable;

static void pub_init(PubTable *t, uint32_t capacity)
{
    t->items    = calloc(capacity, sizeof(Published));
    t->count    = 0;
    t->capacity = capacity;
    pthread_mutex_init(&t->lock, NULL);
}

static void pub_free(PubTable *t)
{
    free(t->items);
    pthread_mutex_destroy(&t->lock);
}

static void pub_add(PubTable *t, const LargeWalIndexEntry *e, uint8_t fill)
{
    pthread_mutex_lock(&t->lock);
    if (t->count < t->capacity) {
        t->items[t->count].entry = *e;
        t->items[t->count].fill  = fill;
        t->count++;
    }
    pthread_mutex_unlock(&t->lock);
}

/* Copies one published record out under the table lock, so the reader
 * never dereferences the array while the writer is appending. */
static int pub_pick(PubTable *t, unsigned *seed, Published *out)
{
    int got = 0;
    pthread_mutex_lock(&t->lock);
    if (t->count > 0) {
        *out = t->items[rand_r(seed) % t->count];
        got  = 1;
    }
    pthread_mutex_unlock(&t->lock);
    return got;
}

/* ------------------------------------------------------------------
 * Reader threads
 * ------------------------------------------------------------------ */

typedef struct {
    LargeWalManager *mgr;
    PubTable        *pub;
    atomic_int      *stop;

    uint32_t reads_ok;        /* returned the exact bytes */
    uint32_t reads_missing;   /* MYDB_ERR_NOT_FOUND -- legitimate: freed */
    uint32_t reads_bad;       /* wrong length or wrong bytes -- a real failure */
} ReaderArgs;

static void *reader_thread(void *arg)
{
    ReaderArgs *a        = arg;
    unsigned    seed     = (unsigned)(uintptr_t)arg;
    uint8_t    *buf      = malloc(MAX_RECORD_LEN);
    useconds_t  throttle = reader_throttle_us();

    while (!atomic_load(a->stop)) {
        if (throttle) usleep(throttle);

        Published p;
        if (!pub_pick(a->pub, &seed, &p)) continue;

        uint32_t len = 0;
        int rc = large_wal_get(a->mgr, p.entry.content_lsn, buf, &len);

        if (rc == MYDB_ERR_NOT_FOUND) {
            /* The archiver freed this segment between publish and read.
             * The correct answer, not a failure. */
            a->reads_missing++;
            continue;
        }
        if (rc != MYDB_OK || len != p.entry.total_size) {
            a->reads_bad++;
            continue;
        }

        /* Compare the BODY only. A record that spans a page boundary has
         * its stored header's flags patched by the writer
         * (WAL_RECORD_FLAG_CONTINUES_ON_NEW_PAGE), so the header bytes on
         * disk legitimately differ from what was submitted. The body is
         * what must survive byte for byte. */
        int ok = 1;
        for (uint32_t i = WAL_RECORD_HEADER_SIZE; i < len; i++) {
            if (buf[i] != p.fill) { ok = 0; break; }
        }
        if (ok) a->reads_ok++;
        else    a->reads_bad++;
    }

    free(buf);
    return NULL;
}

/* ------------------------------------------------------------------
 * Gate provider for the tests.
 *
 * (What used to sit here was a hand-rolled archiver thread doing
 * copy_out on a DONE slot and then freeing it outright -- exactly the
 * relocate-then-delete sequence a reader must never be caught inside.
 * That sequence is unchanged; it is just the real thread running it
 * now instead of this file.)
 *
 * This file used to run its own hand-rolled archiver thread, scanning
 * slots and calling copy_out/try_free itself. That is now the real
 * thread's job (large_wal_archiver.c), started by
 * large_wal_manager_init, so all that is left here is the Gate A / Gate
 * B answer the real thread asks for -- and cannot get anywhere else,
 * since no Checkpointer or Normal WAL Archiver exists yet.
 *
 * Forcing both gates open is deliberate. These tests are about the
 * locking around freeing, not about when freeing is allowed. A gate
 * that ever said "no" would leave segments in the holding area and the
 * relocate-under-a-reader window would never open.
 * ------------------------------------------------------------------ */

static int gate_always_open(void *ctx, uint64_t segment_no, uint64_t segment_end_lsn,
                             uint64_t *out_checkpoint_lsn, int *out_gate_b_cleared)
{
    (void)ctx; (void)segment_no; (void)segment_end_lsn;
    *out_checkpoint_lsn  = UINT64_MAX;   /* Gate A: always past any end_lsn */
    *out_gate_b_cleared  = 1;
    return MYDB_OK;
}

/* Counts how many holding-area files exist right now. Used instead of a
 * counter inside the archiver: the thread keeps no statistics, and
 * asking the filesystem is both simpler and a stronger check -- it
 * observes the actual side effect rather than the archiver's opinion
 * of it. */
static uint32_t count_holding_files(void)
{
    DIR *d = opendir(TEST_WAL_DIR);
    if (!d) return 0;

    uint32_t n = 0;
    struct dirent *ent;
    while ((ent = readdir(d)) != NULL)
        if (strncmp(ent->d_name, "large_wal_archival_", 19) == 0) n++;

    closedir(d);
    return n;
}

/* ------------------------------------------------------------------
 * Test 1 — reader storm vs. writer.
 *
 * Nothing is ever freed here, so every read of a published record must
 * return its exact bytes -- a miss would be a real failure, not a
 * legitimate "it was archived".
 *
 * The archiver thread IS running (large_wal_manager_init starts it),
 * but it has nothing to do: no gate provider is installed, so it can
 * never free anything, and SMALL_BODY records at this count never fill
 * a segment, so no slot ever reaches LSEG_DONE for it to copy out. The
 * total stays under what the 4 rotation slots hold for the same reason
 * it always did -- with no slot being freed, claim_next has nowhere to
 * go once they are used up.
 * ------------------------------------------------------------------ */

static void test_readers_vs_writer(void)
{
    printf("\n[test_readers_vs_writer]\n");
    cleanup_dir();

    const uint32_t n_records = scale_down(400);

    LargeWalManager mgr;
    CHECK(large_wal_manager_init(&mgr, TEST_WAL_DIR, 1, NULL) == MYDB_OK, "manager init succeeds");

    PubTable pub;
    pub_init(&pub, n_records);

    atomic_int stop = 0;
    ReaderArgs   readers[N_READERS];
    pthread_t    tids[N_READERS];
    for (int i = 0; i < N_READERS; i++) {
        memset(&readers[i], 0, sizeof(readers[i]));
        readers[i].mgr  = &mgr;
        readers[i].pub  = &pub;
        readers[i].stop = &stop;
        pthread_create(&tids[i], NULL, reader_thread, &readers[i]);
    }

    const uint32_t rec_len = WAL_RECORD_HEADER_SIZE + SMALL_BODY;
    uint8_t *rec = malloc(rec_len);
    uint32_t written = 0;
    for (uint32_t i = 0; i < n_records; i++) {
        uint8_t fill = (uint8_t)(i & 0xFF);
        build_record(rec, /*lsn=*/1000 + i, fill, SMALL_BODY);

        LargeWalIndexEntry out[4];
        uint32_t           n_out = 0;
        if (large_wal_write(&mgr, rec, rec_len, out, 4, &n_out) != MYDB_OK) break;

        for (uint32_t k = 0; k < n_out; k++) pub_add(&pub, &out[k], fill);
        written++;
    }
    free(rec);

    atomic_store(&stop, 1);
    for (int i = 0; i < N_READERS; i++) pthread_join(tids[i], NULL);

    CHECK(written == n_records, "the writer landed every record");

    uint32_t total_ok = 0, total_bad = 0, total_missing = 0;
    for (int i = 0; i < N_READERS; i++) {
        total_ok      += readers[i].reads_ok;
        total_bad     += readers[i].reads_bad;
        total_missing += readers[i].reads_missing;
    }
    printf("  (%u good reads, %u misses, %u bad)\n", total_ok, total_missing, total_bad);

    CHECK(total_ok > 0, "readers actually got reads in -- the race window was real");
    CHECK(total_bad == 0, "no read ever returned wrong bytes or a wrong length");
    CHECK(total_missing == 0, "nothing was freed, so no read should have missed");

    pub_free(&pub);
    large_wal_manager_shutdown(&mgr);
    cleanup_dir();
}

/* ------------------------------------------------------------------
 * Test 2 — reader storm vs. writer vs. archiver, all three at once.
 *
 * This is the one the locking exists for: the archiver relocates a
 * segment to the holding area and then deletes it, closing its fd, while
 * readers are mid-pread on it and the writer is appending to a different
 * slot. A read may now legitimately MISS (the record really was freed);
 * what it must never do is return wrong bytes, short-read, or crash.
 *
 * It also unblocks the writer: with segments being freed behind it,
 * claim_next keeps finding a LSEG_FREE slot, so this pushes far more
 * records through than the 4 slots could otherwise hold.
 *
 * Uses LARGE_BODY records for a reason. A slot only reaches LSEG_DONE
 * when its segment FILLS, so at SMALL_BODY a scaled-down run would
 * never roll a single segment, the archiver would find nothing to do,
 * and every assertion below would pass without the thing under test
 * ever running. At ~19 pages a record, 6 fill a segment and 24 fill the
 * whole pool -- so the floor below guarantees several rollovers no
 * matter what MYDB_CONCURRENCY_SCALE is set to.
 * ------------------------------------------------------------------ */

/* 6 records per segment x 4 slots. Writing more than this many is only
 * possible if slots were freed and re-claimed. */
#define POOL_CAPACITY_RECORDS 24

static void test_readers_vs_writer_vs_archiver(void)
{
    printf("\n[test_readers_vs_writer_vs_archiver]\n");
    cleanup_dir();

    uint32_t n_records = scale_down(120);
    if (n_records < MIN_ROLLOVER_RECORDS) n_records = MIN_ROLLOVER_RECORDS;

    LargeWalManager mgr;
    CHECK(large_wal_manager_init(&mgr, TEST_WAL_DIR, 1, NULL) == MYDB_OK, "manager init succeeds");

    PubTable pub;
    pub_init(&pub, n_records);

    atomic_int stop = 0;

    ReaderArgs readers[N_READERS];
    pthread_t  reader_tids[N_READERS];
    for (int i = 0; i < N_READERS; i++) {
        memset(&readers[i], 0, sizeof(readers[i]));
        readers[i].mgr  = &mgr;
        readers[i].pub  = &pub;
        readers[i].stop = &stop;
        pthread_create(&reader_tids[i], NULL, reader_thread, &readers[i]);
    }

    /* The archiver thread is already running -- manager_init started
     * it. All this adds is the gate answer it needs before it will free
     * anything. */
    CHECK(large_wal_archiver_set_gates(&mgr.lw_archiver, gate_always_open, NULL) == MYDB_OK,
          "gate provider installed on the running archiver");

    const uint32_t rec_len = WAL_RECORD_HEADER_SIZE + LARGE_BODY;
    uint8_t *rec = malloc(rec_len);
    uint32_t written = 0;
    for (uint32_t i = 0; i < n_records; i++) {
        uint8_t fill = (uint8_t)(i & 0xFF);
        build_record(rec, /*lsn=*/50000 + i, fill, LARGE_BODY);

        LargeWalIndexEntry out[64];
        uint32_t           n_out = 0;
        if (large_wal_write(&mgr, rec, rec_len, out, 64, &n_out) != MYDB_OK) break;

        for (uint32_t k = 0; k < n_out; k++) pub_add(&pub, &out[k], fill);
        written++;
    }
    free(rec);

    atomic_store(&stop, 1);
    for (int i = 0; i < N_READERS; i++) pthread_join(reader_tids[i], NULL);

    uint32_t total_ok = 0, total_bad = 0, total_missing = 0;
    for (int i = 0; i < N_READERS; i++) {
        total_ok      += readers[i].reads_ok;
        total_bad     += readers[i].reads_bad;
        total_missing += readers[i].reads_missing;
    }
    printf("  (%u records written, pool holds %u; %u holding-area files left)\n",
           written, (unsigned)POOL_CAPACITY_RECORDS, count_holding_files());
    printf("  (%u good reads, %u misses, %u bad)\n", total_ok, total_missing, total_bad);

    CHECK(total_ok + total_missing > 0, "readers actually ran against it");
    CHECK(total_bad == 0,
          "no read returned wrong bytes or a wrong length while segments moved underneath");

    /* Freeing is what keeps claim_next supplied, so getting well past
     * what 4 slots hold is the evidence that writer and archiver really
     * did hand the same slots back and forth. This replaces the old
     * "arc.copied > 0" check, which only ever proved the test's own
     * driver had run. */
    CHECK(written == n_records, "the writer landed every record alongside the archiver");
    CHECK(written > POOL_CAPACITY_RECORDS,
          "wrote more than the 4 slots hold -- slots were freed and re-claimed");

    pub_free(&pub);
    large_wal_manager_shutdown(&mgr);
    cleanup_dir();
}

/* ------------------------------------------------------------------
 * Test 3 — registry and index hammered directly.
 *
 * The structures themselves, with no segment files involved: this is
 * where a node freed under a walking reader, or a realloc'd entries
 * array read mid-move, would surface.
 * ------------------------------------------------------------------ */

typedef struct {
    LargeWalRegistry *reg;
    LargeWalIndex    *idx;
    atomic_int       *stop;
    int               id;
    uint32_t          ops;
} HammerArgs;

static void *registry_hammer(void *arg)
{
    HammerArgs *a    = arg;
    unsigned    seed = (unsigned)a->id + 1;

    while (!atomic_load(a->stop)) {
        uint64_t seg = rand_r(&seed) % 64;

        switch (rand_r(&seed) % 4) {
        case 0:
            large_wal_registry_register(a->reg, seg, /*fd=*/-1, /*owns_fd=*/0);
            break;
        case 1: {
            int fd;
            large_wal_registry_lookup(a->reg, seg, &fd);
            break;
        }
        case 2: {
            LargeWalRegistryNode *node = NULL;
            int fd = -1;
            if (large_wal_registry_acquire(a->reg, seg, &node, &fd) == MYDB_OK) {
                /* Hold it briefly — long enough for a remove on another
                 * thread to have to wait rather than free underneath. */
                large_wal_registry_set_fd(node, -1, 0);
                large_wal_registry_release(a->reg, node);
            }
            break;
        }
        default:
            large_wal_registry_remove(a->reg, seg, NULL, NULL);
            break;
        }
        a->ops++;
    }
    return NULL;
}

static void *index_hammer(void *arg)
{
    HammerArgs *a    = arg;
    unsigned    seed = (unsigned)a->id + 100;

    while (!atomic_load(a->stop)) {
        uint64_t lsn = rand_r(&seed) % 512;

        switch (rand_r(&seed) % 3) {
        case 0: {
            LargeWalIndexEntry e;
            memset(&e, 0, sizeof(e));
            e.content_lsn   = lsn;
            e.segment_no    = lsn % 8;
            e.start_page_no = 1;
            e.page_count    = 1;
            e.total_size    = WAL_RECORD_HEADER_SIZE + SMALL_BODY;
            large_wal_index_insert(a->idx, &e);
            break;
        }
        case 1: {
            LargeWalIndexEntry out;
            large_wal_index_lookup(a->idx, lsn, &out);
            break;
        }
        default:
            large_wal_index_delete_by_segment(a->idx, lsn % 8);
            break;
        }
        a->ops++;
    }
    return NULL;
}

static void test_registry_and_index_hammer(void)
{
    printf("\n[test_registry_and_index_hammer]\n");
    cleanup_dir();
    mkdir(TEST_WAL_DIR, 0755);

    LargeWalRegistry reg;
    LargeWalIndex    idx;
    CHECK(large_wal_registry_init(&reg) == MYDB_OK, "registry init succeeds");
    CHECK(large_wal_index_open(&idx, TEST_WAL_DIR) == MYDB_OK, "index open succeeds");

    enum { N_HAMMERS = 4 };
    atomic_int stop = 0;
    HammerArgs   args[N_HAMMERS * 2];
    pthread_t    tids[N_HAMMERS * 2];

    for (int i = 0; i < N_HAMMERS; i++) {
        memset(&args[i], 0, sizeof(args[i]));
        args[i].reg = &reg; args[i].idx = &idx; args[i].stop = &stop; args[i].id = i;
        pthread_create(&tids[i], NULL, registry_hammer, &args[i]);

        int j = N_HAMMERS + i;
        memset(&args[j], 0, sizeof(args[j]));
        args[j].reg = &reg; args[j].idx = &idx; args[j].stop = &stop; args[j].id = j;
        pthread_create(&tids[j], NULL, index_hammer, &args[j]);
    }

    usleep(scale_down(300) * 1000);
    atomic_store(&stop, 1);

    uint32_t total_ops = 0;
    for (int i = 0; i < N_HAMMERS * 2; i++) {
        pthread_join(tids[i], NULL);
        total_ops += args[i].ops;
    }
    printf("  (%u concurrent registry/index operations)\n", total_ops);

    CHECK(total_ops > 0, "the hammer threads actually ran");

    /* Surviving to here at all is the assertion: an unlocked realloc or
     * a node freed under a walker would have corrupted the heap or
     * segfaulted long before this point. */
    CHECK(1, "registry and index survived concurrent mutation without corruption");

    large_wal_index_close(&idx);
    large_wal_registry_shutdown(&reg);
    cleanup_dir();
}

/* ------------------------------------------------------------------
 * Test 4 — claim_next_wait's two outcomes.
 *
 * Tested at the pool level rather than through the writer, for two
 * reasons: the writer's own timeout is 5 seconds (right for production,
 * far too slow for a test suite), and going through the writer would
 * only prove the one-line call site while burying what is actually
 * being checked.
 *
 * The writer's use of it is covered by test 2, whose 120 records roll
 * ~20 segments and therefore pass through claim_next_wait on every
 * rollover.
 * ------------------------------------------------------------------ */

static void fill_pool(LargeWalSegmentPool *pool)
{
    uint32_t slot;
    while (large_wal_segment_pool_claim_next(pool, &slot) == MYDB_OK) { /* until full */ }
}

typedef struct {
    LargeWalSegmentPool *pool;
    atomic_int           finished;
    int                  rc;
    uint32_t             slot;
} ClaimWaitArgs;

static void *claim_wait_thread(void *arg)
{
    ClaimWaitArgs *a = arg;
    a->rc = large_wal_segment_pool_claim_next_wait(a->pool, /*timeout_ms=*/5000, &a->slot);
    atomic_store(&a->finished, 1);
    return NULL;
}

static void test_claim_next_wait(void)
{
    printf("\n[test_claim_next_wait]\n");
    cleanup_dir();

    LargeWalRegistry    reg;
    LargeWalSegmentPool pool;
    CHECK(large_wal_registry_init(&reg) == MYDB_OK, "registry init succeeds");
    CHECK(large_wal_segment_pool_init(&pool, TEST_WAL_DIR, 1, &reg) == MYDB_OK,
          "pool init succeeds");

    fill_pool(&pool);
    uint32_t slot;
    CHECK(large_wal_segment_pool_claim_next(&pool, &slot) != MYDB_OK,
          "pool really is full -- plain claim_next fails");

    /* Outcome 1: nobody frees anything, so the wait expires. Short
     * timeout so this costs milliseconds, not the writer's 5s. */
    CHECK(large_wal_segment_pool_claim_next_wait(&pool, /*timeout_ms=*/150, &slot) != MYDB_OK,
          "a full pool with no archiver times out rather than hanging forever");

    /* Outcome 2: a slot is freed while a writer is parked, and the
     * broadcast in free_slot wakes it. Freeing needs LSEG_DONE, and
     * free_slot's contract needs this segment's registry node held --
     * the same two things copy_out does for real. */
    uint64_t seg_no = 0;
    uint8_t  state  = 0;
    large_wal_segment_pool_slot_info(&pool, 0, &seg_no, &state);
    CHECK(large_wal_segment_pool_mark_done(&pool, NULL, 0, /*end_lsn=*/1, /*data_pages=*/1, NULL) == MYDB_OK,
          "slot 0 marked DONE, ready to be freed");

    ClaimWaitArgs a;
    memset(&a, 0, sizeof(a));
    a.pool = &pool;
    atomic_init(&a.finished, 0);

    pthread_t tid;
    pthread_create(&tid, NULL, claim_wait_thread, &a);

    /* Let it get properly parked before freeing, so this really tests
     * the wake-up rather than a lucky first-try claim. */
    usleep(200 * 1000);
    CHECK(atomic_load(&a.finished) == 0, "the waiter is parked, not spinning through");

    LargeWalRegistryNode *node = NULL;
    int                   fd   = -1;
    int held = (large_wal_registry_acquire(&reg, seg_no, &node, &fd) == MYDB_OK);
    CHECK(large_wal_segment_pool_free_slot(&pool, 0) == MYDB_OK, "slot 0 freed");
    if (held) large_wal_registry_release(&reg, node);

    pthread_join(tid, NULL);
    CHECK(a.rc == MYDB_OK, "the parked waiter woke and claimed the freed slot");
    CHECK(a.slot == 0, "it claimed the slot that was actually freed");

    large_wal_segment_pool_shutdown(&pool);
    large_wal_registry_shutdown(&reg);
    cleanup_dir();
}

/* ------------------------------------------------------------------
 * Test 5 — the gate provider actually drives try_free.
 *
 * Single-threaded, using tick() rather than the thread, so the two
 * states (gates shut / gates open) are observed at exactly the moment
 * they are set instead of whenever a poll happened to land.
 * ------------------------------------------------------------------ */

/* Writes records until at least one slot has rolled, so there is
 * something LSEG_DONE for the archiver to copy out. Returns the
 * content_lsn of the first record, and fills out_fill with its body
 * byte, for callers that then want to read it back.
 *
 * base_lsn is a parameter rather than a constant because the index file
 * SURVIVES a manager shutdown. A second call reusing the first call's
 * LSNs inserts duplicates, which large_wal_index_insert rejects and
 * do_write then reports as a failed write -- so each run of a
 * restart test has to pick its own range. */
static uint32_t write_until_rollover(LargeWalManager *mgr, uint64_t base_lsn,
                                      uint64_t *out_first_lsn, uint8_t *out_first_fill)
{
    const uint32_t rec_len = WAL_RECORD_HEADER_SIZE + LARGE_BODY;
    uint8_t *rec = malloc(rec_len);
    uint32_t written = 0;

    /* 7 records = one full segment plus one, which forces exactly one
     * rollover and therefore exactly one LSEG_DONE slot. */
    for (uint32_t i = 0; i < 7; i++) {
        uint8_t fill = (uint8_t)(0xA0 + i);
        build_record(rec, base_lsn + i, fill, LARGE_BODY);

        LargeWalIndexEntry out[64];
        uint32_t           n_out = 0;
        if (large_wal_write(mgr, rec, rec_len, out, 64, &n_out) != MYDB_OK) break;

        if (i == 0 && n_out > 0) {
            if (out_first_lsn)  *out_first_lsn  = out[0].content_lsn;
            if (out_first_fill) *out_first_fill = fill;
        }
        written++;
    }

    free(rec);
    return written;
}

static void test_gates_drive_freeing(void)
{
    printf("\n[test_gates_drive_freeing]\n");
    cleanup_dir();

    LargeWalManager mgr;
    CHECK(large_wal_manager_init(&mgr, TEST_WAL_DIR, 1, NULL) == MYDB_OK, "manager init succeeds");

    /* Stop the thread the manager started: this test wants tick() at
     * moments it chooses, not a background loop racing its assertions. */
    CHECK(large_wal_archiver_stop(&mgr.lw_archiver) == MYDB_OK, "archiver thread stopped");

    CHECK(write_until_rollover(&mgr, /*base_lsn=*/9000, NULL, NULL) == 7,
          "7 records written, one segment rolled");

    /* No gate provider yet. copy_out should still happen -- that is
     * what unblocks writes -- but nothing should be deleted. */
    CHECK(large_wal_archiver_tick(&mgr.lw_archiver, &mgr.lw_pool,
                                   &mgr.lw_registry, &mgr.lw_idx) == MYDB_OK,
          "tick with no gate provider succeeds");
    uint32_t after_copy = count_holding_files();
    printf("  (%u holding-area files after copy-out)\n", after_copy);
    CHECK(after_copy > 0, "the segment was copied out to the holding area");

    /* Gates open: the same tick should now delete what it finds. */
    CHECK(large_wal_archiver_set_gates(&mgr.lw_archiver, gate_always_open, NULL) == MYDB_OK,
          "gate provider installed");
    CHECK(large_wal_archiver_tick(&mgr.lw_archiver, &mgr.lw_pool,
                                   &mgr.lw_registry, &mgr.lw_idx) == MYDB_OK,
          "tick with gates open succeeds");
    CHECK(count_holding_files() == 0, "gates cleared, so the holding-area file was deleted");

    large_wal_manager_shutdown(&mgr);
    cleanup_dir();
}

/* ------------------------------------------------------------------
 * Test 6 — a restart finds what the previous run archived.
 *
 * LargeWalRegistry is in-memory and starts empty, so without init's
 * holding-area scan a restart leaves archived records unresolvable even
 * though their bytes are sitting on disk, intact.
 *
 * Also covers the half-written file case, which matters more than it
 * looks: copy_out opens with O_EXCL, so a leftover partial file would
 * make copy_out fail on that segment forever.
 * ------------------------------------------------------------------ */

/* Truncates the first holding-area file found, simulating a crash
 * partway through copy_out's 2MB pwrite. Returns its segment_no, or
 * UINT64_MAX if there wasn't one. */
static uint64_t truncate_one_holding_file(void)
{
    DIR *d = opendir(TEST_WAL_DIR);
    if (!d) return UINT64_MAX;

    uint64_t found = UINT64_MAX;
    struct dirent *ent;
    while ((ent = readdir(d)) != NULL) {
        if (strncmp(ent->d_name, "large_wal_archival_", 19) != 0) continue;

        char path[512];
        snprintf(path, sizeof(path), "%s/%s", TEST_WAL_DIR, ent->d_name);
        if (truncate(path, 4096) == 0)
            found = strtoull(ent->d_name + 19, NULL, 10);
        break;
    }

    closedir(d);
    return found;
}

static void test_restart_rescans_holding_area(void)
{
    printf("\n[test_restart_rescans_holding_area]\n");
    cleanup_dir();

    uint64_t first_lsn  = 0;
    uint8_t  first_fill = 0;

    /* --- Run 1: write, roll a segment, copy it out, leave it there. --- */
    {
        LargeWalManager mgr;
        CHECK(large_wal_manager_init(&mgr, TEST_WAL_DIR, 1, NULL) == MYDB_OK,
              "run 1: manager init succeeds");
        CHECK(large_wal_archiver_stop(&mgr.lw_archiver) == MYDB_OK, "run 1: archiver stopped");

        CHECK(write_until_rollover(&mgr, /*base_lsn=*/20000, &first_lsn, &first_fill) == 7,
              "run 1: 7 records written");

        /* No gates installed, so this copies out without freeing --
         * exactly the state a restart has to cope with. */
        large_wal_archiver_tick(&mgr.lw_archiver, &mgr.lw_pool, &mgr.lw_registry, &mgr.lw_idx);
        CHECK(count_holding_files() > 0, "run 1: a segment reached the holding area");

        large_wal_manager_shutdown(&mgr);
    }

    /* --- Run 2: fresh process state. The registry is empty again; only
     *     init's scan can make that archived record readable. --- */
    {
        LargeWalManager mgr;
        CHECK(large_wal_manager_init(&mgr, TEST_WAL_DIR, 1, NULL) == MYDB_OK,
              "run 2: manager init succeeds");
        CHECK(large_wal_archiver_stop(&mgr.lw_archiver) == MYDB_OK, "run 2: archiver stopped");

        uint8_t *buf = malloc(MAX_RECORD_LEN);
        uint32_t len = 0;
        int rc = large_wal_get(&mgr, first_lsn, buf, &len);
        CHECK(rc == MYDB_OK, "run 2: a record archived by the previous run is still readable");

        int body_ok = (rc == MYDB_OK && len > WAL_RECORD_HEADER_SIZE);
        for (uint32_t i = WAL_RECORD_HEADER_SIZE; body_ok && i < len; i++)
            if (buf[i] != first_fill) body_ok = 0;
        CHECK(body_ok, "run 2: its bytes came back exactly as written");

        free(buf);
        large_wal_manager_shutdown(&mgr);
    }

    /* --- Run 3: a crash left a truncated holding-area file behind. --- */
    {
        uint64_t truncated = truncate_one_holding_file();
        CHECK(truncated != UINT64_MAX, "run 3: a holding-area file was truncated to simulate a crash");

        LargeWalManager mgr;
        CHECK(large_wal_manager_init(&mgr, TEST_WAL_DIR, 1, NULL) == MYDB_OK,
              "run 3: manager init succeeds despite the bad file");
        CHECK(large_wal_archiver_stop(&mgr.lw_archiver) == MYDB_OK, "run 3: archiver stopped");

        CHECK(count_holding_files() == 0,
              "run 3: init deleted the truncated file rather than registering it");

        /* The point of deleting it: copy_out's O_EXCL open must be able
         * to create that path again. Proven directly by copying a fresh
         * segment out under the same segment_no's file name -- if the
         * stale file were still there this would fail forever. */
        CHECK(write_until_rollover(&mgr, /*base_lsn=*/30000, NULL, NULL) == 7,
              "run 3: writes still work");
        CHECK(large_wal_archiver_tick(&mgr.lw_archiver, &mgr.lw_pool,
                                       &mgr.lw_registry, &mgr.lw_idx) == MYDB_OK,
              "run 3: tick succeeds");
        CHECK(count_holding_files() > 0, "run 3: copy_out could create a holding-area file again");

        large_wal_manager_shutdown(&mgr);
    }

    cleanup_dir();
}

/* ------------------------------------------------------------------
 * Test 7 — a crash INSIDE free_slot, i.e. partway through zeroing.
 *
 * The other crash window (dying during copy_out's own pwrite) leaves a
 * short file and is covered by test 6's run 3. This one is its mirror
 * image and needs the opposite recovery:
 *
 *   the holding-area copy is COMPLETE and fsynced,
 *   the rotation slot's content is partly zeroed,
 *   the slot header still says LSEG_DONE.
 *
 * Before the resume path existed this slot was lost forever -- copy_out
 * would hit O_EXCL on the already-present file, fail, and be retried
 * every tick for the life of the process, with one of the four rotation
 * slots permanently unusable.
 *
 * The assertion that matters most is the LAST one: the record must read
 * back with its original bytes. If the recovery wrongly re-copied from
 * the rotation slot, it would have overwritten the good file with the
 * zeroed one and the bytes would come back as zeros.
 * ------------------------------------------------------------------ */

/* Does exactly what copy_out's first half does -- write a complete,
 * fsynced holding-area copy -- and then stops, leaving the slot DONE.
 * That is precisely the on-disk state a crash inside free_slot leaves
 * behind. */
static int stage_completed_copy(LargeWalSegmentPool *pool, uint32_t slot_index,
                                 uint64_t segment_no)
{
    uint8_t *buf = malloc(LARGE_WAL_SEGMENT_FILE_SIZE);
    if (!buf) return MYDB_ERR;

    if (large_wal_segment_pool_read_segment(pool, slot_index, buf) != MYDB_OK) {
        free(buf);
        return MYDB_ERR;
    }

    LargeWalSegmentHeader hdr;
    if (large_wal_segment_header_deserialize(buf, &hdr) != MYDB_OK) {
        free(buf);
        return MYDB_ERR;
    }
    hdr.state = LSEG_ARCHIVING;
    large_wal_segment_header_serialize(&hdr, buf);

    char path[512];
    snprintf(path, sizeof(path), "%s/large_wal_archival_%llu.mydb",
             TEST_WAL_DIR, (unsigned long long)segment_no);

    int fd = open(path, O_RDWR | O_CREAT | O_EXCL, 0644);
    if (fd < 0) { free(buf); return MYDB_ERR; }

    int rc = MYDB_OK;
    if (pwrite(fd, buf, LARGE_WAL_SEGMENT_FILE_SIZE, 0) != (ssize_t)LARGE_WAL_SEGMENT_FILE_SIZE ||
        fsync(fd) < 0)
        rc = MYDB_ERR;

    close(fd);
    free(buf);
    return rc;
}

/* Zeroes the first few content pages of a slot, leaving page 0 (the
 * header) alone -- what free_slot had managed before the crash. */
static void partially_zero_slot(LargeWalSegmentPool *pool, uint32_t slot_index)
{
    uint8_t zero_page[PAGE_SIZE];
    memset(zero_page, 0, PAGE_SIZE);
    for (uint32_t p = 1; p <= 4; p++)
        large_wal_segment_pool_write_page(pool, slot_index, p, zero_page);
}

static void test_crash_mid_zeroing_is_recovered(void)
{
    printf("\n[test_crash_mid_zeroing_is_recovered]\n");
    cleanup_dir();

    LargeWalManager mgr;
    CHECK(large_wal_manager_init(&mgr, TEST_WAL_DIR, 1, NULL) == MYDB_OK, "manager init succeeds");
    CHECK(large_wal_archiver_stop(&mgr.lw_archiver) == MYDB_OK, "archiver thread stopped");

    uint64_t first_lsn  = 0;
    uint8_t  first_fill = 0;
    CHECK(write_until_rollover(&mgr, /*base_lsn=*/40000, &first_lsn, &first_fill) == 7,
          "7 records written, one segment rolled to LSEG_DONE");

    /* Find the DONE slot the rollover produced. */
    uint32_t done_slot  = LARGE_WAL_SEGMENT_POOL_SLOTS;
    uint64_t done_seg   = 0;
    for (uint32_t i = 0; i < LARGE_WAL_SEGMENT_POOL_SLOTS; i++) {
        uint64_t seg = 0; uint8_t st = 0;
        large_wal_segment_pool_slot_info(&mgr.lw_pool, i, &seg, &st);
        if (st == LSEG_DONE) { done_slot = i; done_seg = seg; break; }
    }
    CHECK(done_slot < LARGE_WAL_SEGMENT_POOL_SLOTS, "found the LSEG_DONE slot");

    /* Reproduce the crash state on disk. */
    CHECK(stage_completed_copy(&mgr.lw_pool, done_slot, done_seg) == MYDB_OK,
          "a complete holding-area copy exists, as it would after copy_out's fsync");
    partially_zero_slot(&mgr.lw_pool, done_slot);
    CHECK(count_holding_files() == 1, "exactly one holding-area file is staged");

    /* This is the call that used to fail forever on O_EXCL. */
    CHECK(large_wal_archiver_copy_out(&mgr.lw_archiver, &mgr.lw_pool,
                                       &mgr.lw_registry, done_slot) == MYDB_OK,
          "copy_out resumes rather than colliding with the existing file");

    uint8_t st = 0;
    large_wal_segment_pool_slot_info(&mgr.lw_pool, done_slot, NULL, &st);
    CHECK(st == LSEG_FREE, "the stuck slot was finally freed");

    uint32_t reclaimed = 0;
    CHECK(large_wal_segment_pool_claim_next(&mgr.lw_pool, &reclaimed) == MYDB_OK,
          "and it can be claimed again -- the slot is back in the rotation");

    /* The strongest check: if the recovery had re-copied from the
     * half-zeroed rotation slot, it would have destroyed the good copy
     * and these bytes would come back as zeros. */
    uint8_t *buf = malloc(MAX_RECORD_LEN);
    uint32_t len = 0;
    int rc = large_wal_get(&mgr, first_lsn, buf, &len);
    CHECK(rc == MYDB_OK, "the archived record is still readable");

    int body_ok = (rc == MYDB_OK && len > WAL_RECORD_HEADER_SIZE);
    for (uint32_t i = WAL_RECORD_HEADER_SIZE; body_ok && i < len; i++)
        if (buf[i] != first_fill) body_ok = 0;
    CHECK(body_ok, "its bytes are the ORIGINAL ones, not the zeroed slot's");

    free(buf);
    large_wal_manager_shutdown(&mgr);
    cleanup_dir();
}

/* ------------------------------------------------------------------
 * Test 8 — the same crash, but recovered across a RESTART.
 *
 * Test 7 recovers within one process, where the registry entry still
 * points at the rotation slot (claim_next put it there) and copy_out
 * therefore has to open the holding-area file and repoint.
 *
 * After a real crash the process is gone, so recovery runs the other
 * branch: archiver_init's scan has already registered the holding-area
 * file, and copy_out must notice that and free the slot WITHOUT
 * repointing anything. Getting that wrong would leak the scan's fd, or
 * worse, aim the registry at a slot that is about to be zeroed.
 *
 * This is also the only test where the archiver THREAD does the
 * recovery on its own, with nobody calling copy_out by hand.
 * ------------------------------------------------------------------ */

static void test_crash_mid_zeroing_recovered_after_restart(void)
{
    printf("\n[test_crash_mid_zeroing_recovered_after_restart]\n");
    cleanup_dir();

    uint64_t first_lsn  = 0;
    uint8_t  first_fill = 0;
    uint64_t done_seg   = 0;

    /* --- Run 1: reach the crash state and stop dead. --- */
    {
        LargeWalManager mgr;
        CHECK(large_wal_manager_init(&mgr, TEST_WAL_DIR, 1, NULL) == MYDB_OK,
              "run 1: manager init succeeds");
        CHECK(large_wal_archiver_stop(&mgr.lw_archiver) == MYDB_OK, "run 1: archiver stopped");
        CHECK(write_until_rollover(&mgr, /*base_lsn=*/50000, &first_lsn, &first_fill) == 7,
              "run 1: 7 records written");

        uint32_t done_slot = LARGE_WAL_SEGMENT_POOL_SLOTS;
        for (uint32_t i = 0; i < LARGE_WAL_SEGMENT_POOL_SLOTS; i++) {
            uint64_t seg = 0; uint8_t st = 0;
            large_wal_segment_pool_slot_info(&mgr.lw_pool, i, &seg, &st);
            if (st == LSEG_DONE) { done_slot = i; done_seg = seg; break; }
        }
        CHECK(done_slot < LARGE_WAL_SEGMENT_POOL_SLOTS, "run 1: found the LSEG_DONE slot");

        CHECK(stage_completed_copy(&mgr.lw_pool, done_slot, done_seg) == MYDB_OK,
              "run 1: complete holding-area copy staged");
        partially_zero_slot(&mgr.lw_pool, done_slot);

        large_wal_manager_shutdown(&mgr);
    }

    /* --- Run 2: fresh start. init's scan registers the holding-area
     *     file; the archiver thread then has to free the stranded
     *     slot on its own. --- */
    {
        LargeWalManager mgr;
        CHECK(large_wal_manager_init(&mgr, TEST_WAL_DIR, 1, NULL) == MYDB_OK,
              "run 2: manager init succeeds with a stranded LSEG_DONE slot");

        /* Installing gates would DELETE the holding-area file, which is
         * the thing we then want to read from -- so leave them off.
         * Freeing the rotation slot does not depend on the gates. */
        int freed_in_time = 0;
        for (int i = 0; i < 100; i++) {
            usleep(20 * 1000);

            int any_done = 0;
            for (uint32_t sl = 0; sl < LARGE_WAL_SEGMENT_POOL_SLOTS; sl++) {
                uint8_t st = 0;
                large_wal_segment_pool_slot_info(&mgr.lw_pool, sl, NULL, &st);
                if (st == LSEG_DONE) any_done = 1;
            }
            if (!any_done) { freed_in_time = 1; break; }
        }
        CHECK(freed_in_time,
              "run 2: the archiver thread freed the stranded slot by itself");

        uint8_t *buf = malloc(MAX_RECORD_LEN);
        uint32_t len = 0;
        int rc = large_wal_get(&mgr, first_lsn, buf, &len);
        CHECK(rc == MYDB_OK, "run 2: the archived record is still resolvable");

        int body_ok = (rc == MYDB_OK && len > WAL_RECORD_HEADER_SIZE);
        for (uint32_t i = WAL_RECORD_HEADER_SIZE; body_ok && i < len; i++)
            if (buf[i] != first_fill) body_ok = 0;
        CHECK(body_ok, "run 2: with its original bytes intact");

        free(buf);
        large_wal_manager_shutdown(&mgr);
    }

    cleanup_dir();
}

/* ------------------------------------------------------------------ */

int main(void)
{
    /* Line-buffered: under a race detector this runs for minutes, and
     * block-buffered output would hide all progress until the end. */
    setvbuf(stdout, NULL, _IOLBF, 0);

    printf("=== test_large_wal_concurrency ===\n");

    test_readers_vs_writer();
    test_readers_vs_writer_vs_archiver();
    test_registry_and_index_hammer();
    test_claim_next_wait();
    test_gates_drive_freeing();
    test_restart_rescans_holding_area();
    test_crash_mid_zeroing_is_recovered();
    test_crash_mid_zeroing_recovered_after_restart();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
