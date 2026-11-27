#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>

#include "common.h"
#include "wal_group_commit_lock.h"

/*
 * test_wal_group_commit_lock — the group commit lock (MYDB_WAL_DESIGN_V2.md
 * §12.1, after MariaDB's group_commit_lock): EXPIRED and ACQUIRED, a
 * release waking only the waiters it covers, the next leader picked and
 * woken, and many threads under load (run it under TSan too).
 */

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

static void sleep_ms(long ms)
{
    nanosleep(&(struct timespec){ .tv_sec = ms / 1000, .tv_nsec = (ms % 1000) * 1000000L }, NULL);
}

/* One thread calling acquire(num); done flips to 1 when it returns. */
typedef struct {
    WalGroupCommitLock *l;
    uint64_t            num;
    _Atomic int         done;
    _Atomic int         result;
    pthread_t           tid;
} Acquirer;

static void *acquirer_main(void *arg)
{
    Acquirer *a = arg;
    atomic_store(&a->result, (int)wal_gcl_acquire(a->l, a->num));
    atomic_store(&a->done, 1);
    return NULL;
}

static void start_acquirer(Acquirer *a, WalGroupCommitLock *l, uint64_t num)
{
    a->l   = l;
    a->num = num;
    atomic_init(&a->done, 0);
    atomic_init(&a->result, -1);
    pthread_create(&a->tid, NULL, acquirer_main, a);
}

/* Waits up to ms for the acquirer to return; 1 if it did. */
static int returns_within(Acquirer *a, long ms)
{
    for (long t = 0; t < ms; t++) {
        if (atomic_load(&a->done)) return 1;
        sleep_ms(1);
    }
    return atomic_load(&a->done);
}

/* EXPIRED when covered, ACQUIRED when free, release publishes the value. */
static void test_single_thread(void)
{
    printf("\n[test_single_thread]\n");
    _Atomic uint64_t lsn = 0;
    WalGroupCommitLock l;
    CHECK(wal_gcl_init(&l, &lsn) == MYDB_OK, "init");

    CHECK(wal_gcl_acquire(&l, 0) == WAL_GCL_EXPIRED, "num 0 is covered by value 0");
    CHECK(wal_gcl_acquire(&l, 10) == WAL_GCL_ACQUIRED, "num 10, lock free: ACQUIRED");
    wal_gcl_release(&l, 10);
    CHECK(atomic_load(&lsn) == 10, "release stores the value");
    CHECK(wal_gcl_acquire(&l, 5)  == WAL_GCL_EXPIRED, "num 5 covered");
    CHECK(wal_gcl_acquire(&l, 10) == WAL_GCL_EXPIRED, "num 10 covered (equal)");
    CHECK(wal_gcl_acquire(&l, 11) == WAL_GCL_ACQUIRED, "num 11 not covered: ACQUIRED");
    wal_gcl_set_pending(&l, 40);
    wal_gcl_release(&l, 40);
    CHECK(atomic_load(&lsn) == 40, "release past num: value 40");
    CHECK(wal_gcl_acquire(&l, 40) == WAL_GCL_EXPIRED, "num 40 covered");
    CHECK(wal_gcl_init(NULL, &lsn) == MYDB_ERR && wal_gcl_init(&l, NULL) == MYDB_ERR,
          "init rejects NULL");

    wal_gcl_destroy(&l);
}

/* A broken lock can leave a thread asleep forever: report it as a FAIL
 * and stop, instead of hanging in pthread_join. */
static void give_up(const char *why)
{
    printf("  FAIL: %s, a thread never returned; stopping\n", why);
    printf("\n%d/%d tests passed\n", tests_passed, tests_run + 1);
    fflush(stdout);
    _exit(1);
}

/* Waiters sleep while the lock is held, and return EXPIRED once a release
 * covers them, one past its target and one exactly at it. */
static void test_waiter_covered(void)
{
    printf("\n[test_waiter_covered]\n");
    _Atomic uint64_t lsn = 0;
    WalGroupCommitLock l;
    wal_gcl_init(&l, &lsn);

    CHECK(wal_gcl_acquire(&l, 10) == WAL_GCL_ACQUIRED, "main is the leader");
    Acquirer a, b;
    start_acquirer(&a, &l, 15);
    start_acquirer(&b, &l, 20);
    CHECK(!returns_within(&a, 50) && !atomic_load(&b.done),
          "waiters for 15 and 20 sleep while the lock is held");

    wal_gcl_release(&l, 20);
    if (!returns_within(&a, 2000) || !returns_within(&b, 2000)) give_up("release(20)");
    CHECK(atomic_load(&a.result) == WAL_GCL_EXPIRED, "waiter for 15 covered: EXPIRED");
    CHECK(atomic_load(&b.result) == WAL_GCL_EXPIRED, "waiter for 20 covered (equal): EXPIRED");
    pthread_join(a.tid, NULL);
    pthread_join(b.tid, NULL);

    CHECK(!l.locked && !l.waiters, "lock free, no waiter left");
    wal_gcl_destroy(&l);
}

/* release covers some waiters, makes one of the rest the next leader, and
 * leaves the others asleep; the next leader's release covers them. */
static void test_next_leader(void)
{
    printf("\n[test_next_leader]\n");
    _Atomic uint64_t lsn = 0;
    WalGroupCommitLock l;
    wal_gcl_init(&l, &lsn);

    CHECK(wal_gcl_acquire(&l, 1) == WAL_GCL_ACQUIRED, "main is the leader");
    Acquirer a, b, c;
    start_acquirer(&a, &l, 10);
    start_acquirer(&b, &l, 100);
    start_acquirer(&c, &l, 200);
    CHECK(!returns_within(&a, 50) && !atomic_load(&b.done) && !atomic_load(&c.done),
          "all three sleep");

    wal_gcl_release(&l, 50);
    CHECK(returns_within(&a, 2000) && atomic_load(&a.result) == WAL_GCL_EXPIRED,
          "the waiter for 10 is covered by 50: EXPIRED");

    /* Exactly one of b, c becomes the leader; the other keeps sleeping. */
    Acquirer *lead = NULL, *other = NULL;
    for (long t = 0; t < 2000 && !lead; t++) {
        if (atomic_load(&b.done)) { lead = &b; other = &c; }
        else if (atomic_load(&c.done)) { lead = &c; other = &b; }
        else sleep_ms(1);
    }
    if (!lead) give_up("no next leader after release(50)");
    CHECK(atomic_load(&lead->result) == WAL_GCL_ACQUIRED,
          "one uncovered waiter is woken as the next leader: ACQUIRED");
    CHECK(!returns_within(other, 50), "the other uncovered waiter still sleeps");
    CHECK(atomic_load(&lsn) == 50, "value is 50");

    wal_gcl_release(&l, 300);
    if (!returns_within(other, 2000)) give_up("release(300)");
    CHECK(atomic_load(&other->result) == WAL_GCL_EXPIRED,
          "the next leader's release(300) covers the last waiter");
    pthread_join(a.tid, NULL);
    pthread_join(b.tid, NULL);
    pthread_join(c.tid, NULL);

    CHECK(!l.locked && !l.waiters, "lock free, no waiter left");
    wal_gcl_destroy(&l);
}

/* Many threads, each asking for ever higher values. Checks that only one
 * leader ever holds the lock, that every call returns with the value at
 * or past its target, and that leaders cover others (fewer leaders than
 * calls). A hang shows up as the ctest TIMEOUT. */
#define STRESS_THREADS 8
#define STRESS_CALLS   20000

typedef struct {
    WalGroupCommitLock *l;
    _Atomic uint64_t   *lsn;
    _Atomic uint64_t   *next_target;
    _Atomic int        *holders;
    _Atomic int        *bad_holders;
    _Atomic int        *bad_value;
    _Atomic long       *leads;
} Stress;

static void *stress_main(void *arg)
{
    Stress *s = arg;
    for (int i = 0; i < STRESS_CALLS; i++) {
        uint64_t target = atomic_fetch_add(s->next_target, 1) + 1;
        if (wal_gcl_acquire(s->l, target) == WAL_GCL_ACQUIRED) {
            if (atomic_fetch_add(s->holders, 1) != 0) atomic_fetch_add(s->bad_holders, 1);
            atomic_fetch_add(s->leads, 1);

            /* The "work": reach everything asked for so far, as a write
             * step writes everything copied so far. */
            uint64_t reach = atomic_load(s->next_target);
            uint64_t cur   = atomic_load(s->lsn);
            if (reach < target) reach = target;
            if (reach < cur)    reach = cur;
            wal_gcl_set_pending(s->l, reach);
            if ((i & 63) == 0) sched_yield();   /* let others queue up */

            atomic_fetch_sub(s->holders, 1);
            wal_gcl_release(s->l, reach);
        }
        if (atomic_load(s->lsn) < target) atomic_fetch_add(s->bad_value, 1);
    }
    return NULL;
}

static void test_stress(void)
{
    printf("\n[test_stress]\n");
    _Atomic uint64_t lsn = 0, next_target = 0;
    _Atomic int holders = 0, bad_holders = 0, bad_value = 0;
    _Atomic long leads = 0;
    WalGroupCommitLock l;
    wal_gcl_init(&l, &lsn);

    Stress s = { &l, &lsn, &next_target, &holders, &bad_holders, &bad_value, &leads };
    pthread_t t[STRESS_THREADS];
    for (int i = 0; i < STRESS_THREADS; i++) pthread_create(&t[i], NULL, stress_main, &s);
    for (int i = 0; i < STRESS_THREADS; i++) pthread_join(t[i], NULL);

    long calls = (long)STRESS_THREADS * STRESS_CALLS;
    printf("  %ld calls, %ld leaders\n", calls, atomic_load(&leads));
    CHECK(atomic_load(&bad_holders) == 0, "never two leaders at once");
    CHECK(atomic_load(&bad_value) == 0, "every call returns with value >= its target");
    CHECK(atomic_load(&lsn) == (uint64_t)calls, "final value is the last target");
    CHECK(atomic_load(&leads) < calls, "leaders covered other threads (fewer leaders than calls)");
    CHECK(!l.locked && !l.waiters, "lock free, no waiter left");
    wal_gcl_destroy(&l);
}

int main(void)
{
    setvbuf(stdout, NULL, _IOLBF, 0);   /* FAIL lines survive an assert abort */
    printf("=== test_wal_group_commit_lock ===\n");
    test_single_thread();
    test_waiter_covered();
    test_next_leader();
    test_stress();
    printf("\n%d/%d tests passed\n", tests_passed, tests_run);
    return tests_passed == tests_run ? 0 : 1;
}
