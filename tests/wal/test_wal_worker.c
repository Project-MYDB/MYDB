#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>

#include "common.h"
#include "wal_worker.h"

#define TEST_FILE "/tmp/mydb_test_wal_worker.tmp"

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

static void test_start_stop(void)
{
    printf("\n[test_start_stop]\n");

    WalWorker w;
    CHECK(wal_worker_start(&w) == MYDB_OK, "start succeeds");
    CHECK(wal_worker_stop(&w) == MYDB_OK, "stop succeeds");
    CHECK(wal_worker_stop(&w) == MYDB_OK, "calling stop() a second time is a harmless no-op");
}

static void test_wait_with_ticket_zero_is_a_noop(void)
{
    printf("\n[test_wait_with_ticket_zero_is_a_noop]\n");

    WalWorker w;
    wal_worker_start(&w);

    CHECK(wal_worker_wait(&w, 0) == MYDB_OK,
          "wait() with ticket 0 is a cheap no-op -- this caller posted nothing");

    wal_worker_stop(&w);
}

static void test_async_fdatasync_round_trip(void)
{
    printf("\n[test_async_fdatasync_round_trip]\n");

    int fd = open(TEST_FILE, O_CREAT | O_RDWR, 0644);
    CHECK(fd >= 0, "test file opened");

    uint8_t buf[16] = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16};
    CHECK(write(fd, buf, sizeof(buf)) == (ssize_t)sizeof(buf), "wrote some bytes to sync");

    WalWorker w;
    wal_worker_start(&w);

    uint64_t ticket = 0;
    CHECK(wal_worker_async_fdatasync(&w, fd, &ticket) == MYDB_OK, "async_fdatasync accepted");
    CHECK(ticket != 0, "a successful post hands back a non-zero ticket");
    CHECK(wal_worker_wait(&w, ticket) == MYDB_OK, "wait() returns the fdatasync's result");

    wal_worker_stop(&w);
    close(fd);
    unlink(TEST_FILE);
}

static void test_back_to_back_submissions(void)
{
    printf("\n[test_back_to_back_submissions]\n");

    int fd = open(TEST_FILE, O_CREAT | O_RDWR, 0644);

    WalWorker w;
    wal_worker_start(&w);

    int all_ok = 1, tickets_rise = 1;
    uint64_t prev = 0;
    for (int i = 0; i < 20; i++) {
        uint64_t t = 0;
        if (wal_worker_async_fdatasync(&w, fd, &t) != MYDB_OK) all_ok = 0;
        if (t <= prev) tickets_rise = 0;
        prev = t;
        if (wal_worker_wait(&w, t) != MYDB_OK) all_ok = 0;
    }
    CHECK(all_ok, "20 sequential submit+wait round trips from the same thread all succeed");
    CHECK(tickets_rise, "each post hands back a strictly higher ticket than the last");

    wal_worker_stop(&w);
    close(fd);
    unlink(TEST_FILE);
}

/* ------------------------------------------------------------------
 * One worker, two caller threads -- the shape production had while
 * large_wal existed (the Flusher thread and the LargeWalWriter thread
 * shared one WalWorker), and the one any second caller brings back.
 * BOTH call wait() unconditionally at the end of
 * every write(), whether or not that particular call rolled over and
 * posted anything.
 *
 * The fsync here is deliberately made to fail (fd -1 -> EBADF) so the
 * two outcomes are told apart by their VALUE, not just by timing:
 * the thread that posted must get the error, the thread that posted
 * nothing must get MYDB_OK. Before the ticket, they came out swapped.
 * ------------------------------------------------------------------ */

typedef struct {
    WalWorker      *w;
    uint64_t        ticket;
    int             post_rc;
    int             wait_rc;
    pthread_mutex_t m;
    pthread_cond_t  c;
    int             posted;        /* poster has handed its fsync over  */
    int             bystander_done;/* bystander has finished its wait() */
} ShareCtx;

static void *poster_main(void *arg)
{
    ShareCtx *s = (ShareCtx *)arg;

    s->post_rc = wal_worker_async_fdatasync(s->w, -1, &s->ticket);

    pthread_mutex_lock(&s->m);
    s->posted = 1;
    pthread_cond_broadcast(&s->c);
    while (!s->bystander_done)
        pthread_cond_wait(&s->c, &s->m);
    pthread_mutex_unlock(&s->m);

    /* Collected only after the bystander has had its turn, so if the
     * bystander consumed this result the failure shows up right here. */
    s->wait_rc = wal_worker_wait(s->w, s->ticket);
    return NULL;
}

static void test_result_is_not_stolen_by_the_other_caller(void)
{
    printf("\n[test_result_is_not_stolen_by_the_other_caller]\n");

    WalWorker w;
    wal_worker_start(&w);

    ShareCtx s;
    memset(&s, 0, sizeof(s));
    s.w = &w;
    pthread_mutex_init(&s.m, NULL);
    pthread_cond_init(&s.c, NULL);

    pthread_t poster;
    pthread_create(&poster, NULL, poster_main, &s);

    pthread_mutex_lock(&s.m);
    while (!s.posted)
        pthread_cond_wait(&s.c, &s.m);
    pthread_mutex_unlock(&s.m);

    /* The bystander: it never posted, so it passes ticket 0. The other
     * thread's fsync may be in flight or already finished -- either way
     * this call must not touch it. */
    int bystander_rc = wal_worker_wait(&w, 0);

    pthread_mutex_lock(&s.m);
    s.bystander_done = 1;
    pthread_cond_broadcast(&s.c);
    pthread_mutex_unlock(&s.m);

    pthread_join(poster, NULL);

    CHECK(s.post_rc == MYDB_OK, "the posting thread's async_fdatasync was accepted");
    CHECK(bystander_rc == MYDB_OK,
          "a thread that posted nothing collects nothing, even with another fsync in flight");
    CHECK(s.wait_rc == MYDB_ERR,
          "the posting thread gets its OWN fsync's failure, not an empty mailbox");

    pthread_cond_destroy(&s.c);
    pthread_mutex_destroy(&s.m);
    wal_worker_stop(&w);
}

static void test_double_wait_on_a_spent_ticket_is_rejected(void)
{
    printf("\n[test_double_wait_on_a_spent_ticket_is_rejected]\n");

    int fd = open(TEST_FILE, O_CREAT | O_RDWR, 0644);

    WalWorker w;
    wal_worker_start(&w);

    uint64_t ticket = 0;
    wal_worker_async_fdatasync(&w, fd, &ticket);

    CHECK(wal_worker_wait(&w, ticket) == MYDB_OK, "the first wait collects the result");
    CHECK(wal_worker_wait(&w, ticket) == MYDB_ERR,
          "waiting on the same ticket twice is rejected rather than silently succeeding");

    wal_worker_stop(&w);
    close(fd);
    unlink(TEST_FILE);
}

int main(void)
{
    printf("=== test_wal_worker ===\n");

    test_start_stop();
    test_wait_with_ticket_zero_is_a_noop();
    test_async_fdatasync_round_trip();
    test_back_to_back_submissions();
    test_result_is_not_stolen_by_the_other_caller();
    test_double_wait_on_a_spent_ticket_is_rejected();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
