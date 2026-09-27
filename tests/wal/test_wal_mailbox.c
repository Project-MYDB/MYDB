#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <time.h>

#include "common.h"
#include "wal_mailbox.h"

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

static void nap_ms(long ms)
{
    struct timespec ts;
    ts.tv_sec  = ms / 1000;
    ts.tv_nsec = (ms % 1000) * 1000000L;
    nanosleep(&ts, NULL);
}

/* ------------------------------------------------------------------
 * A handler that multiplies, reports back through the same struct, and
 * returns whatever result the request asked it to.
 * ------------------------------------------------------------------ */

typedef struct {
    int in;
    int out;
    int want_rc;
} TestReq;

#define SEEN_CAP 64

typedef struct {
    int calls;               /* bumped per invocation */
    int seen[SEEN_CAP];      /* each request's `in`, in the order handled */
    int seen_n;
    /* Only the mailbox thread touches any of this, and it runs one
     * request at a time, so no lock is needed here. Readers look only
     * after stop() has joined that thread. */
} TestOwner;

static int test_handler(void *owner, void *request)
{
    TestOwner *o = (TestOwner *)owner;
    TestReq   *r = (TestReq *)request;

    if (o->seen_n < SEEN_CAP) o->seen[o->seen_n++] = r->in;
    o->calls++;
    r->out = r->in * 10;
    return r->want_rc;
}

/* ------------------------------------------------------------------ */

static void test_start_stop(void)
{
    printf("\n[test_start_stop]\n");

    TestOwner o; memset(&o, 0, sizeof(o));
    WalMailbox mb;

    CHECK(wal_mailbox_start(&mb, test_handler, &o, 1) == MYDB_OK, "start succeeds");
    CHECK(mb.started == 1, "started flag is set");
    CHECK(wal_mailbox_stop(&mb) == MYDB_OK, "stop succeeds");
    CHECK(mb.started == 0, "started flag is cleared");
    CHECK(wal_mailbox_stop(&mb) == MYDB_OK, "a second stop() is a harmless no-op");

    WalMailbox bad;
    CHECK(wal_mailbox_start(&bad, NULL, &o, 1) == MYDB_ERR, "start rejects a NULL handler");
    CHECK(wal_mailbox_start(&bad, test_handler, &o, 0) == MYDB_ERR, "start rejects max_depth 0");
}

static void test_round_trip_copies_both_ways(void)
{
    printf("\n[test_round_trip_copies_both_ways]\n");

    TestOwner o; memset(&o, 0, sizeof(o));
    WalMailbox mb;
    wal_mailbox_start(&mb, test_handler, &o, 1);

    WalMailboxNode node;
    memset(&node, 0, sizeof(node));

    TestReq req = { .in = 7, .out = 0, .want_rc = MYDB_OK };
    CHECK(wal_mailbox_post(&mb, &node, &req, sizeof(req)) == MYDB_OK, "post accepted");
    CHECK(node.seq != 0, "a successful post stamps a non-zero seq");

    TestReq got;
    memset(&got, 0, sizeof(got));
    CHECK(wal_mailbox_wait(&mb, &node, &got, sizeof(got)) == MYDB_OK,
          "wait returns the handler's result");
    CHECK(got.out == 70, "the handler's answer came back through the request struct");
    CHECK(req.out == 0,
          "the caller's own copy was NOT written behind its back -- post copies in");
    CHECK(o.calls == 1, "the handler ran exactly once");

    wal_mailbox_stop(&mb);
}

static void test_call_answers_in_place(void)
{
    printf("\n[test_call_answers_in_place]\n");

    TestOwner o; memset(&o, 0, sizeof(o));
    WalMailbox mb;
    wal_mailbox_start(&mb, test_handler, &o, 1);

    TestReq req = { .in = 4, .out = 0, .want_rc = MYDB_OK };
    CHECK(wal_mailbox_call(&mb, &req, sizeof(req)) == MYDB_OK, "call() returns the result");
    CHECK(req.out == 40, "call() copies the handler's answer back into the caller's struct");

    TestReq fail = { .in = 1, .out = 0, .want_rc = MYDB_ERR };
    CHECK(wal_mailbox_call(&mb, &fail, sizeof(fail)) == MYDB_ERR,
          "call() reports a handler failure");
    CHECK(fail.out == 10, "a failing handler's partial output still comes back");

    wal_mailbox_stop(&mb);
}

static void test_waiting_on_nothing_is_a_noop(void)
{
    printf("\n[test_waiting_on_nothing_is_a_noop]\n");

    TestOwner o; memset(&o, 0, sizeof(o));
    WalMailbox mb;
    wal_mailbox_start(&mb, test_handler, &o, 1);

    /* Rule 4: a zeroed node means "this caller posted nothing". It must
     * not block, and must not consume anyone else's result. */
    WalMailboxNode never;
    memset(&never, 0, sizeof(never));
    CHECK(wal_mailbox_wait(&mb, &never, NULL, 0) == MYDB_OK,
          "wait on a node that was never posted returns MYDB_OK without blocking");

    WalMailboxNode node;
    memset(&node, 0, sizeof(node));
    TestReq req = { .in = 2, .out = 0, .want_rc = MYDB_OK };
    wal_mailbox_post(&mb, &node, &req, sizeof(req));
    wal_mailbox_wait(&mb, &node, NULL, 0);

    CHECK(node.seq == 0, "collecting clears the node's seq");
    CHECK(wal_mailbox_wait(&mb, &node, NULL, 0) == MYDB_OK,
          "a second wait on the same node is a no-op, not a hang");
    CHECK(o.calls == 1, "the second wait did not re-run anything");

    wal_mailbox_stop(&mb);
}

static void test_oversized_request_is_rejected(void)
{
    printf("\n[test_oversized_request_is_rejected]\n");

    TestOwner o; memset(&o, 0, sizeof(o));
    WalMailbox mb;
    wal_mailbox_start(&mb, test_handler, &o, 1);

    WalMailboxNode node;
    memset(&node, 0, sizeof(node));

    uint8_t big[WAL_MAILBOX_MAX_PAYLOAD + 1];
    memset(big, 0, sizeof(big));
    CHECK(wal_mailbox_post(&mb, &node, big, sizeof(big)) == MYDB_ERR,
          "post rejects a request larger than the payload rather than truncating");
    CHECK(node.seq == 0, "a rejected post leaves the node safe to hand to wait()");
    CHECK(wal_mailbox_wait(&mb, &node, NULL, 0) == MYDB_OK,
          "and waiting on it does nothing");

    wal_mailbox_stop(&mb);
}

/* ------------------------------------------------------------------
 * Rule 1: the depth slot comes back when wait() collects, NOT when the
 * handler finishes. At max_depth 1 a second poster is therefore gated
 * until the first caller has actually collected -- which is what makes
 * "did my request succeed?" a question with an answer.
 * ------------------------------------------------------------------ */

typedef struct {
    WalMailbox     *mb;
    WalMailboxNode  node;
    TestReq         req;
    int             post_rc;
    int             wait_rc;
    volatile int    returned_from_post;
} GateCtx;

static void *gate_poster(void *arg)
{
    GateCtx *g = (GateCtx *)arg;
    g->post_rc = wal_mailbox_post(g->mb, &g->node, &g->req, sizeof(g->req));
    g->returned_from_post = 1;
    g->wait_rc = wal_mailbox_wait(g->mb, &g->node, &g->req, sizeof(g->req));
    return NULL;
}

static void test_depth_one_gates_until_the_result_is_collected(void)
{
    printf("\n[test_depth_one_gates_until_the_result_is_collected]\n");

    TestOwner o; memset(&o, 0, sizeof(o));
    WalMailbox mb;
    wal_mailbox_start(&mb, test_handler, &o, 1);

    /* First request: posted and deliberately left uncollected. */
    WalMailboxNode first;
    memset(&first, 0, sizeof(first));
    TestReq req_a = { .in = 1, .out = 0, .want_rc = MYDB_OK };
    CHECK(wal_mailbox_post(&mb, &first, &req_a, sizeof(req_a)) == MYDB_OK, "first post accepted");

    GateCtx g;
    memset(&g, 0, sizeof(g));
    g.mb  = &mb;
    g.req = (TestReq){ .in = 9, .out = 0, .want_rc = MYDB_OK };

    pthread_t th;
    pthread_create(&th, NULL, gate_poster, &g);

    /* The handler has almost certainly finished the first request by
     * now. The gate must still be shut, because nobody has collected it.
     * This assertion cannot go wrong through slowness -- post() simply
     * cannot return until the collect below happens. */
    nap_ms(80);
    CHECK(g.returned_from_post == 0,
          "a second post stays gated while the first result is uncollected, "
          "even though its handler already ran");

    CHECK(wal_mailbox_wait(&mb, &first, NULL, 0) == MYDB_OK, "collecting the first result");

    pthread_join(th, NULL);
    CHECK(g.returned_from_post == 1, "the second post went through once the slot came back");
    CHECK(g.post_rc == MYDB_OK && g.wait_rc == MYDB_OK, "the second request completed");
    CHECK(g.req.out == 90, "and got its OWN answer, not the first request's");

    wal_mailbox_stop(&mb);
}

/* ------------------------------------------------------------------
 * Two callers in flight at once, each collecting its own result. This is
 * the property the whole utility exists for: before it, a shared
 * pending/done pair let one thread walk off with another's answer.
 * ------------------------------------------------------------------ */

typedef struct {
    WalMailbox *mb;
    int         in;
    int         want_rc;
    int         got_rc;
    int         got_out;
} PairCtx;

static void *pair_caller(void *arg)
{
    PairCtx *p = (PairCtx *)arg;
    TestReq req = { .in = p->in, .out = 0, .want_rc = p->want_rc };
    p->got_rc  = wal_mailbox_call(p->mb, &req, sizeof(req));
    p->got_out = req.out;
    return NULL;
}

static void test_two_callers_each_get_their_own_result(void)
{
    printf("\n[test_two_callers_each_get_their_own_result]\n");

    TestOwner o; memset(&o, 0, sizeof(o));
    WalMailbox mb;
    wal_mailbox_start(&mb, test_handler, &o, 2);

    /* Different inputs AND different results, so a swap is visible in
     * the value rather than only in the timing. */
    PairCtx a; memset(&a, 0, sizeof(a)); a.mb = &mb; a.in = 3; a.want_rc = MYDB_OK;
    PairCtx b; memset(&b, 0, sizeof(b)); b.mb = &mb; b.in = 5; b.want_rc = MYDB_ERR;

    pthread_t ta, tb;
    pthread_create(&ta, NULL, pair_caller, &a);
    pthread_create(&tb, NULL, pair_caller, &b);
    pthread_join(ta, NULL);
    pthread_join(tb, NULL);

    CHECK(a.got_rc == MYDB_OK && a.got_out == 30, "caller A got A's result");
    CHECK(b.got_rc == MYDB_ERR && b.got_out == 50, "caller B got B's result");
    CHECK(o.calls == 2, "the handler ran once per request");

    wal_mailbox_stop(&mb);
}

/* ------------------------------------------------------------------
 * Rule 2: stop() drains the queue before the thread leaves. Otherwise
 * stopping mid-fsync abandons a caller parked in wait() forever.
 * ------------------------------------------------------------------ */

static void test_stop_serves_what_is_already_queued(void)
{
    printf("\n[test_stop_serves_what_is_already_queued]\n");

    TestOwner o; memset(&o, 0, sizeof(o));
    WalMailbox mb;
    wal_mailbox_start(&mb, test_handler, &o, 1);

    WalMailboxNode node;
    memset(&node, 0, sizeof(node));
    TestReq req = { .in = 6, .out = 0, .want_rc = MYDB_OK };
    CHECK(wal_mailbox_post(&mb, &node, &req, sizeof(req)) == MYDB_OK, "post accepted");

    CHECK(wal_mailbox_stop(&mb) == MYDB_OK, "stop() returns");
    CHECK(node.done == 1, "the already-queued request was run before the thread left");
    CHECK(node.result == MYDB_OK, "and its result was published");
    CHECK(o.calls == 1, "the handler ran for it");
}

static void test_post_after_stop_fails_rather_than_hanging(void)
{
    printf("\n[test_post_after_stop_fails_rather_than_hanging]\n");

    TestOwner o; memset(&o, 0, sizeof(o));
    WalMailbox mb;
    wal_mailbox_start(&mb, test_handler, &o, 1);
    wal_mailbox_stop(&mb);

    WalMailboxNode node;
    memset(&node, 0, sizeof(node));
    TestReq req = { .in = 1, .out = 0, .want_rc = MYDB_OK };
    CHECK(wal_mailbox_post(&mb, &node, &req, sizeof(req)) == MYDB_ERR,
          "post on a stopped mailbox fails instead of blocking forever");
    CHECK(node.seq == 0, "and leaves the node harmless");
}

/* ==================================================================
 * Queue behaviour at max_depth > 1.
 *
 * The writer and the worker both run at depth 1 today, so everything
 * below is the capability being proven rather than a path in use. It is
 * what group commit will stand on: several requests outstanding, the
 * thread working through them in order, each caller collecting only its
 * own answer.
 * ================================================================== */

static void test_queue_is_fifo(void)
{
    printf("\n[test_queue_is_fifo]\n");

    TestOwner o; memset(&o, 0, sizeof(o));
    WalMailbox mb;
    wal_mailbox_start(&mb, test_handler, &o, 5);

    /* One poster, so post order is not in doubt -- this isolates the
     * queue's own linkage from any scheduling question. */
    WalMailboxNode node[5];
    TestReq         req[5];
    int post_ok = 1, seq_rises = 1;
    uint64_t prev = 0;

    for (int i = 0; i < 5; i++) {
        memset(&node[i], 0, sizeof(node[i]));
        req[i] = (TestReq){ .in = i + 1, .out = 0, .want_rc = MYDB_OK };
        if (wal_mailbox_post(&mb, &node[i], &req[i], sizeof(req[i])) != MYDB_OK) post_ok = 0;
        if (node[i].seq <= prev) seq_rises = 0;
        prev = node[i].seq;
    }
    CHECK(post_ok, "five requests posted without blocking at max_depth 5");
    CHECK(seq_rises, "each post got a strictly higher seq");

    int collect_ok = 1, answers_ok = 1;
    for (int i = 0; i < 5; i++) {
        TestReq got;
        memset(&got, 0, sizeof(got));
        if (wal_mailbox_wait(&mb, &node[i], &got, sizeof(got)) != MYDB_OK) collect_ok = 0;
        if (got.out != (i + 1) * 10) answers_ok = 0;
    }
    CHECK(collect_ok, "all five collected");
    CHECK(answers_ok, "each node came back with its own answer");

    int order_ok = (o.seen_n == 5);
    for (int i = 0; i < 5 && order_ok; i++)
        if (o.seen[i] != i + 1) order_ok = 0;
    CHECK(order_ok, "the handler saw them in post order -- the queue is FIFO");

    CHECK(mb.depth == 0, "every depth slot came back");

    wal_mailbox_stop(&mb);
}

static void test_collection_order_is_independent_of_run_order(void)
{
    printf("\n[test_collection_order_is_independent_of_run_order]\n");

    TestOwner o; memset(&o, 0, sizeof(o));
    WalMailbox mb;
    wal_mailbox_start(&mb, test_handler, &o, 3);

    WalMailboxNode a, b, c;
    memset(&a, 0, sizeof(a)); memset(&b, 0, sizeof(b)); memset(&c, 0, sizeof(c));
    TestReq ra = { .in = 1, .out = 0, .want_rc = MYDB_OK };
    TestReq rb = { .in = 2, .out = 0, .want_rc = MYDB_ERR };
    TestReq rc_ = { .in = 3, .out = 0, .want_rc = MYDB_OK };

    wal_mailbox_post(&mb, &a, &ra, sizeof(ra));
    wal_mailbox_post(&mb, &b, &rb, sizeof(rb));
    wal_mailbox_post(&mb, &c, &rc_, sizeof(rc_));

    /* Collected back-to-front. The handler still ran them 1,2,3. */
    TestReq got;
    memset(&got, 0, sizeof(got));
    CHECK(wal_mailbox_wait(&mb, &c, &got, sizeof(got)) == MYDB_OK && got.out == 30,
          "the last-posted request can be collected first");
    memset(&got, 0, sizeof(got));
    CHECK(wal_mailbox_wait(&mb, &a, &got, sizeof(got)) == MYDB_OK && got.out == 10,
          "then the first, still carrying its own answer");
    memset(&got, 0, sizeof(got));
    CHECK(wal_mailbox_wait(&mb, &b, &got, sizeof(got)) == MYDB_ERR && got.out == 20,
          "then the middle one, with its own distinct result");

    CHECK(mb.depth == 0, "depth is back to zero after out-of-order collection");

    wal_mailbox_stop(&mb);
}

/* ------------------------------------------------------------------
 * More callers than depth: the gate is under real contention and every
 * caller must still come back with its own answer.
 * ------------------------------------------------------------------ */

#define STRESS_THREADS 8
#define STRESS_ROUNDS  25

typedef struct {
    WalMailbox *mb;
    int         id;
    int         mismatches;
    int         failures;
} StressCtx;

static void *stress_caller(void *arg)
{
    StressCtx *s = (StressCtx *)arg;

    for (int r = 0; r < STRESS_ROUNDS; r++) {
        /* A value unique to this thread and round, so a swapped answer
         * cannot coincidentally look right. */
        int in = s->id * 1000 + r;
        TestReq req = { .in = in, .out = 0, .want_rc = MYDB_OK };

        if (wal_mailbox_call(s->mb, &req, sizeof(req)) != MYDB_OK) s->failures++;
        if (req.out != in * 10) s->mismatches++;
    }
    return NULL;
}

static void test_more_callers_than_depth(void)
{
    printf("\n[test_more_callers_than_depth]\n");

    TestOwner o; memset(&o, 0, sizeof(o));
    WalMailbox mb;
    wal_mailbox_start(&mb, test_handler, &o, 3);   /* 8 threads over depth 3 */

    StressCtx ctx[STRESS_THREADS];
    pthread_t th[STRESS_THREADS];
    for (int i = 0; i < STRESS_THREADS; i++) {
        memset(&ctx[i], 0, sizeof(ctx[i]));
        ctx[i].mb = &mb;
        ctx[i].id = i + 1;
        pthread_create(&th[i], NULL, stress_caller, &ctx[i]);
    }

    int mismatches = 0, failures = 0;
    for (int i = 0; i < STRESS_THREADS; i++) {
        pthread_join(th[i], NULL);
        mismatches += ctx[i].mismatches;
        failures   += ctx[i].failures;
    }

    CHECK(failures == 0, "every call() succeeded with 8 threads over a depth-3 queue");
    CHECK(mismatches == 0, "every caller got the answer to its OWN request");
    CHECK(o.calls == STRESS_THREADS * STRESS_ROUNDS, "the handler ran once per request");
    CHECK(mb.depth == 0, "no depth slot leaked across 200 requests");

    wal_mailbox_stop(&mb);
}

static void test_stop_drains_a_whole_queue(void)
{
    printf("\n[test_stop_drains_a_whole_queue]\n");

    TestOwner o; memset(&o, 0, sizeof(o));
    WalMailbox mb;
    wal_mailbox_start(&mb, test_handler, &o, 4);

    WalMailboxNode node[4];
    TestReq         req[4];
    for (int i = 0; i < 4; i++) {
        memset(&node[i], 0, sizeof(node[i]));
        req[i] = (TestReq){ .in = i + 1, .out = 0, .want_rc = MYDB_OK };
        wal_mailbox_post(&mb, &node[i], &req[i], sizeof(req[i]));
    }

    /* Nobody collects. stop() must still run all four before leaving. */
    CHECK(wal_mailbox_stop(&mb) == MYDB_OK, "stop() returns");

    int all_done = 1;
    for (int i = 0; i < 4; i++) if (!node[i].done) all_done = 0;
    CHECK(all_done, "every queued request was run before the thread left");
    CHECK(o.calls == 4, "the handler ran for all four");
}

/* ------------------------------------------------------------------
 * The shutdown race. A poster blocked at the depth gate is sitting in
 * pthread_cond_wait while holding no request at all. If stop() destroyed
 * the condvar without waiting for it, that is undefined behaviour --
 * and the poster would be left in a torn-down mailbox.
 * ------------------------------------------------------------------ */

typedef struct {
    WalMailbox     *mb;
    WalMailboxNode  node;
    TestReq         req;
    int             post_rc;
    volatile int    returned;
} StopGateCtx;

static void *stop_gate_poster(void *arg)
{
    StopGateCtx *g = (StopGateCtx *)arg;
    g->post_rc = wal_mailbox_post(g->mb, &g->node, &g->req, sizeof(g->req));
    g->returned = 1;
    return NULL;
}

static void test_stop_releases_a_poster_stuck_at_the_gate(void)
{
    printf("\n[test_stop_releases_a_poster_stuck_at_the_gate]\n");

    TestOwner o; memset(&o, 0, sizeof(o));
    WalMailbox mb;
    wal_mailbox_start(&mb, test_handler, &o, 1);

    /* Fill the one slot and never collect it, so the gate is shut. */
    WalMailboxNode held;
    memset(&held, 0, sizeof(held));
    TestReq held_req = { .in = 1, .out = 0, .want_rc = MYDB_OK };
    CHECK(wal_mailbox_post(&mb, &held, &held_req, sizeof(held_req)) == MYDB_OK,
          "the single depth slot is taken and left uncollected");

    StopGateCtx g;
    memset(&g, 0, sizeof(g));
    g.mb  = &mb;
    g.req = (TestReq){ .in = 2, .out = 0, .want_rc = MYDB_OK };

    pthread_t th;
    pthread_create(&th, NULL, stop_gate_poster, &g);

    nap_ms(80);
    CHECK(g.returned == 0, "the second poster is parked at the gate");

    /* stop() has to notice that poster and wait for it to leave before
     * destroying the lock and condvar it is sitting on. */
    CHECK(wal_mailbox_stop(&mb) == MYDB_OK, "stop() returns rather than hanging");

    pthread_join(th, NULL);
    CHECK(g.returned == 1, "the gated poster was released");
    CHECK(g.post_rc == MYDB_ERR, "and told its post failed rather than silently dropped");
    CHECK(g.node.seq == 0, "its node is left harmless");
    CHECK(held.done == 1, "and the request already queued was still run");
}

int main(void)
{
    printf("=== test_wal_mailbox ===\n");

    test_start_stop();
    test_round_trip_copies_both_ways();
    test_call_answers_in_place();
    test_waiting_on_nothing_is_a_noop();
    test_oversized_request_is_rejected();
    test_depth_one_gates_until_the_result_is_collected();
    test_two_callers_each_get_their_own_result();
    test_stop_serves_what_is_already_queued();
    test_post_after_stop_fails_rather_than_hanging();
    test_queue_is_fifo();
    test_collection_order_is_independent_of_run_order();
    test_more_callers_than_depth();
    test_stop_drains_a_whole_queue();
    test_stop_releases_a_poster_stuck_at_the_gate();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
