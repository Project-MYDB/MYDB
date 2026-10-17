#ifndef WAL_MAILBOX_H
#define WAL_MAILBOX_H

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include "common.h"

/*
 * wal_mailbox.h — one background thread plus a request/response queue,
 * shared by every WAL component that needs to hand work to a dedicated
 * thread and hear back about it.
 *
 * This file exists because wal/ had grown two hand-written copies of the
 * same thing. wal_worker.h said so itself: "Single-slot mailbox, same
 * mutex+condvar shape LargeWalWriter's submit() already uses." (large_wal,
 * and with it LargeWalWriter, has since been moved out of the tree;
 * wal_worker is the one user left.) Same
 * thread loop, same start/stop, same idle gate, same three-flag
 * handshake — written twice, so any fix had to be applied twice and
 * stay applied. The tricky parts (below) now live in one place.
 *
 * What it is NOT: a general task executor. It runs one handler function,
 * fixed at start(), one request at a time, in the order posted. No
 * priorities, no cancellation, no work stealing.
 *
 * ------------------------------------------------------------------
 * Who does what
 *
 *   start(fn, owner, max_depth)   brings the thread up. fn is the only
 *                                 work this mailbox will ever do; owner
 *                                 is handed back to it untouched.
 *   post(node, req, len)          copies req into node, queues it,
 *                                 returns. Blocks only if max_depth
 *                                 requests are already outstanding.
 *   wait(node, resp, len)         blocks until THAT node's request has
 *                                 run, returns its handler's result.
 *   call(req, len)                post + wait in one go, using a node on
 *                                 the caller's own stack. Copies the
 *                                 request back out, so a handler can
 *                                 answer through the same struct.
 *
 * A node is supplied by the caller and nothing is heap-allocated. For a
 * blocking caller (post and wait in one function) that means a plain
 * local, which is what call() does for you. For a caller that posts and
 * collects later, the node has to live somewhere that outlasts post() —
 * typically a field in the owner's own struct.
 *
 * TWO THREADS MAY SHARE ONE NODE ONLY AT max_depth == 1. There, the
 * depth gate guarantees the second poster cannot touch the node until
 * the first has collected, so a single node in the owner's struct is
 * enough however many threads call in. Above depth 1 that no longer
 * holds and each concurrent caller needs its own node — which is exactly
 * what call() gives it, for free, on the stack.
 *
 * ------------------------------------------------------------------
 * The request is COPIED, deliberately
 *
 * post() memcpy's request_len bytes into the node and hands the handler
 * a pointer to that copy, rather than keeping the caller's pointer. The
 * reason is a race that the borrowed-pointer version cannot express:
 * a caller that posts and collects later has nowhere to put the request
 * except its own struct, and it would have to fill that field BEFORE
 * post() takes the lock — so two caller threads racing to post would
 * each overwrite the other's request, and whichever one won the queue
 * would run with the loser's data. Copying inside the lock, after the
 * depth gate, makes that impossible. (WalWorker had exactly this shape:
 * it wrote req_fd under the lock precisely to avoid it, which a borrowed
 * pointer could not have reproduced.)
 *
 * Pointers INSIDE the copied struct stay borrowed, and that is fine as
 * long as what they point at outlives the call — which for a blocking
 * caller it does, because the caller is parked inside wait().
 *
 * The handler may write its answer back into the same struct; wait()
 * copies it out again if you give it somewhere to put it.
 *
 * ------------------------------------------------------------------
 * Four rules that are easy to get wrong, and are now only here
 *
 * 1. THE DRAIN RULE. depth is decremented by wait(), not by the handler
 *    finishing. So a request that is posted and never waited on keeps
 *    its slot forever, and at max_depth == 1 the next post() blocks for
 *    good. That is deliberate: it is what lets a caller treat "did my
 *    request succeed" as a question with an answer, instead of silently
 *    dropping results. The cost is that every return path in a function
 *    that posts has to reach its wait(). Both segment pools do this with
 *    a single `out:` label and an unconditional wait, which is why they
 *    use goto rather than return.
 *
 * 2. A QUEUED REQUEST IS STILL SERVED DURING SHUTDOWN. stop() sets the
 *    flag and wakes the thread, but the thread only leaves its loop when
 *    the queue is empty. Otherwise stopping mid-fsync would abandon a
 *    caller that is parked in wait() forever.
 *
 *    stop() also waits for every caller thread to leave the lock before
 *    destroying it, which is a separate concern from draining the queue:
 *    a poster blocked at the depth gate is not holding a request at all,
 *    but it IS sitting in pthread_cond_wait, and destroying a condvar or
 *    mutex from under a waiter is undefined behaviour. So stop() sets
 *    the flag, wakes everyone, waits for the caller count to hit zero,
 *    and only then joins and tears down. Gated posters come back
 *    MYDB_ERR rather than hanging.
 *
 * 3. BROADCAST, NEVER SIGNAL. One condvar serves several different
 *    predicates at once: the thread waiting for work, each waiter
 *    watching its own node->done, and each poster waiting for the depth
 *    gate to open. pthread_cond_signal would wake one arbitrary thread,
 *    which is very often not the one whose predicate just became true.
 *
 * 4. WAITING ON A NODE THAT WAS NEVER POSTED RETURNS MYDB_OK. A zeroed
 *    node has seq == 0 and means "this caller posted nothing", which is
 *    answered without even taking the lock. This is what makes rule 1's
 *    unconditional wait cheap, and — crucially — it asks about THIS
 *    caller's own request rather than about whether the mailbox happens
 *    to be busy. Those are different questions as soon as two threads
 *    share one mailbox, and confusing them is how a failed WAL fsync
 *    once got reported to the wrong thread.
 * ------------------------------------------------------------------
 */

/* Biggest request a mailbox will carry. 64 bytes covered both users it
 * was sized for (an int fd; LargeWalWriter's five-field submit
 * descriptor, gone with large_wal) with
 * room to spare, and keeps a node small enough to sit on a stack
 * without thought. post() rejects anything larger rather than
 * truncating. */
#define WAL_MAILBOX_MAX_PAYLOAD 64

/* Runs on the mailbox's thread, with no mailbox lock held. Reads its
 * inputs from request and may write its outputs back into the same
 * buffer. Whatever it returns is what the waiting caller receives. */
typedef int (*WalMailboxFn)(void *owner, void *request);

/* Caller-owned. Zero it (or let call() own one) before first use; after
 * that post() initialises every field. Must stay alive and untouched by
 * the caller from post() until wait() returns. */
typedef struct WalMailboxNode {
    struct WalMailboxNode *next;      /* queue linkage, mailbox-private  */
    uint64_t seq;                     /* 0 = not posted / already collected */
    int      result;                  /* the handler's return value      */
    uint8_t  done;                    /* handler has finished with this  */
    uint32_t payload_len;
    uint8_t  payload[WAL_MAILBOX_MAX_PAYLOAD];
} WalMailboxNode;

typedef struct {
    pthread_t        thread;
    pthread_mutex_t  lock;
    pthread_cond_t   cond;
    uint8_t          started;
    uint8_t          stop_requested;

    WalMailboxNode  *head;       /* posted, not yet picked up  */
    WalMailboxNode  *tail;
    uint32_t         depth;      /* posted and not yet collected -- see rule 1 */
    uint32_t         max_depth;
    uint32_t         blocked;    /* CALLER threads currently inside a
                                    pthread_cond_wait on mb->cond -- not the
                                    mailbox thread, which is joined instead.
                                    stop() waits for this to reach 0 before
                                    destroying the lock and condvar: tearing
                                    either down with a waiter still on it is
                                    undefined behaviour, and a poster parked
                                    at the depth gate is exactly that. */
    uint64_t         next_seq;   /* monotonic; 0 is never handed out */

    WalMailboxFn     fn;         /* set once by start(), never mutated */
    void            *owner;
} WalMailbox;

/* Brings the thread up. max_depth is how many requests may be
 * outstanding at once; 1 gives a strict one-in-flight handoff, where
 * post() blocks until the previous request has been collected. */
int wal_mailbox_start(WalMailbox *mb, WalMailboxFn fn, void *owner, uint32_t max_depth);

/* Signals stop, lets the thread finish whatever is queued, joins it.
 * Safe to call if start() was never called, or twice — a no-op both
 * times. */
int wal_mailbox_stop(WalMailbox *mb);

/* Copies request into node and queues it. Returns as soon as it is
 * queued, without waiting for the handler. Blocks only while max_depth
 * requests are already outstanding, and fails rather than blocking
 * forever if the mailbox is stopping. A node that has never been
 * successfully posted keeps seq == 0 (zero it before first use), so it
 * can be handed to wait() on any error path and will correctly do
 * nothing. */
int wal_mailbox_post(WalMailbox *mb, WalMailboxNode *node,
                     const void *request, size_t request_len);

/* Blocks until node's request has run and returns the handler's result.
 * With response != NULL, copies response_len bytes of the (possibly
 * handler-modified) request back out. Returns MYDB_OK immediately for a
 * node that was never posted, or whose result has already been
 * collected. */
int wal_mailbox_wait(WalMailbox *mb, WalMailboxNode *node,
                     void *response, size_t response_len);

/* post + wait, on a node this function owns. request is copied in and
 * copied back out, so a handler's answers land in the caller's own
 * struct. The whole round trip happens on one stack frame, which is why
 * pointers inside request stay valid throughout. */
int wal_mailbox_call(WalMailbox *mb, void *request, size_t request_len);

#endif /* WAL_MAILBOX_H */
