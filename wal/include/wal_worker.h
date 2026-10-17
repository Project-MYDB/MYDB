#ifndef WAL_WORKER_H
#define WAL_WORKER_H

#include <pthread.h>
#include <stdint.h>
#include "common.h"
#include "wal_mailbox.h"

/*
 * wal_worker.h — one persistent background thread, owned by WalManager
 * (wal_manager.h) and handed to the Flusher, used to overlap a segment
 * rollover's two genuinely
 * distinct-file fsyncs instead of paying for them sequentially.
 *
 * A rollover does mark_done() (old segment's header) then eventually a
 * trailing fdatasync (new segment's header+data) — two different files,
 * so the two flushes can't be collapsed into one, but they don't need
 * to be sequential either. mark_done() hands its fdatasync to this
 * worker and returns immediately; the caller (write()) continues
 * writing the new segment and fdatasyncs it on its own thread, then
 * calls wal_worker_wait() — total latency becomes max(old, new)
 * instead of old + new.
 *
 * All of the threading now lives in WalMailbox (wal_mailbox.h): the
 * thread, the queue, the depth gate, the shutdown handshake, and the
 * four easy-to-break rules that used to be commented here and in the
 * two segment pools (the WAL's and large_wal's). What is left in this
 * file is the narrow part — an fdatasync, and the ticket bookkeeping
 * that keeps caller threads' results apart.
 *
 * Started at max_depth == 1, which is the strict one-in-flight handoff
 * this worker has always had: async_fdatasync() blocks (briefly, only
 * on the rare collision) until any previous request has been fully
 * consumed via wait(), rather than assuming a single caller. One
 * WalMailboxNode in this struct is therefore enough no matter how many
 * threads call in — see wal_mailbox.h on why depth 1 makes a shared
 * node safe.
 *
 * Deliberately narrow: this is not a generic task-executor. The only
 * operation this subsystem currently needs to offload is fdatasync, so
 * that's the only thing exposed. (The layer underneath is no longer
 * narrow in that way — WalMailbox runs whatever handler it is given —
 * but nothing about that leaks through this interface.)
 *
 * ------------------------------------------------------------------
 * Why there are tickets
 *
 * The depth gate stops two callers from CLOBBERING each other's
 * request. It does not stop one from collecting the other's RESULT,
 * which is what the ticket is for. async_fdatasync() hands back a
 * number naming the request it just posted; wait() takes that number
 * and will only ever report on that one. A caller that posted nothing
 * passes 0 and gets MYDB_OK without touching the mailbox at all.
 *
 * Why it mattered here and not in a single-caller mailbox: both
 * wal_segment_pool_write() and large_wal_segment_pool_write() called
 * wait() UNCONDITIONALLY at their `out:` label, whether or not this
 * particular call rolled over and posted anything. That is deliberate
 * (it is what makes the drain rule impossible to forget on an error
 * path), but it means a thread that posted nothing still reaches wait()
 * while the other thread's fsync may be in flight. Before the ticket,
 * that thread consumed the other's result and the real submitter then
 * found an empty mailbox and read success — a failed WAL fsync reported
 * as durable.
 *
 * With large_wal moved out, the Flusher is the only caller thread today,
 * so the collision cannot happen right now. The tickets stay: they cost
 * nothing, and any second caller (a checkpointer, an archiver) brings
 * the same hazard straight back.
 * ------------------------------------------------------------------
 */

typedef struct {
    WalMailbox      mb;
    WalMailboxNode  node;   /* the one outstanding request; safe to share
                               across caller threads at max_depth 1 */
} WalWorker;

int wal_worker_start(WalWorker *w);

/* Signals stop, joins the thread. Safe to call even if start() was
 * never called, or after an earlier stop() — a no-op in both cases. */
int wal_worker_stop(WalWorker *w);

/* Hands fd off to the background thread for fdatasync() and returns
 * immediately. Blocks only if a previous request hasn't been wait()'d
 * on yet (a collision between two caller threads sharing this one
 * worker; there were two while large_wal existed, the Flusher is the
 * only one now) — otherwise returns without
 * waiting for the fdatasync itself to complete.
 *
 * out_ticket (may be NULL) receives the number naming this request.
 * Pass it to wal_worker_wait() to collect THIS fsync's result. It is
 * written only when the post actually succeeded, so a caller that
 * initialises its ticket to 0 and passes the same variable through
 * every path can never accidentally wait on a request it did not
 * make. */
int wal_worker_async_fdatasync(WalWorker *w, int fd, uint64_t *out_ticket);

/* Blocks until the request named by `ticket` completes and returns its
 * fdatasync() result.
 *
 * ticket == 0 means "I posted nothing": returns MYDB_OK immediately,
 * without even taking the lock. This is what makes it safe for write()
 * to call unconditionally rather than tracking "did I roll over this
 * call" locally — and unlike the old "is the mailbox idle?" test it
 * replaced, it asks a question the caller knows the answer to for
 * certain, so another thread's in-flight fsync can no longer be
 * mistaken for the caller's own.
 *
 * A ticket that is neither 0 nor the one in flight returns MYDB_ERR.
 * That can only mean the same ticket was waited on twice, which is a
 * caller bug; it surfaces here rather than silently reporting success. */
int wal_worker_wait(WalWorker *w, uint64_t ticket);

#endif /* WAL_WORKER_H */
