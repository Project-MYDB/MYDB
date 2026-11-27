#ifndef WAL_WORKER_H
#define WAL_WORKER_H

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include "common.h"
#include "wal_group_commit_lock.h"
#include "wal_segment_pool.h"

/*
 * wal_worker.h — the WAL's background thread. At a rollover the write
 * step asks it to fdatasync the finished segment and goes on without
 * waiting. The worker syncs every requested segment in order, publishes
 * synced_lsn, then tries flush_lock once: if it is free it publishes
 * flushed_lsn up to there; if a commit leader holds it, that leader
 * publishes instead. It never waits for flush_lock, so a leader that
 * waits for synced_lsn while holding flush_lock cannot deadlock with it.
 * It is meant to grow into the background WAL writer.
 */

/* The worker. sync_request and synced_lsn are segment boundaries. */
typedef struct {
    pthread_t            thread;
    pthread_mutex_t      mtx;           /* guards stop, with both condvars */
    pthread_cond_t       work_condvar;  /* wakes the thread: work to do    */
    pthread_cond_t       done_condvar;  /* wakes leaders: synced_lsn moved */
    bool                 started;
    bool                 stop;
    _Atomic uint64_t     sync_request;  /* sync every segment below this   */
    _Atomic uint64_t     synced_lsn;    /* every segment below this synced */
    _Atomic bool         io_error;      /* an fdatasync failed; it stops   */
    WalSegmentPool      *pool;
    WalGroupCommitLock  *flush_lock;    /* its value is flushed_lsn        */
} WalWorker;

/* Starts the thread. synced_lsn: the segment boundary below which
 * everything is already synced. Returns MYDB_OK or MYDB_ERR. */
int wal_worker_start(WalWorker *w, WalSegmentPool *pool, WalGroupCommitLock *flush_lock,
                     uint64_t synced_lsn);

/* Syncs what was requested, then stops and joins the thread. A no-op if
 * not started. */
int wal_worker_stop(WalWorker *w);

/* Asks for every segment below upto (a segment boundary) to be synced,
 * and wakes the thread. Does not wait. */
void wal_worker_request_sync(WalWorker *w, uint64_t upto);

/* Sleeps until every segment below lsn (a segment boundary) is synced.
 * Returns MYDB_OK, or MYDB_ERR if an fdatasync failed. */
int  wal_worker_wait_synced(WalWorker *w, uint64_t lsn);

#endif /* WAL_WORKER_H */
