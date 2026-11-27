#include "wal_worker.h"

#include <string.h>
#include "wal_page.h"

/* Wakes the leaders sleeping in wal_worker_wait_synced, after synced_lsn
 * or io_error changed. Under mtx, so a leader cannot check and then sleep
 * past it. */
static void wake_waiting_leaders(WalWorker *w)
{
    pthread_mutex_lock(&w->mtx);
    pthread_cond_broadcast(&w->done_condvar);
    pthread_mutex_unlock(&w->mtx);
}

/* Syncs the segments in [synced_lsn, upto) one by one, publishing
 * synced_lsn after each, then tries to publish flushed_lsn. */
static void sync_upto(WalWorker *w, uint64_t upto)
{
    uint64_t synced = atomic_load_explicit(&w->synced_lsn, memory_order_relaxed);
    while (synced < upto) {
        uint64_t seg = wal_lsn_segment(synced);
        if (wal_segment_pool_fdatasync_segment(w->pool, seg) != MYDB_OK) {
            atomic_store_explicit(&w->io_error, true, memory_order_release);
            wake_waiting_leaders(w);
            return;
        }
        synced = (seg + 1) << WAL_SEG_SHIFT;
        atomic_store_explicit(&w->synced_lsn, synced, memory_order_release);
        wake_waiting_leaders(w);
    }

    /* Everything below synced is on disk. Publish it unless a leader
     * holds the lock: that leader publishes past it itself. */
    if (wal_gcl_try_acquire(w->flush_lock, synced) == WAL_GCL_ACQUIRED)
        wal_gcl_release(w->flush_lock, synced);
}

/* The thread: sleeps until a request is ahead of synced_lsn, serves it,
 * and exits once stopped with nothing left to do. */
static void *worker_main(void *arg)
{
    WalWorker *w = arg;

    pthread_mutex_lock(&w->mtx);
    for (;;) {
        uint64_t req    = atomic_load_explicit(&w->sync_request, memory_order_acquire);
        uint64_t synced = atomic_load_explicit(&w->synced_lsn, memory_order_relaxed);
        bool     work   = req > synced && !atomic_load(&w->io_error);
        if (!work) {
            if (w->stop) break;
            pthread_cond_wait(&w->work_condvar, &w->mtx);
            continue;
        }
        pthread_mutex_unlock(&w->mtx);
        sync_upto(w, req);
        pthread_mutex_lock(&w->mtx);
    }
    pthread_mutex_unlock(&w->mtx);
    return NULL;
}

int wal_worker_start(WalWorker *w, WalSegmentPool *pool, WalGroupCommitLock *flush_lock,
                     uint64_t synced_lsn)
{
    if (!w || !pool || !flush_lock) return MYDB_ERR;
    memset(w, 0, sizeof(*w));
    w->pool       = pool;
    w->flush_lock = flush_lock;
    atomic_init(&w->sync_request, synced_lsn);
    atomic_init(&w->synced_lsn, synced_lsn);
    atomic_init(&w->io_error, false);

    if (pthread_mutex_init(&w->mtx, NULL) != 0) return MYDB_ERR;
    if (pthread_cond_init(&w->work_condvar, NULL) != 0) {
        pthread_mutex_destroy(&w->mtx);
        return MYDB_ERR;
    }
    if (pthread_cond_init(&w->done_condvar, NULL) != 0) {
        pthread_cond_destroy(&w->work_condvar);
        pthread_mutex_destroy(&w->mtx);
        return MYDB_ERR;
    }
    if (pthread_create(&w->thread, NULL, worker_main, w) != 0) {
        pthread_cond_destroy(&w->done_condvar);
        pthread_cond_destroy(&w->work_condvar);
        pthread_mutex_destroy(&w->mtx);
        return MYDB_ERR;
    }
    w->started = true;
    return MYDB_OK;
}

int wal_worker_stop(WalWorker *w)
{
    if (!w || !w->started) return MYDB_OK;

    pthread_mutex_lock(&w->mtx);
    w->stop = true;
    pthread_cond_signal(&w->work_condvar);
    pthread_mutex_unlock(&w->mtx);

    pthread_join(w->thread, NULL);
    pthread_cond_destroy(&w->done_condvar);
    pthread_cond_destroy(&w->work_condvar);
    pthread_mutex_destroy(&w->mtx);
    w->started = false;
    return atomic_load(&w->io_error) ? MYDB_ERR : MYDB_OK;
}

void wal_worker_request_sync(WalWorker *w, uint64_t upto)
{
    /* Under mtx, so the thread cannot check and then sleep past it. */
    pthread_mutex_lock(&w->mtx);
    if (upto > atomic_load_explicit(&w->sync_request, memory_order_relaxed))
        atomic_store_explicit(&w->sync_request, upto, memory_order_release);
    pthread_cond_signal(&w->work_condvar);
    pthread_mutex_unlock(&w->mtx);
}

int wal_worker_wait_synced(WalWorker *w, uint64_t lsn)
{
    /* The check and the sleep are under mtx, and the worker wakes us
     * under mtx too, so its wake cannot fall between them. */
    pthread_mutex_lock(&w->mtx);
    bool reached;
    while (!(reached = atomic_load_explicit(&w->synced_lsn, memory_order_acquire) >= lsn) &&
           !atomic_load_explicit(&w->io_error, memory_order_acquire))
        pthread_cond_wait(&w->done_condvar, &w->mtx);
    pthread_mutex_unlock(&w->mtx);
    /* An error in a segment past lsn does not matter here. */
    return reached ? MYDB_OK : MYDB_ERR;
}
