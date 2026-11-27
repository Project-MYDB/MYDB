#ifndef WAL_GROUP_COMMIT_LOCK_H
#define WAL_GROUP_COMMIT_LOCK_H

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include "common.h"

/*
 * wal_group_commit_lock.h — the lock behind group commit (V2 §12.1).
 * Threads wait for an LSN to reach their own target. One of them, the
 * leader, does the work (the write step, or the fdatasync) for all of
 * them; its release wakes every waiter whose LSN the new value reached, plus one to
 * lead next. WalManager holds two: write_lock on write_lsn and
 * flush_lock on flushed_lsn.
 *
 * With regards to MariaDB: this follows MariaDB's group_commit_lock
 * (storage/innobase/log/log0sync.h, log0sync.cc, MariaDB Corporation,
 * 2020) step for step, written anew in C11 (MariaDB's code is GPLv2 C++,
 * nothing is copied). Unlike MariaDB: no completion callbacks; the waiter
 * lives on acquire's stack; the value is a pointer to the LSN in
 * WalManager, so others read it without the lock; Linux futex only.
 */

/* One sleeping thread, on its own stack inside acquire. */
typedef struct WalGcWaiter {
    uint64_t            num;        /* the value it waits for                */
    _Atomic uint32_t    signalled;  /* futex word, 1 = woken                 */
    bool                leader;     /* set by release: take the lock next    */
    struct WalGcWaiter *next;
} WalGcWaiter;

/* The lock. value is stored with release and read with acquire, so what
 * the leader did before releasing is visible to whoever sees the value. */
typedef struct {
    pthread_mutex_t   mtx;       /* guards locked and waiters                */
    _Atomic uint64_t *value;     /* the LSN this lock moves                  */
    _Atomic uint64_t  pending;   /* what the running leader promised         */
    bool              locked;    /* a leader holds it                        */
    WalGcWaiter      *waiters;   /* sleeping threads, newest first           */
} WalGroupCommitLock;

typedef enum {
    WAL_GCL_EXPIRED  = 0,   /* *value >= num: the work is already done      */
    WAL_GCL_ACQUIRED = 1,   /* the caller leads: do the work, then release   */
    WAL_GCL_BUSY     = 2    /* try_acquire only: another leader holds it     */
} WalGclResult;

/* Sets up the lock on value, which must outlive it. Returns MYDB_OK, or
 * MYDB_ERR if the mutex cannot be created. */
int  wal_gcl_init(WalGroupCommitLock *l, _Atomic uint64_t *value);

/* Destroys the lock. No leader and no waiter may be left. */
void wal_gcl_destroy(WalGroupCommitLock *l);

/* Waits until *value >= num (EXPIRED) or the caller becomes the leader
 * (ACQUIRED). Spins briefly before sleeping when num <= pending. A
 * thread that release picked to lead next gets ACQUIRED even if num is
 * reached by then: it must release again for the waiters still listed. */
WalGclResult wal_gcl_acquire(WalGroupCommitLock *l, uint64_t num);

/* acquire without waiting: EXPIRED, ACQUIRED, or BUSY if another leader
 * holds the lock. Never sleeps and never joins the waiters. */
WalGclResult wal_gcl_try_acquire(WalGroupCommitLock *l, uint64_t num);

/* The leader is done: *value = num (num >= *value). Wakes every waiter
 * with num <= *value and, if any is left, one of them to lead next. */
void wal_gcl_release(WalGroupCommitLock *l, uint64_t num);

/* The leader promises to reach num: threads arriving with a target up to
 * num spin briefly instead of sleeping. Does nothing if num is already
 * reached. */
void wal_gcl_set_pending(WalGroupCommitLock *l, uint64_t num);

/* For the leader, still holding the lock: finds the waiters whose LSN
 * *value has reached, removes them from the lock's waiting list, and adds
 * them to the front of *list. *value must already be the new value, so a
 * waiter arriving later sees it and does not sleep. */
void wal_gcl_find_waiters_reached(WalGroupCommitLock *l, WalGcWaiter **list);

/* Wakes the first n waiters on *list, or all of them when n is 0, and
 * removes them from it; the rest stay on *list. A woken waiter's
 * acquire returns EXPIRED, or ACQUIRED if release made it the next
 * leader. */
void wal_gcl_wake_waiters(WalGcWaiter **list, uint16_t n);

#endif /* WAL_GROUP_COMMIT_LOCK_H */
