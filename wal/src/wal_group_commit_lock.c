#include "wal_group_commit_lock.h"

#include <assert.h>
#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>
#include "cpu_relax.h"

/* PAUSE instructions in the short spin before sleeping. */
#define WAL_GCL_SPIN_PAUSES 32

/* A binary semaphore on a futex word, with regards to MariaDB's
 * binary_semaphore (log0sync.cc). sema_wait sleeps until the word is 1
 * and resets it to 0. sema_wake sets it to 1 and enters the kernel only
 * if it was 0. The word is on the waiter's stack, which may be gone by
 * the FUTEX_WAKE call; the call never touches the memory, so at worst it
 * wakes nobody. */
static void sema_wait(_Atomic uint32_t *s)
{
    for (;;) {
        if (atomic_exchange_explicit(s, 0, memory_order_acq_rel) == 1)
            return;
        syscall(SYS_futex, s, FUTEX_WAIT_PRIVATE, 0, NULL, NULL, 0);
    }
}

static void sema_wake(_Atomic uint32_t *s)
{
    if (atomic_exchange_explicit(s, 1, memory_order_acq_rel) == 0)
        syscall(SYS_futex, s, FUTEX_WAKE_PRIVATE, 1, NULL, NULL, 0);
}

/* The lock's value, with acquire. */
static inline uint64_t load_value(const WalGroupCommitLock *l)
{
    return atomic_load_explicit(l->value, memory_order_acquire);
}

int wal_gcl_init(WalGroupCommitLock *l, _Atomic uint64_t *value)
{
    if (!l || !value) return MYDB_ERR;
    if (pthread_mutex_init(&l->mtx, NULL) != 0) return MYDB_ERR;
    l->value   = value;
    atomic_init(&l->pending, 0);
    l->locked  = false;
    l->waiters = NULL;
    return MYDB_OK;
}

void wal_gcl_destroy(WalGroupCommitLock *l)
{
    assert(!l->locked && !l->waiters);
    pthread_mutex_destroy(&l->mtx);
}

void wal_gcl_set_pending(WalGroupCommitLock *l, uint64_t num)
{
    /* A leader that release picked may find num already reached:
     * nothing to promise then. */
    if (num <= load_value(l)) return;
    /* Relaxed: only a hint for whether to spin. */
    atomic_store_explicit(&l->pending, num, memory_order_relaxed);
}

/* With regards to MariaDB's group_commit_lock::acquire. */
WalGclResult wal_gcl_acquire(WalGroupCommitLock *l, uint64_t num)
{
    if (num <= load_value(l)) return WAL_GCL_EXPIRED;

    /* The running leader will reach pending: worth a short spin. A
     * target past it waits for a whole next round, so sleep at once. */
    if (num <= atomic_load_explicit(&l->pending, memory_order_relaxed)) {
        for (int i = 0; i < WAL_GCL_SPIN_PAUSES; i++) cpu_relax();
        if (num <= load_value(l)) return WAL_GCL_EXPIRED;
    }

    WalGcWaiter w = { .num = num, .leader = false, .next = NULL };
    atomic_init(&w.signalled, 0);

    /* Loop until num is reached, unless release made us the next leader:
     * then we must take the lock even if it is reached (see the header). */
    while (num > load_value(l) || w.leader) {
        pthread_mutex_lock(&l->mtx);

        /* Reached, and either not the leader, or the lock is held by
         * someone else, whose release will wake the rest. */
        if (num <= load_value(l) && (!w.leader || l->locked)) {
            pthread_mutex_unlock(&l->mtx);
            return WAL_GCL_EXPIRED;
        }

        if (!l->locked) {
            l->locked = true;
            pthread_mutex_unlock(&l->mtx);
            return WAL_GCL_ACQUIRED;
        }

        /* Held by another leader: join the list and sleep. */
        w.leader  = false;
        w.next    = l->waiters;
        l->waiters = &w;
        pthread_mutex_unlock(&l->mtx);

        sema_wait(&w.signalled);   /* release took us off the list first */
    }
    return WAL_GCL_EXPIRED;
}

WalGclResult wal_gcl_try_acquire(WalGroupCommitLock *l, uint64_t num)
{
    if (num <= load_value(l)) return WAL_GCL_EXPIRED;

    WalGclResult r;
    pthread_mutex_lock(&l->mtx);
    if (num <= load_value(l)) r = WAL_GCL_EXPIRED;
    else if (l->locked)       r = WAL_GCL_BUSY;
    else                      { l->locked = true; r = WAL_GCL_ACQUIRED; }
    pthread_mutex_unlock(&l->mtx);
    return r;
}

/* Finds the waiters whose LSN value has reached, removes them from the
 * lock's waiting list, and adds them to the front of *list. Caller holds
 * l->mtx. */
static void find_waiters_reached(WalGroupCommitLock *l, uint64_t value, WalGcWaiter **list)
{
    WalGcWaiter **pp = &l->waiters;
    while (*pp) {
        WalGcWaiter *w = *pp;
        if (w->num <= value) {
            *pp     = w->next;
            w->next = *list;
            *list   = w;
        } else {
            pp = &w->next;
        }
    }
}

/* With regards to MariaDB's group_commit_lock::release. */
void wal_gcl_release(WalGroupCommitLock *l, uint64_t num)
{
    WalGcWaiter *wake = NULL;

    pthread_mutex_lock(&l->mtx);
    assert(l->locked);
    assert(num >= atomic_load_explicit(l->value, memory_order_relaxed));

    l->locked = false;
    atomic_store_explicit(l->value, num, memory_order_release);

    find_waiters_reached(l, num, &wake);

    /* Someone's LSN is still not reached: make one the next leader, so
     * nobody sleeps forever with the lock free. It goes to the head of the
     * wake list, to be woken first. */
    if (l->waiters) {
        WalGcWaiter *w = l->waiters;
        l->waiters = w->next;
        w->leader  = true;
        w->next    = wake;
        wake       = w;
    }
    pthread_mutex_unlock(&l->mtx);

    /* Wake outside the mutex, the next leader first. */
    wal_gcl_wake_waiters(&wake, 0);   /* all */
}

void wal_gcl_find_waiters_reached(WalGroupCommitLock *l, WalGcWaiter **list)
{
    pthread_mutex_lock(&l->mtx);
    assert(l->locked);
    /* locked stays true: the caller still leads. */
    find_waiters_reached(l, atomic_load_explicit(l->value, memory_order_relaxed), list);
    pthread_mutex_unlock(&l->mtx);
}

void wal_gcl_wake_waiters(WalGcWaiter **list, uint16_t n)
{
    /* n == 0 is checked by value, not used as a count, so the whole list
     * is woken however long it is. */
    for (uint16_t i = 0; *list && (n == 0 || i < n); i++) {
        WalGcWaiter *w = *list;
        *list = w->next;           /* before the wake: then its stack is gone */
        sema_wake(&w->signalled);
    }
}
