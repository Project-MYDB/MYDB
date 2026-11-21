#include "wal_mailbox.h"

#include <string.h>

/* ------------------------------------------------------------------
 * Thread loop
 *
 * Takes the head of the queue, runs the handler with NO lock held (the
 * work is an fsync or a 2MB write — holding the lock across it would
 * stall every poster and every waiter), then publishes the result.
 * ------------------------------------------------------------------ */

static void *mailbox_main(void *arg)
{
    WalMailbox *mb = (WalMailbox *)arg;

    for (;;) {
        pthread_mutex_lock(&mb->lock);
        while (!mb->head && !mb->stop_requested)
            pthread_cond_wait(&mb->cond, &mb->lock);

        /* Rule 2: leave only once the queue is drained, never merely
         * because stop was asked for. A request already posted has a
         * caller parked in wait() for it. */
        if (!mb->head) {
            pthread_mutex_unlock(&mb->lock);
            break;
        }

        WalMailboxNode *node = mb->head;
        mb->head = node->next;
        if (!mb->head) mb->tail = NULL;
        node->next = NULL;

        /* The node stays counted in mb->depth while it runs and after it
         * finishes — only wait() gives the slot back (rule 1). */
        pthread_mutex_unlock(&mb->lock);

        int rc = mb->fn(mb->owner, node->payload);

        pthread_mutex_lock(&mb->lock);
        node->result = rc;
        node->done   = 1;
        pthread_cond_broadcast(&mb->cond);   /* rule 3 */
        pthread_mutex_unlock(&mb->lock);
    }

    return NULL;
}

/* ------------------------------------------------------------------
 * Lifecycle
 * ------------------------------------------------------------------ */

int wal_mailbox_start(WalMailbox *mb, WalMailboxFn fn, void *owner, uint32_t max_depth)
{
    if (!mb || !fn || max_depth == 0) return MYDB_ERR;

    memset(mb, 0, sizeof(*mb));
    mb->fn        = fn;
    mb->owner     = owner;
    mb->max_depth = max_depth;

    if (pthread_mutex_init(&mb->lock, NULL) != 0) return MYDB_ERR;
    if (pthread_cond_init(&mb->cond, NULL) != 0) {
        pthread_mutex_destroy(&mb->lock);
        return MYDB_ERR;
    }

    /* fn/owner/max_depth are written before the thread exists, so the
     * thread reading them unlocked is ordered by pthread_create itself.
     * Nothing mutates them afterwards. */
    if (pthread_create(&mb->thread, NULL, mailbox_main, mb) != 0) {
        pthread_cond_destroy(&mb->cond);
        pthread_mutex_destroy(&mb->lock);
        return MYDB_ERR;
    }

    mb->started = 1;
    return MYDB_OK;
}

int wal_mailbox_stop(WalMailbox *mb)
{
    if (!mb || !mb->started) return MYDB_OK;

    pthread_mutex_lock(&mb->lock);
    mb->stop_requested = 1;
    pthread_cond_broadcast(&mb->cond);

    /* Wait for every CALLER thread to leave mb->cond before tearing it
     * down. Two kinds are out there: a poster parked at the depth gate,
     * which will bail with MYDB_ERR now the flag is set, and a collector
     * parked on its own node->done, which the mailbox thread is still
     * alive to satisfy -- which is why this runs BEFORE the join, not
     * after. Destroying a condvar or mutex with a waiter still on it is
     * undefined behaviour, so "they were broadcast at" is not enough;
     * they have to be observed gone. */
    while (mb->blocked > 0)
        pthread_cond_wait(&mb->cond, &mb->lock);

    pthread_mutex_unlock(&mb->lock);

    pthread_join(mb->thread, NULL);

    pthread_cond_destroy(&mb->cond);
    pthread_mutex_destroy(&mb->lock);
    mb->started = 0;

    return MYDB_OK;
}

/* ------------------------------------------------------------------
 * post / wait / call
 * ------------------------------------------------------------------ */

int wal_mailbox_post(WalMailbox *mb, WalMailboxNode *node,
                     const void *request, size_t request_len)
{
    if (!node || !mb || !mb->started) return MYDB_ERR;
    if (request_len > WAL_MAILBOX_MAX_PAYLOAD) return MYDB_ERR;
    if (request_len > 0 && !request) return MYDB_ERR;

    /* Every write to *node happens below, inside the lock and past the
     * depth gate -- including clearing seq. Nothing may touch the node
     * before the gate opens: when two threads share one node (legal only
     * at max_depth == 1, see the header), a pre-gate write would wipe the
     * seq of the request the other thread still has outstanding. */
    pthread_mutex_lock(&mb->lock);

    /* The depth gate. At max_depth == 1 this is the strict handoff the
     * two hand-written mailboxes both had: one request in flight, and a
     * second poster waits its turn rather than clobbering the first. */
    while (mb->depth >= mb->max_depth && !mb->stop_requested) {
        mb->blocked++;
        pthread_cond_wait(&mb->cond, &mb->lock);
        mb->blocked--;
        /* Only on the way out of a shutdown: stop() is the one thing
         * that watches this counter, so the normal path stays quiet. */
        if (mb->stop_requested) pthread_cond_broadcast(&mb->cond);
    }

    if (mb->stop_requested) {
        node->seq = 0;
        pthread_mutex_unlock(&mb->lock);
        return MYDB_ERR;
    }

    /* Copied here, inside the lock and past the gate — see the header's
     * note on why the request is copied rather than borrowed. */
    node->next        = NULL;
    node->result      = MYDB_ERR;
    node->done        = 0;
    node->payload_len = (uint32_t)request_len;
    if (request_len > 0) memcpy(node->payload, request, request_len);
    if (request_len < WAL_MAILBOX_MAX_PAYLOAD)
        memset(node->payload + request_len, 0, WAL_MAILBOX_MAX_PAYLOAD - request_len);

    node->seq = ++mb->next_seq;   /* ++ first: 0 stays reserved */

    if (mb->tail) mb->tail->next = node;
    else          mb->head       = node;
    mb->tail = node;
    mb->depth++;

    pthread_cond_broadcast(&mb->cond);   /* rule 3 -- wakes the thread */
    pthread_mutex_unlock(&mb->lock);
    return MYDB_OK;
}

int wal_mailbox_wait(WalMailbox *mb, WalMailboxNode *node,
                     void *response, size_t response_len)
{
    /* Rule 4. Read without the lock, which is safe because seq is only
     * ever written by this node's own owner: post() and wait() are the
     * only writers and both run on the caller's thread. The mailbox
     * thread never touches it. */
    if (!node || node->seq == 0) return MYDB_OK;

    if (!mb || !mb->started) return MYDB_ERR;
    if (response_len > node->payload_len) return MYDB_ERR;

    pthread_mutex_lock(&mb->lock);

    while (!node->done) {
        mb->blocked++;
        pthread_cond_wait(&mb->cond, &mb->lock);
        mb->blocked--;
        if (mb->stop_requested) pthread_cond_broadcast(&mb->cond);
    }

    int rc = node->result;
    if (response && response_len > 0)
        memcpy(response, node->payload, response_len);

    node->seq = 0;     /* spent -- a second wait() on it is a no-op */
    mb->depth--;       /* rule 1: the slot comes back here, not earlier */

    pthread_cond_broadcast(&mb->cond);   /* a poster may be at the gate */
    pthread_mutex_unlock(&mb->lock);

    return rc;
}

int wal_mailbox_call(WalMailbox *mb, void *request, size_t request_len)
{
    WalMailboxNode node;
    memset(&node, 0, sizeof(node));

    int rc = wal_mailbox_post(mb, &node, request, request_len);
    if (rc != MYDB_OK) return rc;

    /* Copies back into the caller's own buffer, so a handler answers
     * through the same struct it was asked with. */
    return wal_mailbox_wait(mb, &node, request, request_len);
}
