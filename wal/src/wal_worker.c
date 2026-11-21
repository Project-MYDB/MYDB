#include "wal_worker.h"

#include <unistd.h>
#include <string.h>

/* ------------------------------------------------------------------
 * The whole job. Runs on the mailbox thread with no lock held.
 * ------------------------------------------------------------------ */

static int do_fdatasync(void *owner, void *request)
{
    (void)owner;
    int fd = *(const int *)request;
    return (fdatasync(fd) < 0) ? MYDB_ERR : MYDB_OK;
}

/* ------------------------------------------------------------------
 * Lifecycle
 * ------------------------------------------------------------------ */

int wal_worker_start(WalWorker *w)
{
    if (!w) return MYDB_ERR;

    memset(w, 0, sizeof(*w));

    /* Depth 1: one fsync in flight, a second caller waits its turn. That
     * is also what lets the single node above be shared by both caller
     * threads — see wal_mailbox.h. */
    return wal_mailbox_start(&w->mb, do_fdatasync, w, /*max_depth=*/1);
}

int wal_worker_stop(WalWorker *w)
{
    if (!w) return MYDB_OK;
    return wal_mailbox_stop(&w->mb);
}

/* ------------------------------------------------------------------
 * Submit / collect
 * ------------------------------------------------------------------ */

int wal_worker_async_fdatasync(WalWorker *w, int fd, uint64_t *out_ticket)
{
    if (out_ticket) *out_ticket = 0;
    if (!w) return MYDB_ERR;

    /* fd is a plain local: post() copies it into the node inside the
     * mailbox lock, past the depth gate. That ordering is the reason the
     * old `w->req_fd` field is gone — writing the fd into the worker
     * before posting would let two caller threads overwrite each other's
     * fd, and whichever won the gate would fsync the wrong file. */
    int rc = wal_mailbox_post(&w->mb, &w->node, &fd, sizeof(fd));
    if (rc != MYDB_OK) return rc;

    if (out_ticket) *out_ticket = w->node.seq;
    return MYDB_OK;
}

int wal_worker_wait(WalWorker *w, uint64_t ticket)
{
    /* The caller posted nothing, so there is nothing of theirs to
     * collect. Answered before anything else: this is the common case on
     * every write() that did not roll over. Note what is NOT tested
     * here -- whether the mailbox is busy. It may well be, with the
     * other caller thread's fsync, and that is precisely the request
     * this call must leave alone. */
    if (ticket == 0) return MYDB_OK;

    if (!w) return MYDB_ERR;

    /* Not the request in flight. Since a ticket is handed to exactly one
     * caller and node.seq is cleared once collected, this means it was
     * already waited on -- a caller bug, reported rather than swallowed.
     * Read unlocked, like wal_mailbox_wait's own seq check, because seq
     * is only ever written by the node's owner under the mailbox lock
     * and never by the mailbox thread. */
    if (w->node.seq != ticket) return MYDB_ERR;

    return wal_mailbox_wait(&w->mb, &w->node, NULL, 0);
}
