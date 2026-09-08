#ifndef LARGE_WAL_REGISTRY_H
#define LARGE_WAL_REGISTRY_H

#include <stdint.h>
#include <pthread.h>
#include "common.h"

/*
 * large_wal_registry.h — the (segment_no -> fd) table spanning a
 * segment's whole life, this is shared infrastructure both the
 * writer and the archiver reach into.
 *
 * large_wal_writer (large_wal_writer.h) calls register() immediately
 * after every claim_next(), passing the rotation slot's own
 * process-lifetime fd (owns_fd = 0 — the pool's own shutdown closes it,
 * not this registry). large_wal_archiver's copy_out() (large_wal_
 * archiver.h) calls set_fd() to repoint the same entry at the
 * holding-area fd once the segment migrates (owns_fd = 1 — this
 * registry opened that fd itself and is responsible for closing it).
 * A single fd-keyed table works for both locations with no separate
 * location tag: a rotation slot's fd and a holding-area file's fd are
 * just two different fds the same segment_no points at over its
 * lifetime.
 *
 * owns_fd exists to fix a latent bug:
 * blindly closing every entry's fd on shutdown was correct back when
 * every entry was archiver-opened (Phase 3), but wrong once pool-owned
 * rotation fds joined the same table — closing those here would race
 * the pool's own shutdown closing them a second time.
 *
 * ------------------------------------------------------------------
 * Concurrency
 * ------------------------------------------------------------------
 * This is where large_wal's segment locking lives. It is a linked list
 * rather than the growable array it started as, and that is a locking
 * decision, not a performance one: a per-entry pthread_mutex_t cannot
 * live in a realloc'd array (realloc relocates it, and swap-with-last
 * removal silently reassigns it to a different segment) and both are
 * undefined behaviour if any thread is inside it. Individually
 * malloc'd nodes never move, so a node's mutex sits at one address from
 * malloc to free. Insertion is a malloc plus one pointer store, so it
 * also stops disturbing entries nobody asked about — the array's
 * realloc had to copy every one of them.
 *
 * Two locks, following the pattern normal_wal's WalRingBuffer and
 * partition_manager's PartitionBuffer already use — a plain named lock
 * embedded in the struct it protects:
 *
 *   reg->lock    (rwlock) protects the list *structure* — head, next,
 *                count. Read mode to walk it, write mode to insert or
 *                unlink.
 *   node->lock   (mutex)  protects *I/O against that segment's fd*, AND
 *                the fd/owns_fd fields themselves — held across
 *                large_wal_get's pread loop, the writer's pwrite, and
 *                free_slot's zeroing, and taken briefly by lookup() for
 *                nothing more than reading fd. That last one is not
 *                optional: set_fd writes fd under this mutex, so a
 *                reader taking only the list lock races it. The list
 *                lock covers head/next/count and nothing inside a node.
 *
 * THE INVARIANT EVERYTHING RESTS ON: a node mutex is only ever acquired
 * while holding reg->lock in READ mode, and is always released before
 * it. So holding a node mutex implies holding the list read lock. That
 * is what acquire()/release() below exist to enforce — nothing should
 * touch node->lock by hand.
 *
 * It buys the whole lifetime question at once: register's insert and
 * remove's unlink take the WRITE lock, which cannot be held while any
 * reader holds the read lock, which means nobody holds any node's
 * mutex. So unlink-then-free(node) is safe, pthread_mutex_destroy
 * included — no refcount, no epoch reclamation, no retire list.
 *
 * It also decouples the writer from the readers: the writer needs only
 * the read lock plus the active segment's own mutex, so a reader on
 * segment 4 and the writer appending to segment 20 hold the same shared
 * read lock and two different mutexes, and never contend.
 *
 * Global lock order across large_wal is
 *     reg->lock -> node->lock -> pool->lock -> idx->lock -> state->lock
 * acquired in increasing order only. In particular claim_next
 * (large_wal_segment_pool.h) must release pool->lock before calling
 * register() — nesting the other way closes a cycle against copy_out,
 * which goes reg -> node -> free_slot -> pool.
 *
 * Read that rule precisely: it forbids nesting BACKWARDS, not nesting.
 * Holding two of these at once is allowed, and copy_out really does —
 * it holds reg->lock plus a node mutex across free_slot(), which takes
 * pool->lock, because "the rotation slot was zeroed" and "the registry
 * now points at the holding-area copy" have to be one indivisible step
 * to a reader. That is the ONLY site in large_wal where two locks from
 * different levels are live together, and it runs down the order.
 *
 * Everyone else who needs two of them takes them in SEQUENCE instead:
 * locked_pwrite reads slots[].header.segment_no under pool->lock,
 * releases it, and only then takes reg->lock plus the node mutex for
 * the pwrite; claim_next stamps its slot header under pool->lock,
 * releases it, and only then takes reg->lock in write mode to register.
 * Neither ever holds pool->lock while reaching for the registry.
 *
 * That asymmetry is the whole defence. A deadlock needs a cycle, and a
 * cycle needs BOTH directions to exist somewhere in the program. Only
 * one does. Concretely, the pair that would deadlock is:
 *
 *     thread A: holds reg->lock (read)  ->  wants pool->lock
 *     thread B: holds pool->lock        ->  wants reg->lock (write)
 *
 * B cannot have the write lock while A holds a read lock; A cannot have
 * pool->lock while B holds it; neither releases before finishing, so
 * both sleep forever. It costs no CPU and prints nothing — the process
 * simply stops answering, which is why the rule is enforced by ordering
 * up front rather than by noticing the symptom later. Note also that
 * once B is queued for the write lock, glibc stops granting new read
 * locks (writers would starve otherwise), so an uninvolved third reader
 * hangs too and the whole subsystem goes with it. Thread A above does
 * not exist here because of locked_pwrite's release, and thread B does
 * not exist because of claim_next's — either one alone would be enough.
 *
 * lookup() is O(N) with a linear walk, as the array was. N is bounded
 * by live segments — 4 rotation slots plus the holding-area backlog —
 * so it stays far below the cost of the pread it precedes.
 */

typedef struct LargeWalRegistryNode {
    uint64_t segment_no;
    int      fd;
    int      owns_fd;   /* 1 = this registry closes fd on shutdown/remove
                            (archiver-opened holding-area file); 0 = fd is
                            borrowed from the pool's own lifecycle */

    pthread_mutex_t lock;   /* protects I/O against this segment's fd: held
                                across large_wal_get's pread loop, the
                                writer's pwrite, and free_slot's zeroing.
                                Only ever taken via acquire() below, i.e.
                                under reg->lock in read mode. */

    struct LargeWalRegistryNode *next;
} LargeWalRegistryNode;

typedef struct {
    LargeWalRegistryNode *head;   /* individually malloc'd nodes — never
                                      relocated, so node->lock has a stable
                                      address for the node's whole life */
    uint32_t              count;

    pthread_rwlock_t      lock;   /* protects the list structure: head, next,
                                      count. Read mode to walk, write mode to
                                      insert or unlink. */
} LargeWalRegistry;

/* Initialises an empty list and its rwlock. The rwlock is created
 * writer-preferring (PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP):
 * glibc's default is reader-preferring, under which a steady stream of
 * large_wal_get calls would starve register()/remove() indefinitely. */
int large_wal_registry_init(LargeWalRegistry *reg);

/* Closes every node whose owns_fd == 1, leaves borrowed (owns_fd == 0)
 * nodes' fds untouched — their owner (the segment pool) closes those
 * itself. Frees every node either way, and destroys both the node
 * mutexes and the list rwlock. Callers must have quiesced every thread
 * that could still be inside the registry first. */
int large_wal_registry_shutdown(LargeWalRegistry *reg);

/* Registers segment_no -> fd if not already present, else updates the
 * existing node's fd (and owns_fd). Takes the list lock in WRITE mode,
 * so it must not be called while holding a node lock — use set_fd() for
 * that case, which is exactly why it exists. */
int large_wal_registry_register(LargeWalRegistry *reg, uint64_t segment_no, int fd, int owns_fd);

/* Resolves segment_no and hands back BOTH the fd and the node the
 * caller must hand back to release(). On success the list read lock and
 * that node's mutex are both held on return — every byte of I/O against
 * *out_fd must happen between this call and release(), which is the
 * whole point: it is what stops copy_out relocating the segment, or
 * free_slot zeroing it, mid-pread.
 *
 * Returns MYDB_ERR_NOT_FOUND holding nothing. A miss is a legitimate
 * answer, not an error: it means try_free has already freed this
 * segment, so the record genuinely is gone. */
int large_wal_registry_acquire(LargeWalRegistry *reg, uint64_t segment_no,
                                LargeWalRegistryNode **out_node, int *out_fd);

/* Releases what acquire() took, in the reverse order it took them:
 * node mutex first, then the list read lock. */
void large_wal_registry_release(LargeWalRegistry *reg, LargeWalRegistryNode *node);

/* Repoints an already-acquired node at a new fd. The caller must hold
 * that node via acquire() — this exists so copy_out can swap the
 * rotation slot's fd for the holding-area one without releasing the
 * node lock it is already holding across free_slot, which
 * large_wal_registry_register (a WRITE-lock call) could not do without
 * deadlocking against the read lock acquire() still holds. */
int large_wal_registry_set_fd(LargeWalRegistryNode *node, int fd, int owns_fd);

/* Copies the matching fd into *out_fd under the list read lock, then
 * releases it. Returns MYDB_ERR_NOT_FOUND if segment_no was never
 * registered.
 *
 * An identity/existence query only. The returned fd is NOT safe to do
 * I/O on: nothing stops try_free closing it the moment this returns.
 * Anything that reads or writes segment bytes must use acquire()/
 * release() instead. */
int large_wal_registry_lookup(LargeWalRegistry *reg, uint64_t segment_no, int *out_fd);

/* Unlinks and frees the node for segment_no under the list WRITE lock —
 * which, per the invariant above, guarantees no thread is inside that
 * node's mutex, making both the free and the mutex destroy safe.
 *
 * Does NOT close the fd, keeping large_wal_archiver_try_free (which
 * already knows whether it opened this one) responsible for that. It
 * hands the fd back through *out_fd / *out_owns_fd instead — both
 * optional — because once the node is unlinked the caller can no longer
 * look it up to find out. Closing it after this returns is safe: the
 * write lock drained every reader, and the node is gone, so no new
 * reader can ever resolve to that fd again.
 *
 * A no-op, still MYDB_OK, if segment_no isn't registered. */
int large_wal_registry_remove(LargeWalRegistry *reg, uint64_t segment_no,
                               int *out_fd, int *out_owns_fd);

#endif /* LARGE_WAL_REGISTRY_H */
