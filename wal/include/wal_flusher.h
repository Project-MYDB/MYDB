#ifndef WAL_FLUSHER_H
#define WAL_FLUSHER_H

#include <pthread.h>
#include <stdint.h>
#include "common.h"
#include "wal_ring_buffer.h"
#include "wal_segment_pool.h"
#include "wal_worker.h"

/*
 * wal_flusher.h — the Flusher thread (MYDB_WAL_DESIGN.md §7).
 *
 * Two-phase drain, matching the design doc's own stated reason for the
 * algorithm's shape: writer threads calling wal_ring_buffer_append()
 * must never block on disk I/O, only on a brief memcpy. Each cycle:
 *   1. wal_ring_buffer_snapshot() — locks the ring briefly, copies
 *      closed frames into this Flusher's own scratch buffer, unlocks.
 *   2. wal_segment_pool_write() against that copy — the slow I/O part,
 *      entirely off the ring's lock.
 *   3. mark the batch flushed (flush_lsn, below) once it is confirmed
 *      durable. Durability is the Flusher's progress on disk, not ring
 *      memory, and the Flusher is flush_lsn's only writer. WalManager
 *      (wal_manager.h) owns the Flusher and exposes the wait to
 *      committers.
 *
 * Owns the design doc §7 cursor triad faithfully: buf_cursor (ring-side
 * position, reported by snapshot()), seg_cursor (segment-side position,
 * reported by wal_segment_pool_write()), and seg_no (the current
 * segment's own persistent number). seg_slot_index is the one thing not
 * named in the docs — segment addressing is the pool's own internal
 * concern — kept alongside seg_no rather than re-derived every cycle.
 *
 * No ATT/DPT, no real commit-path wiring, no Checkpointer, one Flusher
 * per explicit (WalRingBuffer, WalSegmentPool) pair the caller supplies
 * — still standalone, not wired into PartitionCtx/TransactionManager.
 */

#define WAL_FLUSHER_PERIODIC_MS 20   /* placeholder — same "tunable, not
                                         load-tested" status the design
                                         doc gives every other timing
                                         constant */

typedef struct {
    WalRingBuffer   *rb;
    WalSegmentPool  *pool;
    WalWorker       *worker;   /* not owned; may be NULL — see wal_flusher_start */

    WalCursor  buf_cursor;      /* position in the WAL ring buffer, as the last
                                    snapshot() reported it — page_no is the ring
                                    frame the ring's drained_lsn points at, lsn is
                                    the end of the last copied page, offset always
                                    0 this phase (whole-frame draining only). The
                                    ring owns the reader's position itself
                                    (drained_lsn); this is the Flusher's record of
                                    it */
    WalCursor  seg_cursor;      /* position in the WAL segment — page_no/offset
                                    mirror wal_segment_pool_write's own out-
                                    params exactly; lsn is the end of the most
                                    recently *durably written* page (its
                                    start_lsn + WAL_PAGE_SIZE) —
                                    deliberately distinct from buf_cursor.lsn
                                    (which advances at snapshot time, before
                                    I/O; this only advances once a frame is
                                    actually written) */
    uint64_t   seg_no;          /* the current segment's own persistent number
                                    (design doc §7) — refreshed from
                                    pool->slots[seg_slot_index].header.segment_no
                                    after every write, since a write may
                                    auto-roll to a new slot */
    uint32_t   seg_slot_index;  /* which physical pool slot seg_cursor targets */

    uint8_t         *scratch;   /* WAL_RING_BUFFER_SIZE bytes, allocated once at start */

    pthread_t        thread;
    uint8_t          started;
    uint8_t          stop_requested;

    pthread_mutex_t  demand_mutex;
    pthread_cond_t   demand_cond;   /* signaled for demand-wake; timedwait
                                        also covers the periodic trigger */

    /* Durability. flush_lsn is a byte position in the log: everything
     * before it is on disk. It can fall in the middle of a record; a
     * record is durable once flush_lsn >= its end LSN (what
     * wal_ring_buffer_append hands back). Every read goes through
     * flush_mutex. */
    uint64_t         flush_lsn;
    pthread_mutex_t  flush_mutex;   /* protects flush_lsn */
    pthread_cond_t   flush_cond;    /* paired with flush_mutex, for
                                       wal_flusher_wait_flushed() waiters */
} WalFlusher;

/* Allocates scratch, seeds seg_cursor/seg_no/seg_slot_index from the
 * already-claimed slot the caller passes (wal_segment_pool_claim_next
 * must have been called first — this function does not claim one
 * itself), starts the thread. The ring must start exactly at that
 * segment's first record (wal_segment_first_lsn(seg_no)) — page 1, where
 * seg_cursor starts — or this returns MYDB_ERR: there is no resume into
 * a half-written segment yet. flush_lsn starts at that same position
 * (nothing before it was ever this log's to flush). worker (may be NULL) is stored and
 * forwarded into every wal_segment_pool_write() call this Flusher makes
 * — see wal_segment_pool.h's own worker parameter doc comment. */
int wal_flusher_start(WalFlusher *f, WalRingBuffer *rb, WalSegmentPool *pool,
                       uint32_t seg_slot_index, WalWorker *worker);

/* Sets stop_requested, signals demand_cond so the thread wakes
 * immediately rather than waiting out its timeout, pthread_join's,
 * frees scratch. Safe to call even if start() was never called
 * (started == 0) or after an earlier stop() — a no-op in both cases. */
int wal_flusher_stop(WalFlusher *f);

/* Demand-wake: pthread_cond_signal(&f->demand_cond) under demand_mutex.
 * The primitive a future commit path calls right after appending its
 * COMMIT record, before waiting on wal_flusher_wait_flushed — design
 * doc §11's commit gate.
 * Also how wal_manager_append wakes the Flusher when the ring is full.
 * Not wired to any real commit path yet. */
void wal_flusher_signal(WalFlusher *f);

/* Blocks until flush_lsn >= lsn, via flush_mutex/flush_cond. Pass a
 * record's end LSN. Known gap: if the Flusher can never write, this waits
 * forever; the Flusher piece adds an error state that ends the wait. */
int wal_flusher_wait_flushed(WalFlusher *f, uint64_t lsn);

/* flush_lsn, read under flush_mutex. Non-blocking. */
uint64_t wal_flusher_flushed_lsn(WalFlusher *f);

#endif /* WAL_FLUSHER_H */