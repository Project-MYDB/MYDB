#ifndef WAL_MANAGER_H
#define WAL_MANAGER_H

#include <stdint.h>
#include "common.h"
#include "wal_ring_buffer.h"
#include "wal_segment_pool.h"
#include "wal_flusher.h"
#include "wal_worker.h"

/*
 * wal_manager.h — owns every WAL sub-piece for one partition: the
 * WalWorker, the ring buffer, the segment pool and the Flusher. It is
 * the WAL's single front door. Lifecycle-and-ownership only, no
 * business-logic functions here — mostly composition of pieces that
 * already exist (WalWorker, WalRingBuffer, WalSegmentPool, WalFlusher),
 * no new logic.
 *
 * This was normal_wal_manager.h, which mirrored LargeWalManager's shape
 * (large_wal/large_wal_manager.h), with a separate top-level wal_manager
 * owning the shared worker plus both subsystems' managers. Once records
 * were allowed to span pages and segments there was one log and no
 * large_wal, so the two layers were merged into this one and it took over
 * owning the worker (it used to only pass one through).
 *
 * Known, pre-existing, unrelated gap this inherits rather than solves:
 * wal_flusher_start()'s own doc comment already says it takes an
 * already-claimed slot and does no reload/resume logic of its own —
 * the WAL never got the equivalent of large_wal_writer_init's
 * find_or_claim_active() (scan for an already-ACTIVE slot on restart).
 * So init() here always claims a fresh segment, same limitation
 * WalFlusher already has today.
 */

typedef struct {
    WalWorker      worker;
    WalRingBuffer  rb;
    WalSegmentPool pool;
    WalFlusher     flusher;
} WalManager;

/* wal_worker_start -> wal_segment_pool_init -> claim a fresh segment ->
 * wal_ring_buffer_init at that segment's first record
 * (wal_segment_first_lsn(segment_no)) -> wal_flusher_start(...,
 * &wm->worker), unwinding whatever already succeeded on any failure.
 * The ring used to be initialised first, always at LSN 0; now that the
 * LSN is a position in the log it has to start where the claimed segment
 * is, so the pool comes first. Each boot still starts a fresh segment
 * (resuming the old tail is the recovery/Analysis piece). The worker is
 * now a field of WalManager (it was a pass-through parameter): it is
 * handed to the Flusher so its write() calls can overlap a rollover's
 * two fsyncs (wal_worker.h). */
int wal_manager_init(WalManager *wm, const char *wal_dir, uint32_t partition_id);

/* Appends one record (wal_ring_buffer_append). If the ring is full it
 * wakes the Flusher, waits for space (wal_ring_buffer_append_wait) and
 * tries again, so it only returns once the record is in the ring.
 * *out_lsn / *out_end_lsn are where it starts and just past its end.
 * Known gaps (T-039 part 1): a record bigger than the ring would wait
 * forever, and so would any append if the Flusher can never write. */
int wal_manager_append(WalManager *wm, WalRecordHeader *hdr,
                       const void *body, size_t body_len,
                       uint64_t *out_lsn, uint64_t *out_end_lsn);

/* Wakes the Flusher and blocks until everything before lsn is on disk
 * (pass a record's end LSN). The commit gate (design doc §11). */
int wal_manager_wait_flushed(WalManager *wm, uint64_t lsn);

/* Reverses init(): flusher_stop, segment_pool_shutdown,
 * ring_buffer_shutdown, worker_stop. Safe on a partially-initialised wm. */
int wal_manager_shutdown(WalManager *wm);

#endif /* WAL_MANAGER_H */
