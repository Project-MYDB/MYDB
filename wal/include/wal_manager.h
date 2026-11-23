#ifndef WAL_MANAGER_H
#define WAL_MANAGER_H

#include <stdint.h>
#include "common.h"
#include "wal_segment_pool.h"
#include "wal_worker.h"
#include "wal_frame_state.h"

/*
 * wal_manager.h — owns every WAL sub-piece for one partition. It is the
 * WAL's single front door, Lifecycle and ownership
 * here; the insert path will live in wal_insert.{h,c} and the write and
 * flush path in wal_flush.{h,c}, both working on a WalManager *.

 *
 * Start-up finds the end of the log (wal_segment_pool_tail_scan) before
 * anything else; only a brand-new log gets a fresh segment. ARIES
 * redo/undo on top of that is the recovery piece's job.
 */

/* Ring frames, a power of two (the ring frame of an LSN is a mask).
 * 1024 frames = a 4MB ring. V2 leaves the default OPEN (§16, T-068). */
#define WAL_RING_FRAMES 1024u
_Static_assert((WAL_RING_FRAMES & (WAL_RING_FRAMES - 1)) == 0, "WAL_RING_FRAMES must be a power of two");

/* Layout as V2 §5: fields nobody writes after init share the first cache
 * line (no false sharing from fields that never change), and each hot
 * atomic gets its own line: current_pos is hit by every insert,
 * write_lsn by every space check that has to reload it.
 *
 * Because of the _Alignas(64) members the struct's alignment is 64. On
 * the stack or as a global that is automatic; a heap-allocated
 * WalManager (e.g. inside PartitionCtx later) must come from
 * aligned_alloc(64, ...), since malloc only promises 16. */
typedef struct {
    /* set at init, read-only after */
    uint8_t         *ring_buf;        /* WAL_RING_FRAMES * 4KB, aligned 4096   */
    WalBlockCounter *fs_counters;     /* one per 512B block, aligned 64, zeroed */
    uint32_t         ring_buf_size;   /* frames, power of two                 */
    uint32_t         ring_frame_mask; /* ring_buf_size - 1                    */
    uint32_t         ctr_mask;        /* ring_buf_size * WAL_FRAME_BLOCKS - 1 */
    uint32_t         partition_id;

    /* hot atomics, one cache line each */
    _Alignas(64) _Atomic uint64_t current_pos;  /* next reservation, in record bytes (V2 §4) */
    _Alignas(64) _Atomic uint64_t write_lsn;    /* pwritten to the segment up to here       */

    /* owned by whoever runs the write step (wal_flush); the pool and
     * worker were here before V2 */
    _Alignas(64) uint32_t curr_slot;    /* the active segment slot (V2 §5)      */
    uint32_t              last_ctr_idx; /* the block the next write-step scan
                                           starts at (V2 §10.2, §10.7)        */
    WalSegmentPool        pool;
    WalWorker             worker;
} WalManager;



/* wal_worker_start -> wal_segment_pool_init -> find the end of the log
 * and resume there, unwinding whatever already succeeded on any failure:
 *   - an existing log: wal_segment_pool_tail_scan finds the end and its
 *     segment's slot (curr_slot), wal_segment_pool_truncate cuts the log
 *     there (later segments discarded, everything after the end zeroed,
 *     synced);
 *   - a brand-new log (no ACTIVE segment): claim a fresh segment, whose
 *     start is the end.
 * Then allocates the ring (WAL_RING_FRAMES frames, 4096-aligned) and its
 * block counters (64-aligned), both zeroed, and resumes at the end (V2
 * §10.7): current_pos = record bytes before it, write_lsn = the end, the
 * end's partial page copied into its ring frame, the counter of the
 * end's block seeded with the bytes already in it, last_ctr_idx = that
 * block. An end at a page's offset 24 resumes at the page's start (the
 * next copier writes that header). Same path after a clean shutdown or a
 * crash. ARIES redo/undo is the recovery piece, on top of this. */
int wal_manager_init(WalManager *wm, const char *wal_dir, uint32_t partition_id);

/* Reverses init(): frees the ring and counters, segment_pool_shutdown,
 * worker_stop. Safe on a partially-initialised wm. */
int wal_manager_shutdown(WalManager *wm);

#endif /* WAL_MANAGER_H */
