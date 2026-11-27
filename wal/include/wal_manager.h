#ifndef WAL_MANAGER_H
#define WAL_MANAGER_H

#include <stdbool.h>
#include <stdint.h>
#include "common.h"
#include "wal_segment_pool.h"
#include "wal_worker.h"
#include "wal_frame_state.h"
#include "wal_group_commit_lock.h"

/*
 * wal_manager.h — one partition's WAL: the ring and its block counters,
 * the log positions, the group commit locks, the segment pool and the
 * worker. The insert path (wal_insert) and the write and flush path
 * (wal_flush) work on it. Start-up resumes at the end of the log found
 * on disk; replaying it (ARIES redo/undo) is recovery's job.
 */

/* Frames in the ring, a power of two: 2048 frames = an 8MB ring. */
#define WAL_RING_FRAMES 2048u
_Static_assert((WAL_RING_FRAMES & (WAL_RING_FRAMES - 1)) == 0, "WAL_RING_FRAMES must be a power of two");

/* The WAL of one partition (V2 §5). Fields fixed after init share the
 * first cache line; each hot atomic and each lock has its own. Its
 * alignment is 64, so a heap-allocated one needs aligned_alloc(64, ...). */
typedef struct {
    /* set at init, read-only after */
    uint8_t         *ring_buf;        /* WAL_RING_FRAMES * 4KB, aligned 4096   */
    WalBlockCounter *fs_counters;     /* one per 512B block, aligned 64, zeroed */
    uint32_t         ring_buf_size;   /* frames, power of two                 */
    uint32_t         ring_frame_mask; /* ring_buf_size - 1                    */
    uint32_t         ctr_mask;        /* ring_buf_size * WAL_FRAME_BLOCKS - 1 */
    uint32_t         partition_id;

    /* hot atomics, one cache line each */
    _Alignas(64) _Atomic uint64_t current_pos;  /* next reservation, in record bytes */
    _Alignas(64) _Atomic uint64_t write_lsn;    /* written to the files up to here   */
    _Alignas(64) _Atomic uint64_t flushed_lsn;  /* synced up to here                 */

    /* their values are write_lsn and flushed_lsn */
    _Alignas(64) WalGroupCommitLock write_lock;   /* guards the write step */
    _Alignas(64) WalGroupCommitLock flush_lock;   /* guards the fdatasync  */

    /* touched only by the holder of write_lock (and init) */
    _Alignas(64) uint32_t curr_slot;    /* the slot being written               */
    uint32_t              last_ctr_idx; /* the block the next scan starts at   */
    WalSegmentPool        pool;
    bool                  locks_up;     /* write_lock and flush_lock exist */
    WalWorker             worker;
} WalManager;



/* Opens the pool and resumes at the end of the log: an existing log is
 * scanned (wal_segment_pool_tail_scan) and cut there
 * (wal_segment_pool_truncate); a new log claims its first segment. Then
 * allocates the ring and counters, copies the end's partial page into its
 * frame and seeds its block's counter, sets current_pos, write_lsn,
 * flushed_lsn and last_ctr_idx to the end, and starts the worker. Undoes
 * everything on failure. */
int wal_manager_init(WalManager *wm, const char *wal_dir, uint32_t partition_id);

/* Undoes init, the worker first (it syncs what was requested). Safe on a
 * partly initialised wm; no other thread may use it. Returns MYDB_ERR if
 * the worker had hit a failed fdatasync. */
int wal_manager_shutdown(WalManager *wm);

#endif /* WAL_MANAGER_H */
