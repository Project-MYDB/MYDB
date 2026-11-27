#ifndef WAL_FLUSH_H
#define WAL_FLUSH_H

#include <stdbool.h>
#include <stdint.h>
#include "common.h"
#include "wal_manager.h"

/*
 * wal_flush.h — the write and flush path (V2 §12). The write step finds
 * how far the ring is completely copied, writes that range to the
 * segment files and publishes write_lsn. Two callers run it through
 * write_lock: a copier waiting for ring space, and a committing session,
 * which then fdatasyncs through flush_lock and publishes flushed_lsn.
 * Both locks are group commit locks: one leader works for all waiters.
 */

/* What a scan found. */
typedef struct {
    uint64_t end_lsn;      /* the ring is complete in [write_lsn, end_lsn) */
    uint32_t full_blocks;  /* blocks read as 512, from last_ctr_idx on    */
    bool     tail;         /* stopped in the last block with reservations,
                              complete so far; false at a hole or after a
                              whole ring                                  */
} WalScanResult;

/* Reports how far the ring is completely copied, starting at the block
 * holding write_lsn (last_ctr_idx). Reads only.
 *
 * A block whose counter is 512 is full, and the scan goes on. Otherwise
 * it reads current_pos, after the counter, and stops: the block is a
 * complete tail if its counter equals the bytes reserved in it so far,
 * and a hole if not. Reading the counter first means every byte it
 * counts was reserved below that current_pos, so a hole cannot hide.
 * Page header bytes are counted like any other bytes.
 *
 * A hole in the first block gives end_lsn = write_lsn. The scan stops
 * after one ring of blocks: then the whole ring is complete.
 * Caller holds write_lock. */
void wal_flush_scan(const WalManager *wm, WalScanResult *out);

/* The write step (V2 §12.2), caller holding write_lock. Scans (result in
 * *out), then:
 *   1. pwrites [write_lsn, end_lsn) in chunks that end at end_lsn, the
 *      ring's end or the segment's end;
 *   2. before a chunk in a segment curr_slot does not hold, marks the old
 *      segment DONE, asks the worker to fdatasync it (no waiting), and
 *      claims the next;
 *   3. zeroes the full blocks' counters, never the tail block's;
 *   4. sets last_ctr_idx to the block holding end_lsn;
 *   5. stores write_lsn = end_lsn with release, after the zeroes, so a
 *      copier that sees it sees them too.
 * No fsync of the active segment and no wake-up.
 *
 * Returns MYDB_OK, also with nothing to write. On a failed write returns
 * MYDB_ERR with write_lsn, last_ctr_idx and the counters unchanged. */
int wal_flush_write_chunk(WalManager *wm, WalScanResult *out);

/* For the holder of write_lock: runs the write step until write_lsn >=
 * target, then releases write_lock, which wakes the waiters whose LSN it reached.
 *
 * When a write step stops at a hole short of target, it waits for the
 * hole's block, still holding the lock (V2 §12.4). First it finds the
 * waiters whose LSN the new write_lsn already reached. Then, until the
 * block is complete: wake one of them, recheck, wake the next; with none
 * left, spin, yield, then sleep briefly. The rest are woken at the end.
 * The copier that owns the hole is always among them if it sleeps
 * there: it needs space only up to the hole's block.
 *
 * target must be reserved already. Returns MYDB_ERR if a write step
 * failed; write_lock is released either way. */
int wal_log_write(WalManager *wm, uint64_t target);

/* The space waiter (V2 §12.6): returns once write_lsn >= target. Takes
 * write_lock and writes the ring itself, or sleeps while another leader
 * does. Never fsyncs. Returns MYDB_ERR if a write step it led failed. */
int wal_flush_space_wait(WalManager *wm, uint64_t target);

/* The commit path (V2 §12.5): returns once flushed_lsn >= lsn. Returns at
 * once if already so. Otherwise takes flush_lock (or another leader
 * reaches lsn for it), gets write_lsn past lsn through write_lock, then
 * fdatasyncs the segment holding write_lsn's last byte with only
 * flush_lock held, waits until the worker has synced every segment before
 * it, and releases it with flushed_lsn = that write_lsn: everything
 * written so far, so later commits ride along.
 *
 * Returns MYDB_ERR if a write, its fdatasync or the worker's failed;
 * flushed_lsn then stays as it was. */
int wal_log_flush(WalManager *wm, uint64_t lsn);

#endif /* WAL_FLUSH_H */
