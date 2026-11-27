#include "wal_flush.h"

#include <assert.h>
#include <sched.h>
#include <stdatomic.h>
#include <time.h>
#include "cpu_relax.h"
#include "wal_frame_state.h"
#include "wal_page.h"

void wal_flush_scan(const WalManager *wm, WalScanResult *out)
{
    /* Relaxed: only write_lock's holder stores write_lsn, and that is us. */
    uint64_t write_lsn = atomic_load_explicit(&wm->write_lsn, memory_order_relaxed);
    uint32_t idx       = wm->last_ctr_idx;
    /* last_ctr_idx must match write_lsn. */
    assert(idx == ((write_lsn >> WAL_BLOCK_SHIFT) & wm->ctr_mask));

    uint64_t block_start = write_lsn & ~(uint64_t)(WAL_BLOCK_SIZE - 1);
    uint64_t end         = write_lsn;
    uint32_t full        = 0;
    bool     tail        = false;

    for (uint32_t n = 0; n <= wm->ctr_mask; n++) {   /* at most one ring */
        uint64_t block_end = block_start + WAL_BLOCK_SIZE;
        uint16_t d = atomic_load_explicit(&wm->fs_counters[idx].done, memory_order_acquire);

        if (d == WAL_BLOCK_SIZE) {                   /* full block */
            full++;
            end         = block_end;
            block_start = block_end;
            idx         = (idx + 1) & wm->ctr_mask;
            continue;
        }

        /* Hole or tail. current_pos is read after d (see wal_flush.h). */
        uint64_t current_lsn = wal_byte_to_lsn(atomic_load_explicit(&wm->current_pos, memory_order_acquire));
        assert(current_lsn >= block_start);

        /* A complete tail: every byte reserved in the block is counted. */
        if (d == current_lsn - block_start) {
            end  = current_lsn;
            tail = true;
        }
        break;
    }

    out->end_lsn     = end;
    out->full_blocks = full;
    out->tail        = tail;
}

/* curr_slot holds seg - 1: finish it, hand its fdatasync to the worker,
 * and claim seg. */
static int roll_over(WalManager *wm, uint64_t seg)
{
    assert(wal_segment_pool_segment_no(&wm->pool, wm->curr_slot) + 1 == seg);
    if (wal_segment_pool_mark_done(&wm->pool, wm->curr_slot,
                                   seg << WAL_SEG_SHIFT, WAL_SEG_PAGES) != MYDB_OK)
        return MYDB_ERR;
    wal_worker_request_sync(&wm->worker, seg << WAL_SEG_SHIFT);

    uint32_t slot;
    if (wal_segment_pool_claim_next(&wm->pool, &slot) != MYDB_OK) return MYDB_ERR;
    /* The pool's number must match the LSN's segment. */
    if (wal_segment_pool_segment_no(&wm->pool, slot) != seg) return MYDB_ERR;
    wm->curr_slot = slot;
    return MYDB_OK;
}

int wal_flush_write_chunk(WalManager *wm, WalScanResult *out)
{
    if (!wm || !out) return MYDB_ERR;

    wal_flush_scan(wm, out);

    uint64_t pos        = atomic_load_explicit(&wm->write_lsn, memory_order_relaxed);
    uint64_t end        = out->end_lsn;
    uint64_t ring_bytes = (uint64_t)wm->ring_buf_size << WAL_PAGE_SHIFT;

    while (pos < end) {
        uint64_t seg = wal_lsn_segment(pos);
        if (wal_segment_pool_segment_no(&wm->pool, wm->curr_slot) != seg &&
            roll_over(wm, seg) != MYDB_OK)
            return MYDB_ERR;

        uint64_t chunk_end = end;
        uint64_t ring_end  = (pos | (ring_bytes - 1)) + 1;
        uint64_t seg_end   = (seg + 1) << WAL_SEG_SHIFT;
        if (ring_end < chunk_end) chunk_end = ring_end;
        if (seg_end  < chunk_end) chunk_end = seg_end;

        if (wal_segment_pool_pwrite(&wm->pool, wm->curr_slot, pos,
                                    wm->ring_buf + (pos & (ring_bytes - 1)),
                                    (size_t)(chunk_end - pos)) != MYDB_OK)
            return MYDB_ERR;
        pos = chunk_end;
    }

    /* All written: zero the full blocks, then publish. The release orders
     * the relaxed zeroes. */
    uint32_t idx = wm->last_ctr_idx;
    for (uint32_t i = 0; i < out->full_blocks; i++) {
        atomic_store_explicit(&wm->fs_counters[idx].done, 0, memory_order_relaxed);
        idx = (idx + 1) & wm->ctr_mask;
    }
    assert(idx == ((end >> WAL_BLOCK_SHIFT) & wm->ctr_mask));   /* the block holding end */
    wm->last_ctr_idx = idx;
    atomic_store_explicit(&wm->write_lsn, end, memory_order_release);
    return MYDB_OK;
}

/* Spin with PAUSE, then yield, then sleep at least 50 µs (Linux timer
 * slack usually makes it 50 to 100 µs), by how long it has waited. */
static void thread_wait(uint32_t tries)
{
    if (tries < 64)        cpu_relax();
    else if (tries < 128)  sched_yield();
    else                   nanosleep(&(struct timespec){ .tv_nsec = 50000 }, NULL);
}

/* Waits until the block holding end is complete, waking the waiters whose
 * LSN is reached one at a time meanwhile (see wal_log_write).
 * Those it did not get to stay on *to_wake. */
static void wal_flush_hole_wait(WalManager *wm, uint64_t end, WalGcWaiter **wake_list)
{
    uint64_t          block_start = end & ~(uint64_t)(WAL_BLOCK_SIZE - 1);
    _Atomic uint16_t *counter     = wal_block_counter(wm->fs_counters, wm->ctr_mask, block_start);
    wal_gcl_find_waiters_reached(&wm->write_lock, wake_list);

    for (uint32_t tries = 0; ; ) {
        /* Complete when full, or when every byte reserved in the block so
         * far is copied. The counter is read before current_pos, as in the
         * scan, so a hole cannot hide. */
        uint16_t done = atomic_load_explicit(counter, memory_order_acquire);
        if (done == WAL_BLOCK_SIZE) return;
        uint64_t reserved_lsn = wal_byte_to_lsn(atomic_load_explicit(&wm->current_pos, memory_order_acquire));
        if (done == reserved_lsn - block_start) return;

        if (*wake_list) wal_gcl_wake_waiters(wake_list, 1);   /* one wake, then check again */
        else          thread_wait(tries++);
    }
}

int wal_log_write(WalManager *wm, uint64_t target)
{
    WalGcWaiter *to_wake = NULL;
    int rc = MYDB_OK;

    for (;;) {
        WalScanResult r;
        if (wal_flush_write_chunk(wm, &r) != MYDB_OK) { rc = MYDB_ERR; break; }
        if (r.end_lsn >= target) break;
        /* A tail short of target would mean target is not reserved. */
        assert(!r.tail);
        /* A hole, or a stop after a whole ring (then the wait may return
         * at once). */
        wal_flush_hole_wait(wm, r.end_lsn, &to_wake);
    }

    wal_gcl_wake_waiters(&to_wake, 0);   /* all */
    wal_gcl_release(&wm->write_lock, atomic_load_explicit(&wm->write_lsn, memory_order_relaxed));
    return rc;
}

int wal_flush_space_wait(WalManager *wm, uint64_t target)
{
    while (atomic_load_explicit(&wm->write_lsn, memory_order_acquire) < target) {
        if (wal_gcl_acquire(&wm->write_lock, target) == WAL_GCL_ACQUIRED) {
            wal_gcl_set_pending(&wm->write_lock, target);
            if (wal_log_write(wm, target) != MYDB_OK) return MYDB_ERR;
        }
    }
    return MYDB_OK;
}

int wal_log_flush(WalManager *wm, uint64_t lsn)
{
    if (!wm) return MYDB_ERR;
    if (wal_gcl_acquire(&wm->flush_lock, lsn) != WAL_GCL_ACQUIRED) return MYDB_OK;   /* another leader reached lsn */
    wal_gcl_set_pending(&wm->flush_lock, lsn);

    /* The write part, through write_lock. */
    if (wal_flush_space_wait(wm, lsn) != MYDB_OK) {
        wal_gcl_release(&wm->flush_lock, atomic_load_explicit(&wm->flushed_lsn, memory_order_relaxed));
        return MYDB_ERR;
    }

    /* Sync everything written so far, not just up to lsn. The worker
     * cannot change flushed_lsn meanwhile: it only try-acquires the lock. */
    uint64_t target  = atomic_load_explicit(&wm->write_lsn, memory_order_acquire);
    uint64_t flushed = atomic_load_explicit(&wm->flushed_lsn, memory_order_relaxed);
    if (target <= flushed) {             /* nothing new to sync */
        wal_gcl_release(&wm->flush_lock, flushed);
        return MYDB_OK;
    }
    wal_gcl_set_pending(&wm->flush_lock, target);

    uint64_t seg = wal_lsn_segment(target - 1);
    int rc = wal_segment_pool_fdatasync_segment(&wm->pool, seg);

    /* The segments before seg are the worker's: wait until it has synced
     * them. It never waits for flush_lock, so holding it here is safe. */
    if (rc == MYDB_OK) rc = wal_worker_wait_synced(&wm->worker, seg << WAL_SEG_SHIFT);

    wal_gcl_release(&wm->flush_lock, rc == MYDB_OK ? target : flushed);
    return rc;
}
