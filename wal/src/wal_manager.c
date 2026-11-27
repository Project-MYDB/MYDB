#include "wal_manager.h"

#include <stdlib.h>
#include <string.h>

int wal_manager_init(WalManager *wm, const char *wal_dir, uint32_t partition_id)
{
    if (!wm || !wal_dir) return MYDB_ERR;
    wm->locks_up       = false;
    wm->worker.started = false;

    if (wal_segment_pool_init(&wm->pool, wal_dir, partition_id) != MYDB_OK)
        return MYDB_ERR;

    /* Find the end of the log and cut it there, as PostgreSQL's
     * StartupXLOG does on every start. A new log (no ACTIVE segment)
     * claims its first segment and ends at its start. */
    uint64_t end_lsn = 0;
    int rc = wal_segment_pool_tail_scan(&wm->pool, &wm->curr_slot, &end_lsn);
    if (rc == MYDB_OK) {
        rc = wal_segment_pool_truncate(&wm->pool, wm->curr_slot, end_lsn);
    } else if (rc == MYDB_ERR_NOT_FOUND) {
        rc = wal_segment_pool_claim_next(&wm->pool, &wm->curr_slot);
        end_lsn = wm->pool.slots[wm->curr_slot].header.segment_no << WAL_SEG_SHIFT;
    }
    if (rc != MYDB_OK) {
        wal_segment_pool_shutdown(&wm->pool);
        return MYDB_ERR;
    }

    /* Frames page aligned; counters cache-line aligned, 4 to a line. */
    size_t ring_bytes = (size_t)WAL_RING_FRAMES * WAL_PAGE_SIZE;
    size_t ctr_bytes  = (size_t)WAL_RING_FRAMES * WAL_FRAME_BLOCKS * sizeof(WalBlockCounter);
    wm->ring_buf    = aligned_alloc(WAL_PAGE_SIZE, ring_bytes);
    wm->fs_counters = aligned_alloc(64, ctr_bytes);
    if (!wm->ring_buf || !wm->fs_counters) {
        free(wm->ring_buf);
        free(wm->fs_counters);
        wm->ring_buf    = NULL;
        wm->fs_counters = NULL;
        wal_segment_pool_shutdown(&wm->pool);
        return MYDB_ERR;
    }
    /* Zeroed: no stale memory reaches a file, and counters start at 0. */
    memset(wm->ring_buf, 0, ring_bytes);
    memset(wm->fs_counters, 0, ctr_bytes);

    /* From here on a failure goes through wal_manager_shutdown. */
    if (wal_gcl_init(&wm->write_lock, &wm->write_lsn) != MYDB_OK) {
        free(wm->ring_buf);
        free(wm->fs_counters);
        wm->ring_buf    = NULL;
        wm->fs_counters = NULL;
        wal_segment_pool_shutdown(&wm->pool);
        return MYDB_ERR;
    }
    if (wal_gcl_init(&wm->flush_lock, &wm->flushed_lsn) != MYDB_OK) {
        wal_gcl_destroy(&wm->write_lock);
        free(wm->ring_buf);
        free(wm->fs_counters);
        wm->ring_buf    = NULL;
        wm->fs_counters = NULL;
        wal_segment_pool_shutdown(&wm->pool);
        return MYDB_ERR;
    }
    wm->locks_up = true;

    wm->ring_buf_size   = WAL_RING_FRAMES;
    wm->ring_frame_mask = WAL_RING_FRAMES - 1;
    wm->ctr_mask        = WAL_RING_FRAMES * WAL_FRAME_BLOCKS - 1;
    wm->partition_id    = partition_id;

    /* Resume at end_lsn (V2 §10.7). An end at a page's offset 24 resumes
     * at the page's start: the next copier writes and counts that header. */
    uint32_t off    = wal_lsn_offset(end_lsn);
    uint64_t resume = (off == WAL_PAGE_HEADER_SIZE) ? end_lsn - WAL_PAGE_HEADER_SIZE : end_lsn;
    off = wal_lsn_offset(resume);   /* 0, or past the header */

    /* An end inside a page: copy the page's bytes before it into the
     * ring frame, and count the end's block's share of them in its
     * counter, since no copier will. The scan starts at that block. */
    if (off != 0) {
        uint8_t *frame = wm->ring_buf + (size_t)((resume >> WAL_PAGE_SHIFT) & wm->ring_frame_mask) * WAL_PAGE_SIZE;
        if (wal_segment_pool_read_page(&wm->pool, wm->curr_slot, wal_lsn_page(resume) + 1, frame) != MYDB_OK) {
            wal_manager_shutdown(wm);
            return MYDB_ERR;
        }
        memset(frame + off, 0, WAL_PAGE_SIZE - off);
        atomic_store_explicit(wal_block_counter(wm->fs_counters, wm->ctr_mask, resume),
                              (uint16_t)(off & (WAL_BLOCK_SIZE - 1)), memory_order_relaxed);
    }
    wm->last_ctr_idx = (uint32_t)((resume >> WAL_BLOCK_SHIFT) & wm->ctr_mask);

    /* current_pos: record bytes before the end. write_lsn and flushed_lsn:
     * the resume point, synced by truncate. Relaxed: no other thread yet. */
    uint64_t bytes = (wal_lsn_offset(end_lsn) == 0)
                   ? (end_lsn >> WAL_PAGE_SHIFT) * WAL_PAGE_USABLE
                   : wal_lsn_to_byte(end_lsn);
    atomic_store_explicit(&wm->current_pos, bytes,  memory_order_relaxed);
    atomic_store_explicit(&wm->write_lsn,   resume, memory_order_relaxed);
    atomic_store_explicit(&wm->flushed_lsn, resume, memory_order_relaxed);

    /* The worker last: it uses the pool and flush_lock. Every segment
     * before the resume point's is synced already. */
    if (wal_worker_start(&wm->worker, &wm->pool, &wm->flush_lock,
                         wal_lsn_segment(resume) << WAL_SEG_SHIFT) != MYDB_OK) {
        wal_manager_shutdown(wm);
        return MYDB_ERR;
    }
    return MYDB_OK;
}

int wal_manager_shutdown(WalManager *wm)
{
    if (!wm) return MYDB_ERR;

    /* The worker first: it syncs what was requested, using the pool and
     * flush_lock. */
    int rc = wal_worker_stop(&wm->worker);
    if (wm->locks_up) {
        wal_gcl_destroy(&wm->write_lock);
        wal_gcl_destroy(&wm->flush_lock);
        wm->locks_up = false;
    }
    free(wm->ring_buf);
    free(wm->fs_counters);
    wm->ring_buf    = NULL;
    wm->fs_counters = NULL;

    wal_segment_pool_shutdown(&wm->pool);
    return rc;
}
