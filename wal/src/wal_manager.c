#include "wal_manager.h"

#include <stdlib.h>
#include <string.h>

int wal_manager_init(WalManager *wm, const char *wal_dir, uint32_t partition_id)
{
    if (!wm || !wal_dir) return MYDB_ERR;

    if (wal_worker_start(&wm->worker) != MYDB_OK)
        return MYDB_ERR;

    if (wal_segment_pool_init(&wm->pool, wal_dir, partition_id) != MYDB_OK) {
        wal_worker_stop(&wm->worker);
        return MYDB_ERR;
    }

    /* Every start finds the end of the log first, after a clean shutdown
     * or a crash alike (V2 §4; PostgreSQL's StartupXLOG). An existing log
     * is cut at that end (tail_scan + truncate); only a brand-new log,
     * with no ACTIVE segment, gets a fresh segment, which is the same as
     * an end at that segment's start: nothing of it written yet. (init
     * used to claim a fresh segment on every start.) */
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
        wal_worker_stop(&wm->worker);
        return MYDB_ERR;
    }

    /* The ring is written to disk page by page, so frames are page
     * aligned. The counters are aligned to a cache line: 4 counters (2KB
     * of ring) per line only holds if the array starts on one. Both
     * sizes are multiples of their alignment, as aligned_alloc needs. */
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
        wal_worker_stop(&wm->worker);
        return MYDB_ERR;
    }
    /* Zeroed, so no uninitialised memory ever reaches a segment file in
     * the unfilled tail of a page; counters zero-based (V2 §10.4). */
    memset(wm->ring_buf, 0, ring_bytes);
    memset(wm->fs_counters, 0, ctr_bytes);

    wm->ring_buf_size   = WAL_RING_FRAMES;
    wm->ring_frame_mask = WAL_RING_FRAMES - 1;
    wm->ctr_mask        = WAL_RING_FRAMES * WAL_FRAME_BLOCKS - 1;
    wm->partition_id    = partition_id;

    /* Resume at end_lsn (V2 §10.7). An end at a page's offset 24 (its
     * header written, no record yet) is treated as that page's start: the
     * next copier writes the header itself and counts its 24 bytes in
     * block 0 (V2 §10.3), so they must not be seeded here too. */
    uint32_t off    = wal_lsn_offset(end_lsn);
    uint64_t resume = (off == WAL_PAGE_HEADER_SIZE) ? end_lsn - WAL_PAGE_HEADER_SIZE : end_lsn;
    off = wal_lsn_offset(resume);   /* 0, or past the header */

    /* Otherwise the page's bytes before the end are already on disk and no
     * copier will write them again: copy them into the page's ring frame,
     * so the page is whole when the write step writes it again, and seed
     * the counter of the block holding the end with the bytes of that
     * block already there (header bytes included in block 0). The earlier
     * blocks are never scanned: the write step starts at the end's block,
     * and zeroes them with their line once it passes it. */
    if (off != 0) {
        uint8_t *frame = wm->ring_buf + (size_t)((resume >> WAL_PAGE_SHIFT) & wm->ring_frame_mask) * WAL_PAGE_SIZE;
        if (wal_segment_pool_read_page(&wm->pool, wm->curr_slot, wal_lsn_page(resume) + 1, frame) != MYDB_OK) {
            wal_manager_shutdown(wm);
            return MYDB_ERR;
        }
        memset(frame + off, 0, WAL_PAGE_SIZE - off);   /* already zero on disk (truncate) */
        atomic_store_explicit(wal_block_counter(wm->fs_counters, wm->ctr_mask, resume),
                              (uint16_t)(off & (WAL_BLOCK_SIZE - 1)), memory_order_relaxed);
    }
    wm->last_ctr_idx = (uint32_t)((resume >> WAL_BLOCK_SHIFT) & wm->ctr_mask);

    /* current_pos counts record bytes before the end; an end on a page
     * end counts the whole page. write_lsn: the log is on disk up to the
     * resume point (truncate synced it). flushed_lsn, when it exists with
     * wal_flush, starts at the same place. No thread uses wm yet, so
     * relaxed stores are enough. */
    uint64_t bytes = (wal_lsn_offset(end_lsn) == 0)
                   ? (end_lsn >> WAL_PAGE_SHIFT) * WAL_PAGE_USABLE
                   : wal_lsn_to_byte(end_lsn);
    atomic_store_explicit(&wm->current_pos, bytes,  memory_order_relaxed);
    atomic_store_explicit(&wm->write_lsn,   resume, memory_order_relaxed);

    return MYDB_OK;
}

int wal_manager_shutdown(WalManager *wm)
{
    if (!wm) return MYDB_ERR;

    free(wm->ring_buf);
    free(wm->fs_counters);
    wm->ring_buf    = NULL;
    wm->fs_counters = NULL;

    wal_segment_pool_shutdown(&wm->pool);
    wal_worker_stop(&wm->worker);

    return MYDB_OK;
}
