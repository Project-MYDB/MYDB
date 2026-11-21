#include "wal_manager.h"

int wal_manager_init(WalManager *wm, const char *wal_dir, uint32_t partition_id)
{
    if (!wm || !wal_dir) return MYDB_ERR;

    if (wal_worker_start(&wm->worker) != MYDB_OK)
        return MYDB_ERR;

    if (wal_segment_pool_init(&wm->pool, wal_dir, partition_id) != MYDB_OK) {
        wal_worker_stop(&wm->worker);
        return MYDB_ERR;
    }

    uint32_t slot;
    if (wal_segment_pool_claim_next(&wm->pool, &slot) != MYDB_OK) {
        wal_segment_pool_shutdown(&wm->pool);
        wal_worker_stop(&wm->worker);
        return MYDB_ERR;
    }

    /* The ring starts where the claimed segment's records start. */
    uint64_t seg = wm->pool.slots[slot].header.segment_no;
    if (wal_ring_buffer_init(&wm->rb, wal_segment_first_lsn(seg)) != MYDB_OK) {
        wal_segment_pool_shutdown(&wm->pool);
        wal_worker_stop(&wm->worker);
        return MYDB_ERR;
    }

    if (wal_flusher_start(&wm->flusher, &wm->rb, &wm->pool, slot, &wm->worker) != MYDB_OK) {
        wal_segment_pool_shutdown(&wm->pool);
        wal_ring_buffer_shutdown(&wm->rb);
        wal_worker_stop(&wm->worker);
        return MYDB_ERR;
    }

    return MYDB_OK;
}

int wal_manager_shutdown(WalManager *wm)
{
    if (!wm) return MYDB_ERR;

    wal_flusher_stop(&wm->flusher);
    wal_segment_pool_shutdown(&wm->pool);
    wal_ring_buffer_shutdown(&wm->rb);
    wal_worker_stop(&wm->worker);

    return MYDB_OK;
}

int wal_manager_append(WalManager *wm, WalRecordHeader *hdr,
                       const void *body, size_t body_len,
                       uint64_t *out_lsn, uint64_t *out_end_lsn)
{
    if (!wm) return MYDB_ERR;

    for (;;) {
        int rc = wal_ring_buffer_append(&wm->rb, hdr, body, body_len, out_lsn, out_end_lsn);
        if (rc != MYDB_ERR_FULL) return rc;

        /* Ring full: the Flusher frees frames by copying them out. Wake it
         * and wait until this record would fit; another appender may get
         * there first, hence the loop. */
        wal_flusher_signal(&wm->flusher);
        wal_ring_buffer_append_wait(&wm->rb, WAL_RECORD_HEADER_SIZE + body_len);
    }
}

int wal_manager_wait_flushed(WalManager *wm, uint64_t lsn)
{
    if (!wm) return MYDB_ERR;
    wal_flusher_signal(&wm->flusher);
    return wal_flusher_wait_flushed(&wm->flusher, lsn);
}
