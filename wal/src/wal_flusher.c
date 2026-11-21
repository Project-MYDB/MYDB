#include "wal_flusher.h"
#include "wal_page.h"

#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Locks flush_mutex, raises flush_lsn to new_flush_lsn (only if it's
 * actually higher — defensive against any out-of-order caller),
 * broadcasts flush_cond, unlocks. Called once per drain cycle, after the
 * segment write(s) for that batch are confirmed durable. (Was
 * wal_ring_buffer_mark_flushed.) */
static void mark_flushed(WalFlusher *f, uint64_t new_flush_lsn)
{
    pthread_mutex_lock(&f->flush_mutex);
    if (new_flush_lsn > f->flush_lsn)
        f->flush_lsn = new_flush_lsn;
    pthread_cond_broadcast(&f->flush_cond);
    pthread_mutex_unlock(&f->flush_mutex);
}

static void *flusher_main(void *arg)
{
    WalFlusher *f = (WalFlusher *)arg;

    for (;;) {
        pthread_mutex_lock(&f->demand_mutex);
        if (!f->stop_requested) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_nsec += (long)WAL_FLUSHER_PERIODIC_MS * 1000000L;
            if (ts.tv_nsec >= 1000000000L) {
                ts.tv_sec  += 1;
                ts.tv_nsec -= 1000000000L;
            }
            /* Return value ignored on purpose: both a demand signal and
             * a timeout mean the same thing here — "go check for work." */
            pthread_cond_timedwait(&f->demand_cond, &f->demand_mutex, &ts);
        }
        int stop = f->stop_requested;
        pthread_mutex_unlock(&f->demand_mutex);

        if (stop) break;

        uint32_t num_frames = 0;
        if (wal_ring_buffer_snapshot(f->rb, f->scratch,
                                      WAL_RING_BUFFER_FRAMES, &num_frames,
                                      &f->buf_cursor) != MYDB_OK)
            continue;
        if (num_frames == 0) continue;

        int any_written = 0;
        for (uint32_t i = 0; i < num_frames; i++) {
            uint8_t *frame = f->scratch + (size_t)i * WAL_PAGE_SIZE;

            /* WalCursor.offset is uint16_t (design doc §7); wal_segment_
             * pool_write's offset param is uint32_t. Widen in, narrow
             * back — always < WAL_PAGE_SIZE (4096), safe either way. */
            uint32_t offset32 = f->seg_cursor.offset;
            int rc = wal_segment_pool_write(f->pool, f->worker, &f->seg_slot_index,
                                             &f->seg_cursor.page_no, &offset32,
                                             frame, WAL_PAGE_SIZE);
            f->seg_cursor.offset = (uint16_t)offset32;
            if (rc != MYDB_OK) break;   /* stop this cycle's batch — see wal_flusher.h deferred notes */

            any_written = 1;
            /* This write may have auto-rolled to a new segment (Phase 2's
             * wal_segment_pool_write claims the next one internally) —
             * refresh seg_no from the pool's own record rather than
             * assuming it's unchanged. */
            f->seg_no = f->pool->slots[f->seg_slot_index].header.segment_no;

            /* Everything up to this page's end is now on disk. (Was the
             * page's end_lsn; with the LSN a position, the end is the
             * page's own start_lsn + WAL_PAGE_SIZE.) */
            WalPageHeader hdr;
            if (wal_page_header_deserialize(frame, &hdr) == MYDB_OK)
                f->seg_cursor.lsn = hdr.start_lsn + WAL_PAGE_SIZE;
        }

        if (any_written)
            mark_flushed(f, f->seg_cursor.lsn);
    }

    return NULL;
}

int wal_flusher_start(WalFlusher *f, WalRingBuffer *rb, WalSegmentPool *pool,
                       uint32_t seg_slot_index, WalWorker *worker)
{
    if (!f || !rb || !pool || seg_slot_index >= WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;

    memset(f, 0, sizeof(*f));
    f->rb     = rb;
    f->pool   = pool;
    f->worker = worker;

    f->seg_slot_index    = seg_slot_index;
    f->seg_no            = pool->slots[seg_slot_index].header.segment_no;
    /* The ring must start at this segment's first record: page 1, where
     * seg_cursor starts. No resume into a half-written segment yet. */
    uint64_t start = wal_segment_first_lsn(f->seg_no);
    pthread_mutex_lock(&rb->lock);
    int aligned = (rb->current_lsn == start);
    pthread_mutex_unlock(&rb->lock);
    if (!aligned) return MYDB_ERR;

    f->seg_cursor.lsn     = start;
    f->seg_cursor.page_no = 1;   /* first data page — page 0 is the segment's own header */
    f->seg_cursor.offset  = 0;
    f->buf_cursor.lsn     = 0;
    f->buf_cursor.page_no = (uint32_t)(wal_lsn_data_page(start) % WAL_RING_BUFFER_FRAMES);
    f->buf_cursor.offset  = 0;   /* the ring drains from its own drained_lsn */
    f->flush_lsn          = start;

    f->scratch = malloc(WAL_RING_BUFFER_SIZE);
    if (!f->scratch) return MYDB_ERR;

    if (pthread_mutex_init(&f->demand_mutex, NULL) != 0) {
        free(f->scratch);
        f->scratch = NULL;
        return MYDB_ERR;
    }
    if (pthread_cond_init(&f->demand_cond, NULL) != 0) {
        pthread_mutex_destroy(&f->demand_mutex);
        free(f->scratch);
        f->scratch = NULL;
        return MYDB_ERR;
    }
    if (pthread_mutex_init(&f->flush_mutex, NULL) != 0) {
        pthread_cond_destroy(&f->demand_cond);
        pthread_mutex_destroy(&f->demand_mutex);
        free(f->scratch);
        f->scratch = NULL;
        return MYDB_ERR;
    }
    if (pthread_cond_init(&f->flush_cond, NULL) != 0) {
        pthread_mutex_destroy(&f->flush_mutex);
        pthread_cond_destroy(&f->demand_cond);
        pthread_mutex_destroy(&f->demand_mutex);
        free(f->scratch);
        f->scratch = NULL;
        return MYDB_ERR;
    }

    f->stop_requested = 0;
    if (pthread_create(&f->thread, NULL, flusher_main, f) != 0) {
        pthread_cond_destroy(&f->flush_cond);
        pthread_mutex_destroy(&f->flush_mutex);
        pthread_cond_destroy(&f->demand_cond);
        pthread_mutex_destroy(&f->demand_mutex);
        free(f->scratch);
        f->scratch = NULL;
        return MYDB_ERR;
    }

    f->started = 1;
    return MYDB_OK;
}

int wal_flusher_stop(WalFlusher *f)
{
    if (!f || !f->started) return MYDB_OK;   /* no-op: never started / already stopped */

    pthread_mutex_lock(&f->demand_mutex);
    f->stop_requested = 1;
    pthread_cond_signal(&f->demand_cond);
    pthread_mutex_unlock(&f->demand_mutex);

    pthread_join(f->thread, NULL);

    pthread_cond_destroy(&f->flush_cond);
    pthread_mutex_destroy(&f->flush_mutex);
    pthread_cond_destroy(&f->demand_cond);
    pthread_mutex_destroy(&f->demand_mutex);
    free(f->scratch);
    f->scratch = NULL;
    f->started = 0;

    return MYDB_OK;
}

void wal_flusher_signal(WalFlusher *f)
{
    if (!f || !f->started) return;

    pthread_mutex_lock(&f->demand_mutex);
    pthread_cond_signal(&f->demand_cond);
    pthread_mutex_unlock(&f->demand_mutex);
}
int wal_flusher_wait_flushed(WalFlusher *f, uint64_t lsn)
{
    if (!f || !f->started) return MYDB_ERR;

    pthread_mutex_lock(&f->flush_mutex);
    while (f->flush_lsn < lsn)
        pthread_cond_wait(&f->flush_cond, &f->flush_mutex);
    pthread_mutex_unlock(&f->flush_mutex);

    return MYDB_OK;
}

uint64_t wal_flusher_flushed_lsn(WalFlusher *f)
{
    if (!f || !f->started) return 0;

    pthread_mutex_lock(&f->flush_mutex);
    uint64_t v = f->flush_lsn;
    pthread_mutex_unlock(&f->flush_mutex);
    return v;
}
