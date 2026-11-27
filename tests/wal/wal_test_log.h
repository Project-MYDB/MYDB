#ifndef WAL_TEST_LOG_H
#define WAL_TEST_LOG_H

/*
 * wal_test_log.h — test-only: hand-built WAL segments, shared by
 * test_wal_segment_pool (tail_scan, truncate) and test_wal_manager
 * (resume at start-up). Lays records out the way the insert path writes
 * them, independently of the production code that reads them back.
 */

#include <stdlib.h>
#include <string.h>

#include "common.h"
#include "wal_page.h"
#include "wal_types.h"
#include "wal_segment_pool.h"

/* ------------------------------------------------------------------
 * A hand-built segment for the tail_scan tests: a 16 MB image of one
 * segment's log, laid out the way the insert path writes it (a 24 B page
 * header at every page: start_lsn, continuation_len, INCOMING flag;
 * records back to back), then written page by page into its slot.
 * ------------------------------------------------------------------ */
typedef struct {
    uint8_t  *img;        /* TL_SEGS segments of log, from seg_start */
    uint64_t  seg_start;  /* first segment << WAL_SEG_SHIFT          */
    uint64_t  pos;        /* just past the last byte laid out        */
} TestLog;

#define TL_SEGS 2         /* a record can cross into the next segment */

static inline void tl_page_header(TestLog *tl, uint64_t base, uint32_t cont)
{
    WalPageHeader h;
    memset(&h, 0, sizeof(h));
    h.id.file_type      = FILETYPE_WAL_PAGE;
    h.start_lsn         = base;
    h.continuation_len  = (uint16_t)cont;
    h.flags             = cont ? WAL_PAGE_INCOMING_CONTINUATION : 0;
    wal_page_header_serialize(&h, tl->img + (base - tl->seg_start));
}

/* lead > 0: page 0 starts with lead bytes continuing a record from the
 * previous segment. */
static inline void tl_begin(TestLog *tl, uint64_t seg, uint32_t lead)
{
    tl->img       = calloc(TL_SEGS, WAL_SEG_SIZE);
    tl->seg_start = seg << WAL_SEG_SHIFT;
    tl->pos       = tl->seg_start;
    if (lead) {
        tl_page_header(tl, tl->seg_start, lead);
        memset(tl->img + WAL_PAGE_HEADER_SIZE, 0xEE, lead);
        tl->pos = tl->seg_start + WAL_PAGE_HEADER_SIZE + lead;
    }
}

/* Appends one record with body_len body bytes; returns its LSN. */
static inline uint64_t tl_record(TestLog *tl, uint32_t body_len, uint8_t fill)
{
    uint32_t total = WAL_RECORD_HEADER_SIZE + body_len;
    uint64_t lsn   = (wal_lsn_offset(tl->pos) == 0) ? tl->pos + WAL_PAGE_HEADER_SIZE : tl->pos;

    uint8_t *body = malloc(body_len ? body_len : 1);
    memset(body, fill, body_len);
    uint8_t *rec = malloc(total);
    WalRecordHeader h;
    memset(&h, 0, sizeof(h));
    h.lsn = lsn; h.txn_id = 1; h.total_len = total; h.rec_type = WAL_REC_INSERT;
    wal_record_header_serialize(&h, body, body_len, rec);

    uint32_t done = 0;
    while (done < total) {
        if (wal_lsn_offset(tl->pos) == 0) {   /* a new page: its header first */
            uint32_t left = total - done;
            tl_page_header(tl, tl->pos, done == 0 ? 0 : (left < WAL_PAGE_USABLE ? left : WAL_PAGE_USABLE));
            tl->pos += WAL_PAGE_HEADER_SIZE;
        }
        uint32_t room = WAL_PAGE_SIZE - wal_lsn_offset(tl->pos);
        uint32_t c    = (total - done < room) ? total - done : room;
        memcpy(tl->img + (tl->pos - tl->seg_start), rec + done, c);
        done    += c;
        tl->pos += c;
    }
    free(rec);
    free(body);
    return lsn;
}

/* Writes every page laid out so far, the first segment's pages into
 * slot and the next segment's into next_slot (file page = log page + 1
 * within its segment). */
static inline void tl_write2(TestLog *tl, WalSegmentPool *pool, uint32_t slot, uint32_t next_slot)
{
    uint64_t used  = tl->pos - tl->seg_start;
    uint32_t pages = (uint32_t)((used + WAL_PAGE_SIZE - 1) / WAL_PAGE_SIZE);
    for (uint32_t i = 0; i < pages; i++)
        wal_segment_pool_write_page(pool, i < WAL_SEG_PAGES ? slot : next_slot,
                                    i % WAL_SEG_PAGES + 1, tl->img + (size_t)i * WAL_PAGE_SIZE);
}

static inline void tl_write(TestLog *tl, WalSegmentPool *pool, uint32_t slot)
{
    tl_write2(tl, pool, slot, slot);
}

/* Record bytes laid out before tl->pos (an end on a page end counts the
 * whole page). */
static inline uint64_t tl_bytes(const TestLog *tl)
{
    uint32_t off = wal_lsn_offset(tl->pos);
    return (tl->pos >> WAL_PAGE_SHIFT) * WAL_PAGE_USABLE + (off ? off - WAL_PAGE_HEADER_SIZE : 0);
}

/* Lays records into segment 0 so that exactly `room` record bytes are
 * left before the boundary: 4000-byte records while more than 8000 are
 * left to fill, then two records splitting the rest (each >= 2000). */
static inline void tl_fill_segment_0(TestLog *tl, uint32_t room)
{
    const uint64_t seg_bytes = (uint64_t)WAL_SEG_PAGES * WAL_PAGE_USABLE;   /* 4,169,728 */
    uint64_t d = seg_bytes - room - tl_bytes(tl);
    while (d > 8000) {
        tl_record(tl, 4000 - WAL_RECORD_HEADER_SIZE, 0x5A);
        d -= 4000;
    }
    tl_record(tl, (uint32_t)(d / 2 - WAL_RECORD_HEADER_SIZE), 0x5B);
    tl_record(tl, (uint32_t)(d - d / 2 - WAL_RECORD_HEADER_SIZE), 0x5C);
}

#endif /* WAL_TEST_LOG_H */
