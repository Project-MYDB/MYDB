#include "large_wal/large_wal_writer.h"
#include "large_wal/large_wal_page.h"
#include "normal_wal/wal_types.h"

#include <string.h>

/* ------------------------------------------------------------------
 * Blob walking — records are self-describing, so the writer discovers
 * the batch's shape from the bytes themselves rather than from a
 * caller-supplied descriptor array. Each WalRecordHeader carries its own
 * total_len (self-terminating, the same property impl doc §10.4 leaned
 * on to drop the footer), lsn and rec_type, which is everything an
 * index entry needs.
 * ------------------------------------------------------------------ */

typedef struct {
    uint32_t pos;        /* byte offset of this record within the blob */
    uint32_t len;        /* total_len: header + body                   */
    uint64_t lsn;
    uint8_t  rec_type;
} RecRef;

#define LARGE_WAL_MAX_RECS_PER_BATCH 256

/* Walks content[0..total_size), filling out[] and *out_n. Rejects a
 * truncated header, an implausible total_len, an overrun, or a walk that
 * doesn't land exactly on total_size — any of which mean the blob isn't
 * the run of records it claims to be. */
static int walk_records(const uint8_t *content, uint32_t total_size,
                         RecRef *out, uint32_t out_cap, uint32_t *out_n)
{
    uint32_t n = 0, pos = 0;

    while (pos < total_size) {
        if (n >= out_cap) return MYDB_ERR;
        if (total_size - pos < WAL_RECORD_HEADER_SIZE) return MYDB_ERR;

        uint32_t total_len;
        uint64_t lsn;
        memcpy(&lsn,       content + pos + 0,  8);
        memcpy(&total_len, content + pos + 16, 4);

        if (total_len < WAL_RECORD_HEADER_SIZE) return MYDB_ERR;
        if (total_len > total_size - pos)       return MYDB_ERR;

        out[n].pos      = pos;
        out[n].len      = total_len;
        out[n].lsn      = lsn;
        out[n].rec_type = content[pos + 36];
        n++;
        pos += total_len;
    }

    if (pos != total_size || n == 0) return MYDB_ERR;
    *out_n = n;
    return MYDB_OK;
}

static uint32_t pages_for(uint32_t start_offset, uint32_t bytes)
{
    return (start_offset + bytes + LARGE_WAL_PAGE_USABLE - 1) / LARGE_WAL_PAGE_USABLE;
}

/* ------------------------------------------------------------------
 * Layout planning.
 *
 * The writer already tracks its own cursor (cur_slot_index/cur_page_no/
 * cur_offset) and a segment's page count is a fixed 128, so where every
 * record lands — including whether the batch crosses a segment boundary
 * and exactly which page it crosses on — is pure arithmetic known
 * before a single byte is copied. Nothing needs discovering afterwards.
 *
 * A batch may span segments freely; a record may not, because
 * LargeWalIndexEntry names one segment_no. So when a record won't fit
 * what's left of the current segment, the planner pads out the
 * remainder of that segment and starts the record on page 1 of the
 * next. The next segment_no is knowable too: claim_next hands out
 * pool->next_segment_no and increments it.
 *
 * That leaves segment_pool_write's own rollover doing exactly what it
 * was designed to do — it fills the last page, closes the segment and
 * claims the next — while the writer, having predicted all of it, still
 * knows where everything went.
 * ------------------------------------------------------------------ */

typedef struct {
    uint32_t buf_page;    /* page index within the buffer where this record starts */
    uint32_t write_pos;   /* content-area offset within that page */
    uint64_t seg_no;      /* segment this record lands in */
    uint32_t seg_page;    /* that segment's own page number */
    uint8_t  page_count;
} RecPlan;

/* Per-buffer-page header state, accumulated while packing. */
typedef struct {
    uint64_t start_lsn;
    uint64_t end_lsn;
    uint32_t data_len;
    uint8_t  flags;
    uint8_t  have_lsn;
} PagePlan;

#define LARGE_WAL_MAX_BATCH_PAGES 256

static int plan_layout(LargeWalWriter *writer, const RecRef *recs, uint32_t n,
                        RecPlan *plan, uint32_t *out_buf_pages,
                        uint32_t *out_end_seg_page, uint32_t *out_end_write_pos)
{
    uint32_t buf_page   = 0;
    uint32_t write_pos  = writer->cur_offset;
    uint32_t seg_page   = writer->cur_page_no;
    uint32_t pages_left = LARGE_WAL_SEGMENT_PAGES_PER_FILE - writer->cur_page_no;
    uint64_t seg_no     = writer->pool->slots[writer->cur_slot_index].header.segment_no;
    uint64_t next_seg   = writer->pool->next_segment_no;

    for (uint32_t i = 0; i < n; i++) {
        uint32_t need = pages_for(write_pos, recs[i].len);

        if (need > pages_left) {
            /* Doesn't fit this segment. Pad out its remaining pages and
             * begin the record at the head of the next one — this is
             * what keeps a record segment-local. */
            buf_page  += pages_left;
            write_pos  = 0;
            seg_page   = 1;
            pages_left = LARGE_WAL_SEGMENT_PAGES_PER_FILE - 1;
            seg_no     = next_seg++;

            need = pages_for(0, recs[i].len);
            if (need > pages_left) return MYDB_ERR;   /* bigger than a whole segment */
        }

        plan[i].buf_page   = buf_page;
        plan[i].write_pos  = write_pos;
        plan[i].seg_no     = seg_no;
        plan[i].seg_page   = seg_page;
        plan[i].page_count = (uint8_t)need;

        uint32_t end   = write_pos + recs[i].len;
        uint32_t full  = end / LARGE_WAL_PAGE_USABLE;
        buf_page   += full;
        seg_page   += full;
        pages_left -= full;
        write_pos   = end % LARGE_WAL_PAGE_USABLE;
    }

    uint32_t buf_pages = buf_page + (write_pos > 0 ? 1 : 0);
    if (buf_pages == 0 || buf_pages > LARGE_WAL_MAX_BATCH_PAGES) return MYDB_ERR;

    *out_buf_pages     = buf_pages;
    *out_end_seg_page  = seg_page;
    *out_end_write_pos = write_pos;
    return MYDB_OK;
}

/* ------------------------------------------------------------------
 * Packing — lays the batch out in the buffer exactly as planned, so the
 * buffer becomes a byte-for-byte image of the pages about to be written.
 *
 * A page here can hold the tail of one record, several whole records,
 * and the head of another, so its start_lsn and end_lsn genuinely
 * differ — which is why the shared WalPageHeader carries the pair
 * rather than the single content_lsn the old LargeWalPageHeader had.
 * ------------------------------------------------------------------ */

static void pack_batch(LargeWalWriter *writer, const uint8_t *content,
                        const RecRef *recs, const RecPlan *plan, uint32_t n,
                        uint32_t buf_pages, LargeWalIndexEntry *out_entries)
{
    PagePlan pp[LARGE_WAL_MAX_BATCH_PAGES];
    memset(pp, 0, sizeof(PagePlan) * buf_pages);

    /* Buffer page 0 may be a partially-filled page we read back — keep
     * its existing start_lsn/flags and count the bytes already on it. */
    if (writer->cur_offset > 0) {
        WalPageHeader existing;
        if (large_wal_page_header_deserialize(writer->buf.buf, &existing) == MYDB_OK) {
            pp[0].start_lsn = existing.start_lsn;
            pp[0].flags     = existing.flags;
            pp[0].have_lsn  = 1;
        }
        pp[0].data_len = writer->cur_offset;
    }

    uint64_t last_lsn = 0;

    for (uint32_t i = 0; i < n; i++) {
        const RecRef  *rec = &recs[i];
        const RecPlan *pl  = &plan[i];

        LargeWalIndexEntry *e = &out_entries[i];
        memset(e, 0, sizeof(*e));
        e->content_lsn   = rec->lsn;
        e->rec_type      = rec->rec_type;
        e->segment_no    = pl->seg_no;
        e->start_page_no = pl->seg_page;
        e->offset        = pl->write_pos;
        e->page_count    = pl->page_count;
        e->total_size    = rec->len;

        /* A record that won't finish on the page it starts on must say
         * so in its own header. The caller's blob is const, so patch a
         * 44-byte copy and pack that in place of the original's header. */
        uint8_t patched[WAL_RECORD_HEADER_SIZE];
        int     use_patched = 0;
        if (pl->write_pos + rec->len > LARGE_WAL_PAGE_USABLE) {
            memcpy(patched, content + rec->pos, WAL_RECORD_HEADER_SIZE);
            wal_record_header_patch_flags(patched,
                                           content + rec->pos + WAL_RECORD_HEADER_SIZE,
                                           rec->len - WAL_RECORD_HEADER_SIZE,
                                           WAL_RECORD_FLAG_CONTINUES_ON_NEW_PAGE);
            use_patched = 1;
        }

        uint32_t page      = pl->buf_page;
        uint32_t write_pos = pl->write_pos;
        uint32_t copied    = 0;

        while (copied < rec->len) {
            uint32_t room  = LARGE_WAL_PAGE_USABLE - write_pos;
            uint32_t chunk = (rec->len - copied < room) ? rec->len - copied : room;

            uint8_t *dst = writer->buf.buf + (size_t)page * PAGE_SIZE
                          + LARGE_WAL_PAGE_HEADER_ON_DISK_SIZE + write_pos;

            for (uint32_t k = 0; k < chunk; k++) {
                uint32_t at = copied + k;
                dst[k] = (use_patched && at < WAL_RECORD_HEADER_SIZE)
                         ? patched[at]
                         : content[rec->pos + at];
            }

            if (!pp[page].have_lsn) {
                pp[page].start_lsn = rec->lsn;
                pp[page].have_lsn  = 1;
            }
            pp[page].end_lsn  = rec->lsn;
            pp[page].data_len = write_pos + chunk;
            if (page > pl->buf_page)
                pp[page].flags |= WAL_PAGE_FLAG_INCOMING_CONTINUATION;

            copied    += chunk;
            write_pos += chunk;

            if (write_pos == LARGE_WAL_PAGE_USABLE) {
                if (copied < rec->len)          /* this record runs on */
                    pp[page].flags |= WAL_PAGE_FLAG_OUTGOING_CONTINUATION;
                page++;
                write_pos = 0;
            }
        }

        last_lsn = rec->lsn;
    }

    /* Stamp every page, padding pages included. A padding page (one the
     * planner skipped to keep a record out of a segment's tail) is a
     * real, valid, empty page: data_len 0, carrying the last LSN so the
     * segment's own end_lsn still reads sensibly when write() finalizes
     * it from the last page's header. */
    for (uint32_t i = 0; i < buf_pages; i++) {
        WalPageHeader hdr;
        memset(&hdr, 0, sizeof(hdr));
        hdr.id.file_type = FILETYPE_LARGE_WAL_PAGE;
        hdr.start_lsn    = pp[i].have_lsn ? pp[i].start_lsn : last_lsn;
        hdr.end_lsn      = pp[i].have_lsn ? pp[i].end_lsn   : last_lsn;
        hdr.data_len     = (uint16_t)pp[i].data_len;
        hdr.flags        = pp[i].flags;
        large_wal_page_header_serialize(&hdr, writer->buf.buf + (size_t)i * PAGE_SIZE);
    }
}

/* ------------------------------------------------------------------
 * do_write — walk the blob, plan the layout, pack it, write it once.
 *
 * Segment rollover is left to large_wal_segment_pool_write, which
 * already does it correctly: it fills the segment's last page, reads
 * that page's end_lsn (which we stamped) into mark_done, and claims the
 * next segment — which registers itself. The planner predicted all of
 * that, so the index entries are right without anything being read back.
 * ------------------------------------------------------------------ */

static int do_write(LargeWalWriter *writer, const uint8_t *content, uint32_t total_size,
                     LargeWalIndexEntry *out_entries, uint32_t out_cap, uint32_t *out_count)
{
    *out_count = 0;

    RecRef   recs[LARGE_WAL_MAX_RECS_PER_BATCH];
    RecPlan  plan[LARGE_WAL_MAX_RECS_PER_BATCH];
    uint32_t n = 0;

    if (walk_records(content, total_size, recs,
                      out_cap < LARGE_WAL_MAX_RECS_PER_BATCH ? out_cap : LARGE_WAL_MAX_RECS_PER_BATCH,
                      &n) != MYDB_OK)
        return MYDB_ERR;

    uint32_t buf_pages = 0, end_seg_page = 0, end_write_pos = 0;
    if (plan_layout(writer, recs, n, plan, &buf_pages, &end_seg_page, &end_write_pos) != MYDB_OK)
        return MYDB_ERR;

    /* Size the buffer by pages: an exact multiple of the usable size
     * makes acquire's own ceil() land on precisely buf_pages. */
    if (large_wal_buffer_acquire(&writer->buf, buf_pages * LARGE_WAL_PAGE_USABLE) != MYDB_OK)
        return MYDB_ERR;

    /* Seed page 0. Resuming a partially-filled page means its existing
     * bytes and header have to survive, so read them back (a page-cache
     * hit) instead of zeroing over them. */
    if (writer->cur_offset > 0) {
        if (large_wal_segment_pool_read_page(writer->pool, writer->cur_slot_index,
                                              writer->cur_page_no, writer->buf.buf) != MYDB_OK) {
            large_wal_buffer_release(&writer->buf);
            return MYDB_ERR;
        }
        memset(writer->buf.buf + PAGE_SIZE, 0, (size_t)(buf_pages - 1) * PAGE_SIZE);
    } else {
        memset(writer->buf.buf, 0, (size_t)buf_pages * PAGE_SIZE);
    }

    pack_batch(writer, content, recs, plan, n, buf_pages, out_entries);

    uint32_t slot    = writer->cur_slot_index;
    uint32_t page_no = writer->cur_page_no;
    uint32_t offset  = 0;
    int rc = large_wal_segment_pool_write(writer->pool, writer->worker, &slot, &page_no, &offset,
                                           writer->buf.buf, (size_t)buf_pages * PAGE_SIZE);
    large_wal_buffer_release(&writer->buf);
    if (rc != MYDB_OK) return rc;

    /* Cursor: the plan already says where we ended up. Take the slot
     * from write() (it may have rolled, possibly more than once), and
     * the position from the plan — write()'s own page_no/offset report
     * a page boundary, losing the sub-page position tight packing needs. */
    writer->cur_slot_index = slot;
    if (end_seg_page >= LARGE_WAL_SEGMENT_PAGES_PER_FILE) {
        /* The batch ended exactly on the segment's last page, so write()
         * rolled after writing it — we resume at the head of the fresh
         * segment it claimed. */
        writer->cur_page_no = 1;
        writer->cur_offset  = 0;
    } else {
        writer->cur_page_no = end_seg_page;
        writer->cur_offset  = end_write_pos;
    }
    writer->cur_segment_last_lsn = recs[n - 1].lsn;

    /* Index only now: an entry is a promise the bytes are on disk, so
     * nothing is inserted until write() has confirmed them. (The reverse
     * failure is benign — durable bytes with no entry are unreachable.) */
    for (uint32_t i = 0; i < n; i++) {
        if (large_wal_index_insert(writer->idx, &out_entries[i]) != MYDB_OK) return MYDB_ERR;
        (*out_count)++;
    }

    return large_wal_state_advance(writer->state, recs[n - 1].lsn);
}

/* ------------------------------------------------------------------
 * Thread loop
 * ------------------------------------------------------------------ */

static void *writer_main(void *arg)
{
    LargeWalWriter *writer = (LargeWalWriter *)arg;

    for (;;) {
        pthread_mutex_lock(&writer->lock);
        while (!writer->request_pending && !writer->stop_requested)
            pthread_cond_wait(&writer->cond, &writer->lock);

        if (writer->stop_requested && !writer->request_pending) {
            pthread_mutex_unlock(&writer->lock);
            break;
        }

        const uint8_t      *content     = writer->req_content;
        uint32_t             total_size  = writer->req_total_size;
        LargeWalIndexEntry  *out_entries = writer->req_out_entries;
        uint32_t             out_cap     = writer->req_out_cap;
        pthread_mutex_unlock(&writer->lock);

        uint32_t count = 0;
        int rc = do_write(writer, content, total_size, out_entries, out_cap, &count);

        pthread_mutex_lock(&writer->lock);
        writer->request_pending = 0;
        writer->request_done    = 1;
        writer->last_result      = rc;
        writer->req_out_count    = count;   /* meaningful even when rc != MYDB_OK */
        pthread_cond_broadcast(&writer->cond);
        pthread_mutex_unlock(&writer->lock);
    }

    return NULL;
}

/* ------------------------------------------------------------------
 * Lifecycle
 * ------------------------------------------------------------------ */

static int find_or_claim_active(LargeWalWriter *writer)
{
    for (uint32_t i = 0; i < LARGE_WAL_SEGMENT_POOL_SLOTS; i++) {
        if (writer->pool->slots[i].header.state == LSEG_ACTIVE) {
            writer->cur_slot_index = i;
            writer->cur_page_no    = writer->pool->slots[i].header.data_pages + 1;
            /* Resume on a fresh page rather than mid-page. The tail-scan
             * that recovered data_pages counts whole valid pages and
             * can't report how full the last one was, so appending into
             * it would need a byte count we don't have. Wasting the
             * remainder of one page per restart is the cheap, safe
             * choice; recovering it needs the page's own data_len, which
             * is a job for the recovery phase. */
            writer->cur_offset     = 0;
            return large_wal_registry_register(writer->registry, writer->pool->slots[i].header.segment_no,
                                                writer->pool->slots[i].fd, /*owns_fd=*/0);
        }
    }

    /* claim_next registers the segment it mints, so there's nothing to
     * register here — unlike the resume path above, where no claim
     * happens and the writer must do it. */
    uint32_t slot;
    if (large_wal_segment_pool_claim_next(writer->pool, &slot) != MYDB_OK) return MYDB_ERR;
    writer->cur_slot_index = slot;
    writer->cur_page_no    = 1;
    writer->cur_offset     = 0;
    return MYDB_OK;
}

int large_wal_writer_init(LargeWalWriter *writer, LargeWalSegmentPool *pool,
                           LargeWalRegistry *registry, LargeWalIndex *idx,
                           LargeWalState *state, WalWorker *worker)
{
    if (!writer || !pool || !registry || !idx || !state) return MYDB_ERR;

    memset(writer, 0, sizeof(*writer));
    writer->pool     = pool;
    writer->registry = registry;
    writer->idx      = idx;
    writer->state    = state;
    writer->worker   = worker;

    /* content_lsn is the same shared, monotonic sequence across the
     * whole engine (confirmed this session), and this writer only ever
     * has one segment open at a time, so state's own already-durable
     * flush_lsn is the correct "last content_lsn written into the
     * resumed segment" seed on reload -- a genuine value, not a
     * placeholder. */
    writer->cur_segment_last_lsn = state->flush_lsn;

    return find_or_claim_active(writer);
}

int large_wal_writer_start(LargeWalWriter *writer)
{
    if (!writer) return MYDB_ERR;

    if (pthread_mutex_init(&writer->lock, NULL) != 0) return MYDB_ERR;
    if (pthread_cond_init(&writer->cond, NULL) != 0) {
        pthread_mutex_destroy(&writer->lock);
        return MYDB_ERR;
    }

    writer->stop_requested = 0;
    if (pthread_create(&writer->thread, NULL, writer_main, writer) != 0) {
        pthread_cond_destroy(&writer->cond);
        pthread_mutex_destroy(&writer->lock);
        return MYDB_ERR;
    }

    writer->started = 1;
    return MYDB_OK;
}

int large_wal_writer_stop(LargeWalWriter *writer)
{
    if (!writer || !writer->started) return MYDB_OK;

    pthread_mutex_lock(&writer->lock);
    writer->stop_requested = 1;
    pthread_cond_broadcast(&writer->cond);
    pthread_mutex_unlock(&writer->lock);

    pthread_join(writer->thread, NULL);

    pthread_cond_destroy(&writer->cond);
    pthread_mutex_destroy(&writer->lock);
    writer->started = 0;

    return MYDB_OK;
}

/* ------------------------------------------------------------------
 * submit — blocking request/response handoff
 * ------------------------------------------------------------------ */

int large_wal_writer_submit(LargeWalWriter *writer, const uint8_t *content, uint32_t total_size,
                             LargeWalIndexEntry *out_entries, uint32_t out_cap,
                             uint32_t *out_count)
{
    if (!writer || !writer->started || !content || !out_entries || !out_count || out_cap == 0)
        return MYDB_ERR;

    *out_count = 0;

    pthread_mutex_lock(&writer->lock);
    if (writer->stop_requested) {
        pthread_mutex_unlock(&writer->lock);
        return MYDB_ERR;
    }

    writer->req_content     = content;
    writer->req_total_size  = total_size;
    writer->req_out_entries = out_entries;
    writer->req_out_cap     = out_cap;
    writer->req_out_count   = 0;
    writer->request_pending = 1;
    writer->request_done    = 0;
    pthread_cond_broadcast(&writer->cond);

    while (!writer->request_done)
        pthread_cond_wait(&writer->cond, &writer->lock);

    int rc = writer->last_result;
    /* Reported regardless of rc: on a partial failure this is how many
     * records really did land, durably and indexed. */
    *out_count = writer->req_out_count;
    pthread_mutex_unlock(&writer->lock);

    return rc;
}
