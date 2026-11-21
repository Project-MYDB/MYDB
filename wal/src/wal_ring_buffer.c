#include "wal_ring_buffer.h"

#include <stdlib.h>
#include <string.h>
#include <time.h>

/* The ring frame holding data page k: pages pass through the ring in
 * order, so page k lives in frame k % WAL_RING_BUFFER_FRAMES. */
static uint8_t *frame_of_page(WalRingBuffer *rb, uint64_t k)
{
    return rb->buf + (size_t)(k % WAL_RING_BUFFER_FRAMES) * WAL_PAGE_SIZE;
}

/* Writes a frame's page header in place. (This replaces
 * reset_write_frame_header and the write_frame_hdr side copy: the header
 * lives only in the frame.) It is written once, when the frame is
 * opened: every field (start_lsn, continuation_len, flags) is known then
 * and never changes afterwards. */
static void frame_write_header(uint8_t *frame, uint64_t page_base,
                               uint16_t continuation_len, uint8_t flags)
{
    WalPageHeader h;
    memset(&h, 0, sizeof(h));
    h.id.file_type      = FILETYPE_WAL_PAGE;
    h.start_lsn         = page_base;
    h.continuation_len  = continuation_len;
    h.flags             = flags;
    wal_page_header_serialize(&h, frame);
}



/* Data page holding the last byte of a total_len-byte record placed at
 * data position lsn. */
static uint64_t last_page_of(uint64_t lsn, size_t total_len)
{
    return (wal_lsn_data_ord(lsn) + total_len - 1) / WAL_PAGE_USABLE;
}

/* The full check from append: the record's last page must not reach the
 * frame that still holds the first un-drained page. */
static int fits(const WalRingBuffer *rb, uint64_t lsn, size_t total_len)
{
    return last_page_of(lsn, total_len) - wal_lsn_data_page(rb->drained_lsn)
           < WAL_RING_BUFFER_FRAMES;
}

/* Copies n bytes of the record being appended to data position *pos,
 * crossing frames as needed. *left is how many bytes of the whole record
 * are still to be copied (this call's n included) — it decides whether a
 * next frame is opened and its continuation_len. (It used to decide the
 * OUT flag on the frame being closed too, before OUT was dropped.)
 * Called with rb->lock held. */
static void ring_put(WalRingBuffer *rb, uint64_t *pos, size_t *left,
                     const uint8_t *src, size_t n)
{
    while (n > 0) {
        uint32_t off  = wal_lsn_offset(*pos);
        uint32_t room = WAL_PAGE_SIZE - off;
        size_t   c    = (n < room) ? n : room;

        memcpy(frame_of_page(rb, wal_lsn_data_page(*pos)) + off, src, c);
        src   += c;
        n     -= c;
        *left -= c;
        *pos  += c;

        if (wal_lsn_offset(*pos) != 0) continue;   /* still inside the page */

        /* The page just filled exactly. Its header needs nothing more
         * (it used to be closed here: data_len set to full, and OUT if the
         * record went on).
         *
         * Open the next page only if there is more of this record. If not,
         * *pos stays on the page end and the next append opens it. */
        if (*left) {
            uint64_t next = wal_lsn_next_data(*pos);   /* steps over page 0 at a segment end */
            uint16_t cont = (uint16_t)((*left < WAL_PAGE_USABLE) ? *left : WAL_PAGE_USABLE);
            frame_write_header(frame_of_page(rb, wal_lsn_data_page(next)),
                               next - WAL_PAGE_HEADER_SIZE, cont,
                               WAL_PAGE_INCOMING_CONTINUATION);
            *pos = next;
        }
    }
}

int wal_ring_buffer_init(WalRingBuffer *rb, uint64_t start_lsn)
{
    if (!rb) return MYDB_ERR;
    if (wal_lsn_offset(start_lsn) < WAL_PAGE_HEADER_SIZE || wal_lsn_page(start_lsn) == 0)
        return MYDB_ERR;   /* must be a data position */

    rb->buf = malloc(WAL_RING_BUFFER_SIZE);
    if (!rb->buf) return MYDB_ERR;
    memset(rb->buf, 0, WAL_RING_BUFFER_SIZE);

    uint64_t base = start_lsn - wal_lsn_offset(start_lsn);
    rb->current_lsn = start_lsn;
    rb->drained_lsn = base;
    frame_write_header(frame_of_page(rb, wal_lsn_data_page(start_lsn)), base, 0, 0);

    if (pthread_mutex_init(&rb->lock, NULL) != 0) {
        free(rb->buf);
        return MYDB_ERR;
    }
    if (pthread_cond_init(&rb->append_wait_cond, NULL) != 0) {
        pthread_mutex_destroy(&rb->lock);
        free(rb->buf);
        return MYDB_ERR;
    }

    return MYDB_OK;
}

void wal_ring_buffer_shutdown(WalRingBuffer *rb)
{
    if (!rb) return;
    pthread_cond_destroy(&rb->append_wait_cond);
    pthread_mutex_destroy(&rb->lock);
    free(rb->buf);
    rb->buf = NULL;
}

int wal_ring_buffer_append(WalRingBuffer *rb, WalRecordHeader *hdr,
                            const void *body, size_t body_len,
                            uint64_t *out_lsn, uint64_t *out_end_lsn)
{
    if (!rb || !hdr || (!body && body_len > 0) || !out_lsn || !out_end_lsn) return MYDB_ERR;

    size_t total_len = WAL_RECORD_HEADER_SIZE + body_len;

    pthread_mutex_lock(&rb->lock);

    /* Where this record starts: current_lsn, or the next page's first data
     * byte if the last page filled exactly. */
    uint64_t lsn = wal_lsn_next_data(rb->current_lsn);

    /* Ring-full backpressure, exact: counted in frames against the reader's
     * end (drained_lsn), under the same lock snapshot() moves it under. It
     * used to be current_lsn - flush_lsn, an approximation read without
     * flush_mutex — see wal_ring_buffer.h. */
    if (!fits(rb, lsn, total_len)) {
        pthread_mutex_unlock(&rb->lock);
        return MYDB_ERR_FULL;
    }

    /* Open the next frame now that we know its old page was drained
     * (lazily — see current_lsn in wal_ring_buffer.h). */
    if (lsn != rb->current_lsn)
        frame_write_header(frame_of_page(rb, wal_lsn_data_page(lsn)),
                           lsn - WAL_PAGE_HEADER_SIZE, 0, 0);

    uint64_t end = wal_lsn_advance(lsn, total_len);

    /* The record-side twin of the page OUT flag: set when the record's last
     * byte lands on a later page than its first. Known before packing, so
     * the header is never rewritten after it is placed. */
    if (last_page_of(lsn, total_len) != wal_lsn_data_page(lsn))
        hdr->flags |= WAL_RECORD_FLAG_CONTINUES_ON_NEW_PAGE;
    else
        hdr->flags &= (uint8_t)~WAL_RECORD_FLAG_CONTINUES_ON_NEW_PAGE;
    hdr->lsn       = lsn;
    hdr->total_len = (uint32_t)total_len;

    uint8_t packed[WAL_RECORD_HEADER_SIZE];
    wal_record_header_pack(hdr, body, body_len, packed);

    uint64_t pos  = lsn;
    size_t   left = total_len;
    ring_put(rb, &pos, &left, packed, WAL_RECORD_HEADER_SIZE);
    ring_put(rb, &pos, &left, (const uint8_t *)body, body_len);

    /* (The open frame's header used to be rewritten here so its data_len
     * reached pos. With data_len gone the header written when the frame
     * was opened stays valid, so there is nothing to do.) */

    rb->current_lsn = pos;   /* == end */

    pthread_mutex_unlock(&rb->lock);

    *out_lsn     = lsn;
    *out_end_lsn = end;
    return MYDB_OK;
}

int wal_ring_buffer_append_wait(WalRingBuffer *rb, size_t total_len)
{
    if (!rb) return MYDB_ERR;

    pthread_mutex_lock(&rb->lock);
    while (!fits(rb, wal_lsn_next_data(rb->current_lsn), total_len))
        pthread_cond_wait(&rb->append_wait_cond, &rb->lock);
    pthread_mutex_unlock(&rb->lock);

    return MYDB_OK;
}

int wal_ring_buffer_snapshot(WalRingBuffer *rb,
                              uint8_t *scratch, uint32_t scratch_frame_cap,
                              uint32_t *out_num_frames, WalCursor *out_cursor)
{
    if (!rb || !scratch || !out_num_frames || !out_cursor)
        return MYDB_ERR;

    pthread_mutex_lock(&rb->lock);

    /* Closed pages are [first, end): end is the open frame's page, or the
     * page after current_lsn's when current_lsn sits on a page end. */
    uint64_t first = wal_lsn_data_page(rb->drained_lsn);
    uint64_t end   = (wal_lsn_offset(rb->current_lsn) == 0)
                   ? wal_lsn_data_page(rb->current_lsn - 1) + 1
                   : wal_lsn_data_page(rb->current_lsn);

    uint64_t available = end - first;
    uint32_t n = (available < scratch_frame_cap) ? (uint32_t)available : scratch_frame_cap;

    if (n == 0) {
        pthread_mutex_unlock(&rb->lock);
        *out_num_frames = 0;
        out_cursor->lsn      = 0;
        out_cursor->page_no  = (uint32_t)(first % WAL_RING_BUFFER_FRAMES);
        out_cursor->offset   = 0;
        return MYDB_OK;
    }

    /* Copy n frames starting at first's frame, possibly wrapping around
     * the ring's physical end — two memcpy's in that case, one otherwise. */
    uint32_t start_frame = (uint32_t)(first % WAL_RING_BUFFER_FRAMES);
    uint32_t first_chunk = WAL_RING_BUFFER_FRAMES - start_frame;
    if (first_chunk > n) first_chunk = n;

    memcpy(scratch, rb->buf + (size_t)start_frame * WAL_PAGE_SIZE,
           (size_t)first_chunk * WAL_PAGE_SIZE);
    if (n > first_chunk) {
        memcpy(scratch + (size_t)first_chunk * WAL_PAGE_SIZE, rb->buf,
               (size_t)(n - first_chunk) * WAL_PAGE_SIZE);
    }

    rb->drained_lsn = wal_data_page_base(first + n);
    pthread_cond_broadcast(&rb->append_wait_cond);

    pthread_mutex_unlock(&rb->lock);

    /* The end of the last copied page comes straight from arithmetic now
     * (it used to be read out of that page's end_lsn in scratch). */
    *out_num_frames      = n;
    out_cursor->lsn      = wal_data_page_base(first + n - 1) + WAL_PAGE_SIZE;
    out_cursor->page_no  = (uint32_t)((first + n) % WAL_RING_BUFFER_FRAMES);
    out_cursor->offset   = 0;
    return MYDB_OK;
}
