#include "wal_insert.h"
#include "wal_types.h"

#include <assert.h>
#include <sched.h>
#include <stdatomic.h>
#include <stddef.h>
#include <string.h>
#include <time.h>

_Static_assert(WAL_PAGE_USABLE <= UINT16_MAX, "continuation_len is a uint16_t");

/* total_len of the record whose header starts at rec. */
static uint32_t record_len_at(const uint8_t *rec)
{
    uint32_t total_len;
    memcpy(&total_len, rec + offsetof(WalRecordHeader, total_len), sizeof(total_len));
    return total_len;
}

uint64_t wal_insert_reserve(WalManager *wm, uint32_t len)
{
    /* Relaxed: the add only hands out the range. */

    return atomic_fetch_add_explicit(&wm->current_pos, len, memory_order_relaxed);
}

uint64_t wal_insert_fill_headers(uint8_t *buf, uint32_t len,
                                 uint64_t byte_pos, uint64_t prev_lsn)
{
    /* One plain loop: no record's CRC waits for another's, so the CPU
     * overlaps them by itself. */
    for (uint32_t off = 0; off < len; ) {
        WalRecordHeader h;
        memcpy(&h, buf + off, WAL_RECORD_HEADER_SIZE);

        /* A record is at least its header, for debug builds only. */
        assert(h.total_len >= WAL_RECORD_HEADER_SIZE);

        h.lsn      = wal_byte_to_lsn(byte_pos + off);
        h.prev_lsn = prev_lsn;
        h.flags   &= (uint8_t)~WAL_RECORD_FLAG_CONTINUES_ON_NEW_PAGE;
        if ((uint64_t)wal_lsn_offset(h.lsn) + h.total_len > WAL_PAGE_SIZE)
            h.flags |= WAL_RECORD_FLAG_CONTINUES_ON_NEW_PAGE;
        wal_record_header_pack_crc(&h, h.checksum, buf + off);   /* checksum held body_checksum */

        prev_lsn = h.lsn;
        off     += h.total_len;
    }
    return prev_lsn;
}

void wal_insert_plan(const WalManager *wm, WalByteInsert *p, uint64_t lsn,
                     const uint8_t *buf, uint32_t len, uint32_t off)
{
    if (off == 0) {         /* a new buffer: no record stepped onto yet */
        p->rec_start = 0;
        p->rec_end   = 0;
    }

    uint32_t page_off = wal_lsn_offset(lsn);
    assert(page_off >= WAL_PAGE_HEADER_SIZE);
    uint32_t n        = WAL_PAGE_SIZE - page_off;
    if (n > len - off) n = len - off;

    p->lsn          = lsn;
    p->src          = buf + off;
    p->len          = n;
    p->frame        = (uint32_t)((lsn >> WAL_PAGE_SHIFT) & wm->ring_frame_mask);
    p->has_page_hdr = (page_off == WAL_PAGE_HEADER_SIZE);

    uint64_t from = lsn;   /* first byte this piece writes */
    if (p->has_page_hdr) {
        /* Step to the record holding off; records that started and ended
         * between two page starts are passed over here. */
        while (p->rec_end <= off) {
            uint32_t rec_len = record_len_at(buf + p->rec_end);

            /* A record is at least its header, for debug builds only. */
            assert(rec_len >= WAL_RECORD_HEADER_SIZE);

            p->rec_start = p->rec_end;
            p->rec_end  += rec_len;
        }

        /* Only facts this copier knows, so the header is written once and
         * never changes (V2 §8.2). */
        WalPageHeader h = {0};
        h.id.file_type = FILETYPE_WAL_PAGE;
        h.start_lsn    = lsn - WAL_PAGE_HEADER_SIZE;
        if (off != p->rec_start) {   /* mid-record: it began on an earlier page */
            uint32_t rest      = p->rec_end - off;
            h.continuation_len = (uint16_t)(rest < WAL_PAGE_USABLE ? rest : WAL_PAGE_USABLE);
            h.flags            = WAL_PAGE_INCOMING_CONTINUATION;
        }
        wal_page_header_serialize(&h, p->page_hdr);
        from = h.start_lsn;
    }

    /* One add per 512-byte block of [from, lsn + n). */
    uint64_t end = lsn + n;
    uint8_t  k   = 0;
    while (from < end) {
        uint64_t block_end = (from | (WAL_BLOCK_SIZE - 1)) + 1;
        uint64_t upto      = (block_end < end) ? block_end : end;
        p->ctr_idx[k] = (uint32_t)((from >> WAL_BLOCK_SHIFT) & wm->ctr_mask);
        p->ctr_add[k] = (uint16_t)(upto - from);
        k++;
        from = upto;
    }
    p->n_ctr = k;
}

void wal_insert_copy(WalManager *wm, const WalByteInsert *p)
{
    uint8_t *frame = wm->ring_buf + (size_t)p->frame * WAL_PAGE_SIZE;

    if (p->has_page_hdr)
        memcpy(frame, p->page_hdr, WAL_PAGE_HEADER_SIZE);
    memcpy(frame + wal_lsn_offset(p->lsn), p->src, p->len);

    for (uint8_t i = 0; i < p->n_ctr; i++)
        atomic_fetch_add_explicit(&wm->fs_counters[p->ctr_idx[i]].done, p->ctr_add[i],
                                  memory_order_release);
}

/* Interim wait until write_lsn reaches target: spin, then yield, then
 * sleep. The flush piece replaces it with running the write step here. */
static void wait_write_lsn(WalManager *wm, uint64_t target, uint64_t *write_lsn_cached)
{
    for (uint32_t tries = 0; ; tries++) {
        *write_lsn_cached = atomic_load_explicit(&wm->write_lsn, memory_order_acquire);
        if (*write_lsn_cached >= target) return;

        if (tries < 64)        continue;
        else if (tries < 128)  sched_yield();
        else                   nanosleep(&(struct timespec){ .tv_nsec = 50000 }, NULL);
    }
}

void wal_insert_space_check(WalManager *wm, uint64_t end_lsn, uint64_t *write_lsn_cached)
{
    uint64_t ring_size_bytes = (uint64_t)wm->ring_buf_size << WAL_PAGE_SHIFT;
    uint64_t space_required      = (end_lsn + WAL_BLOCK_SIZE - 1) & ~(uint64_t)(WAL_BLOCK_SIZE - 1);

    /* space_required <= write_lsn + ring_size_bytes, written as an add:
     * space_required - ring_size_bytes would wrap below zero for the
     * ring's first lap. */
    if (space_required <= *write_lsn_cached + ring_size_bytes) return;   /* the session's copy */

    /* Acquire: the write step zeroes the blocks it passed before publishing
     * write_lsn, so the copy after this sees them zeroed. */
    *write_lsn_cached = atomic_load_explicit(&wm->write_lsn, memory_order_acquire);
    if (space_required <= *write_lsn_cached + ring_size_bytes) return;

    wait_write_lsn(wm, space_required - ring_size_bytes, write_lsn_cached);
}

int wal_insert(WalManager *wm, uint8_t *buf, uint32_t len, uint64_t prev_lsn,
               uint64_t *write_lsn_cached, uint64_t *out_last_lsn, uint64_t *out_end_lsn)
{
    /* Before the reservation, the last point where failing is possible. */
    if (!wm || !buf || !write_lsn_cached || !out_last_lsn || !out_end_lsn ||
        len < WAL_RECORD_HEADER_SIZE)
        return MYDB_ERR;

    uint64_t byte_pos = wal_insert_reserve(wm, len);                              /* 1. reserve */
    uint64_t lsn      = wal_byte_to_lsn(byte_pos);                                /* 2. convert */
    uint64_t last_lsn = wal_insert_fill_headers(buf, len, byte_pos, prev_lsn);    /* 3. headers */

    WalByteInsert p;
    uint32_t off = 0;
    while (off < len) {
        wal_insert_plan(wm, &p, lsn, buf, len, off);                              /* 4. plan    */
        wal_insert_space_check(wm, p.lsn + p.len, write_lsn_cached);               /*    space   */
        wal_insert_copy(wm, &p);                                                  /* 5. copy    */
        off += p.len;
        lsn += p.len + WAL_PAGE_HEADER_SIZE;   /* next page, past its header */
    }

    *out_last_lsn = last_lsn;
    *out_end_lsn  = p.lsn + p.len;             /* the last piece's end */
    return MYDB_OK;
}
