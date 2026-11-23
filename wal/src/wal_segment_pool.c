#include "wal_segment_pool.h"
#include "wal_page.h"
#include "wal_types.h"
#include "common.h"

/* wal_page.h's LSN arithmetic assumes this pool's file layout: one header
 * page-slot outside the LSN, then exactly WAL_SEG_SIZE bytes of log. (It
 * was WAL_SEG_SIZE == WAL_SEGMENT_FILE_SIZE while the header page was
 * inside the LSN.) */
_Static_assert(WAL_SEGMENT_FILE_SIZE == WAL_PAGE_SIZE + WAL_SEG_SIZE,
               "a segment file must be one header page plus WAL_SEG_SIZE bytes of log");

#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>

/* ------------------------------------------------------------------
 * Internal helpers — same shape as disk_manager.c's pwrite_all/
 * pread_all (each on-disk-I/O file in this codebase keeps its own
 * static copy rather than sharing one; ensure_dir mirrors the existing
 * copies already duplicated in engine/src/engine.c and
 * engine/src/stats_buffer.c).
 * ------------------------------------------------------------------ */

static int pwrite_all(int fd, const void *buf, size_t n, off_t offset)
{
    ssize_t written = pwrite(fd, buf, n, offset);
    return (written == (ssize_t)n) ? MYDB_OK : MYDB_ERR;
}

static int pread_all(int fd, void *buf, size_t n, off_t offset)
{
    ssize_t got = pread(fd, buf, n, offset);
    return (got == (ssize_t)n) ? MYDB_OK : MYDB_ERR;
}

/* mkdir, ignoring EEXIST. Returns 0 if the directory now exists. */
static int ensure_dir(const char *path)
{
    if (mkdir(path, 0755) == 0) return 0;
    if (errno == EEXIST) {
        struct stat st;
        if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) return 0;
    }
    return -1;
}

static int page_no_valid(uint32_t page_no)
{
    return page_no >= 1 && page_no < WAL_SEGMENT_PAGES_PER_FILE;
}

/* ------------------------------------------------------------------
 * Per-slot open-or-create.
 *
 * A segment file's page-0 slot is a full WAL_PAGE_SIZE page (not just
 * WAL_SEGMENT_HEADER_ON_DISK_SIZE bytes) — 64 meaningful bytes followed
 * by zero padding to fill the page, same "page N starts at N *
 * WAL_PAGE_SIZE" convention every other on-disk format in this codebase
 * follows (disk_manager.h's FileHeader = page 0, data starts at page 1).
 * Real WAL pages therefore start at page_no = 1. The header page-slot is
 * outside the LSN (wal_page.h), so file page_no p holds log page p - 1 of
 * the segment: the LSN (seg << 22) + (p - 1) * WAL_PAGE_SIZE.
 * ------------------------------------------------------------------ */
static int open_existing_or_create_slot(WalSegmentPool *pool, uint32_t slot_no)
{
    char path[300];
    snprintf(path, sizeof(path), "%s/wal_%u.mydb", pool->wal_dir, slot_no);

    WalSlot *slot = &pool->slots[slot_no];
    slot->slot_no = (uint8_t)slot_no;
    slot->fd = -1;

    int fd = open(path, O_RDWR);
    if (fd >= 0) {
        /* Existing segment file from a prior run — reload its header. */
        uint8_t page_buf[WAL_PAGE_SIZE];
        if (pread_all(fd, page_buf, WAL_PAGE_SIZE, 0) != MYDB_OK) {
            close(fd);
            return MYDB_ERR;
        }
        int rc = wal_segment_header_deserialize(page_buf, &slot->header);
        if (rc != MYDB_OK) {
            close(fd);
            return rc;
        }
        slot->fd = fd;
        return MYDB_OK;
    }
    if (errno != ENOENT) return MYDB_ERR;

    /* Doesn't exist yet — create, pre-allocate, write an initial
     * SEG_FREE header. posix_fallocate() (portable POSIX call) stands in
     * for the design doc's "via fallocate()" — Linux fallocate() needs
     * _GNU_SOURCE, which nothing else in this codebase defines;
     * posix_fallocate() gives the same practical guarantee (blocks
     * reserved up front, no later ENOSPC surprise) without it. */
    fd = open(path, O_RDWR | O_CREAT | O_EXCL, 0644);
    if (fd < 0) return MYDB_ERR;

    if (posix_fallocate(fd, 0, WAL_SEGMENT_FILE_SIZE) != 0) {
        close(fd);
        return MYDB_ERR;
    }

    WalSegmentHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.id.file_type = FILETYPE_WAL_SEGMENT;
    hdr.segment_no   = 0;   /* placeholder — the real value is stamped at claim time */
    hdr.partition_id = pool->partition_id;
    hdr.state        = SEG_FREE;

    uint8_t page_buf[WAL_PAGE_SIZE];
    memset(page_buf, 0, WAL_PAGE_SIZE);
    wal_segment_header_serialize(&hdr, page_buf);

    if (pwrite_all(fd, page_buf, WAL_PAGE_SIZE, 0) != MYDB_OK) {
        close(fd);
        return MYDB_ERR;
    }
    if (fdatasync(fd) < 0) {
        close(fd);
        return MYDB_ERR;
    }

    /* Read back rather than trust the local hdr — picks up the real
     * checksum wal_segment_header_serialize stamped into page_buf. */
    wal_segment_header_deserialize(page_buf, &slot->header);
    slot->fd = fd;
    return MYDB_OK;
}

/* ------------------------------------------------------------------
 * Lifecycle
 * ------------------------------------------------------------------ */

int wal_segment_pool_init(WalSegmentPool *pool, const char *wal_dir, uint32_t partition_id)
{
    if (!pool || !wal_dir) return MYDB_ERR;
    if (ensure_dir(wal_dir) != 0) return MYDB_ERR;

    memset(pool, 0, sizeof(*pool));
    snprintf(pool->wal_dir, sizeof(pool->wal_dir), "%s", wal_dir);
    pool->partition_id = partition_id;

    for (uint32_t i = 0; i < WAL_SEGMENT_POOL_SLOTS; i++) {
        int rc = open_existing_or_create_slot(pool, i);
        if (rc != MYDB_OK) {
            /* Close whatever we already opened before bailing out. */
            for (uint32_t j = 0; j < i; j++) {
                if (pool->slots[j].fd >= 0) close(pool->slots[j].fd);
            }
            return rc;
        }
    }

    /* A slot reloaded SEG_ACTIVE has a stale on-disk data_pages by
     * definition (impl doc §8.4). Init used to recount it here by
     * counting valid pages; the end of the log is now found by
     * wal_segment_pool_tail_scan, which wal_manager_init calls, so the
     * cached data_pages of an ACTIVE slot is left as read. */

    /* Reseed the shared claim counter from whatever's on disk — see
     * wal_segment_pool.h's header comment for why this scheme (one
     * pool-level counter, assigned at claim time) stands in for the
     * design doc's Archiver-driven free-time stamping. Counts every slot
     * that was ever claimed (start_lsn != 0), FREE included, not just the
     * non-FREE ones: the segment number is part of the LSN, so it must
     * never drop back (T-010). */
    uint64_t max_used = 0;
    int      any_used = 0;
    for (uint32_t i = 0; i < WAL_SEGMENT_POOL_SLOTS; i++) {
        if (pool->slots[i].header.state != SEG_FREE ||
            pool->slots[i].header.start_lsn != 0) {
            if (!any_used || pool->slots[i].header.segment_no > max_used)
                max_used = pool->slots[i].header.segment_no;
            any_used = 1;
        }
    }
    pool->next_segment_no = any_used ? (max_used + 1) : 0;

    return MYDB_OK;
}

int wal_segment_pool_shutdown(WalSegmentPool *pool)
{
    if (!pool) return MYDB_ERR;
    for (uint32_t i = 0; i < WAL_SEGMENT_POOL_SLOTS; i++) {
        if (pool->slots[i].fd >= 0) {
            close(pool->slots[i].fd);
            pool->slots[i].fd = -1;
        }
    }
    return MYDB_OK;
}

/* ------------------------------------------------------------------
 * Claiming
 * ------------------------------------------------------------------ */

int wal_segment_pool_claim_next(WalSegmentPool *pool, uint32_t *out_slot_index)
{
    if (!pool || !out_slot_index) return MYDB_ERR;

    uint32_t target = (uint32_t)(pool->next_segment_no % WAL_SEGMENT_POOL_SLOTS);
    WalSlot *slot = &pool->slots[target];

    if (slot->header.state != SEG_FREE) return MYDB_ERR;

    /* Mutate a local copy, not slot->header: if the write below fails we
     * must leave the in-memory header exactly as it was, or memory and
     * disk disagree about this slot's state and every later decision
     * trusts the wrong one. slot->header is only committed once the
     * write has actually succeeded. */
    WalSegmentHeader next = slot->header;
    next.segment_no   = pool->next_segment_no;
    next.start_lsn    = wal_segment_first_lsn(pool->next_segment_no);   /* T-039 */
    next.end_lsn      = 0;
    next.data_pages   = 0;
    next.state        = SEG_ACTIVE;
    next.partition_id = pool->partition_id;

    uint8_t page_buf[WAL_PAGE_SIZE];
    memset(page_buf, 0, WAL_PAGE_SIZE);
    wal_segment_header_serialize(&next, page_buf);

    if (pwrite_all(slot->fd, page_buf, WAL_PAGE_SIZE, 0) != MYDB_OK) return MYDB_ERR;
    /* No fdatasync here — see the header's doc comment for why claim_next
     * never needs one. */

    wal_segment_header_deserialize(page_buf, &slot->header);

    pool->next_segment_no++;
    *out_slot_index = target;
    return MYDB_OK;
}

int wal_segment_pool_mark_done(WalSegmentPool *pool, WalWorker *worker,
                               uint32_t slot_index, uint64_t end_lsn, uint32_t data_pages,
                               uint64_t *out_ticket)
{
    if (out_ticket) *out_ticket = 0;
    if (!pool || slot_index >= WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;

    WalSlot *slot = &pool->slots[slot_index];
    if (slot->header.state != SEG_ACTIVE) return MYDB_ERR;

    /* Local copy, committed to slot->header only after the write and
     * sync both succeed — see claim_next's own note. */
    WalSegmentHeader next = slot->header;
    next.end_lsn    = end_lsn;
    next.data_pages = data_pages;
    next.state      = SEG_DONE;

    uint8_t page_buf[WAL_PAGE_SIZE];
    memset(page_buf, 0, WAL_PAGE_SIZE);
    wal_segment_header_serialize(&next, page_buf);

    if (pwrite_all(slot->fd, page_buf, WAL_PAGE_SIZE, 0) != MYDB_OK) return MYDB_ERR;
    if (worker) {
        /* out_ticket is written by async_fdatasync itself, in the same
         * call that posts -- so it is set before any later step here can
         * fail. That ordering is the point: a mark_done that posts and
         * then returns an error would otherwise leave its caller with no
         * ticket to drain with, and an undrained mailbox blocks the next
         * post forever, for the whole partition. */
        if (wal_worker_async_fdatasync(worker, slot->fd, out_ticket) != MYDB_OK) return MYDB_ERR;
    } else {
        if (fdatasync(slot->fd) < 0) return MYDB_ERR;
    }

    wal_segment_header_deserialize(page_buf, &slot->header);
    return MYDB_OK;
}

/* ------------------------------------------------------------------
 * Raw page I/O
 * ------------------------------------------------------------------ */

int wal_segment_pool_write_page(WalSegmentPool *pool, uint32_t slot_index,
                                 uint32_t page_no, const uint8_t *buf)
{
    if (!pool || !buf || slot_index >= WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;
    if (!page_no_valid(page_no)) return MYDB_ERR;

    off_t offset = (off_t)page_no * WAL_PAGE_SIZE;
    return pwrite_all(pool->slots[slot_index].fd, buf, WAL_PAGE_SIZE, offset);
}

int wal_segment_pool_read_page(WalSegmentPool *pool, uint32_t slot_index,
                                uint32_t page_no, uint8_t *out_buf)
{
    if (!pool || !out_buf || slot_index >= WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;
    if (!page_no_valid(page_no)) return MYDB_ERR;

    off_t offset = (off_t)page_no * WAL_PAGE_SIZE;
    return pread_all(pool->slots[slot_index].fd, out_buf, WAL_PAGE_SIZE, offset);
}

int wal_segment_pool_read_segment(WalSegmentPool *pool, uint32_t slot_index,
                                   uint8_t *out_buf)
{
    if (!pool || !out_buf || slot_index >= WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;
    return pread_all(pool->slots[slot_index].fd, out_buf, WAL_SEGMENT_FILE_SIZE, 0);
}

/* ------------------------------------------------------------------
 * wal_segment_pool_write — the Flusher-facing abstraction. A blind byte
 * mover: never parses or constructs WalPageHeader content (the Flusher's
 * buf already carries whatever headers/LSNs it needs, copied straight
 * from ring-buffer frames — impl doc §8.6). Only knows page/segment
 * *geometry*. slot_index/page_no/offset are the caller's own segment-
 * side cursor, purely positional — never derived from anything inside
 * buf (see wal_segment_pool.h for the full contract, including why page
 * numbers never need reconciling between the ring buffer and the
 * segment: WalPageHeader carries no page_no field at all).
 * ------------------------------------------------------------------ */

int wal_segment_pool_write(WalSegmentPool *pool, WalWorker *worker,
                            uint32_t *slot_index, uint32_t *page_no, uint32_t *offset,
                            const uint8_t *buf, size_t buf_len)
{
    if (!pool || !slot_index || !page_no || !offset || !buf) return MYDB_ERR;
    if (*slot_index >= WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;
    if (!page_no_valid(*page_no)) return MYDB_ERR;
    if (*offset >= WAL_PAGE_SIZE) return MYDB_ERR;

    /* Single exit through `out:` from here on. Once mark_done() below has
     * handed a flush to worker, EVERY return path has to drain it:
     * wal_worker_wait() is the only thing that clears the mailbox's done
     * flag, so returning early without it wedges the next
     * wal_worker_async_fdatasync() forever — for the whole partition,
     * not just this call. */
    int rc = MYDB_OK;

    /* Names the fsync mark_done() handed to worker, if this call rolled
     * over at all. Stays 0 otherwise, and wal_worker_wait() treats 0 as
     * "I posted nothing" -- which is what lets the drain at `out:` stay
     * unconditional without ever collecting the OTHER caller thread's
     * result. Both WAL paths share one WalWorker (wal_manager.h). */
    uint64_t flush_ticket = 0;

    size_t written = 0;
    while (written < buf_len) {
        uint32_t room_left = WAL_PAGE_SIZE - *offset;
        size_t   remaining = buf_len - written;
        uint32_t chunk = (remaining < (size_t)room_left) ? (uint32_t)remaining : room_left;

        off_t abs_offset = (off_t)(*page_no) * WAL_PAGE_SIZE + *offset;
        if (pwrite_all(pool->slots[*slot_index].fd, buf + written, chunk, abs_offset) != MYDB_OK) {
            rc = MYDB_ERR;
            goto out;
        }

        written += chunk;
        *offset += chunk;

        if (*offset < WAL_PAGE_SIZE) {
            /* This page-slot isn't full — every remaining byte must have
             * fit (chunk was bounded by `remaining`, not `room_left`),
             * so the loop is about to exit with the cursor left mid-page
             * for a future call to resume appending into. */
            break;
        }

        /* This page-slot just filled exactly. */
        *offset = 0;

        if (*page_no + 1 >= WAL_SEGMENT_PAGES_PER_FILE) {
            /* The segment is full too. The one non-"blind" step: read
             * the page we just filled back and work out where its data
             * ends — start_lsn + WAL_PAGE_SIZE, since every page the
             * Flusher writes is full. (It used to copy the page's own
             * end_lsn field, dropped in T-039, and then add data_len,
             * dropped after it; see wal_page.h.) A field read, not
             * content interpretation. */
            uint8_t  last_page[WAL_PAGE_SIZE];
            uint64_t seg_end_lsn = 0;
            if (wal_segment_pool_read_page(pool, *slot_index, *page_no, last_page) == MYDB_OK) {
                WalPageHeader hdr;
                if (wal_page_header_deserialize(last_page, &hdr) == MYDB_OK)
                    seg_end_lsn = hdr.start_lsn + WAL_PAGE_SIZE;
            }

            uint32_t finished_slot       = *slot_index;
            uint32_t finished_data_pages = *page_no;
            if (wal_segment_pool_mark_done(pool, worker, finished_slot, seg_end_lsn,
                                           finished_data_pages, &flush_ticket) != MYDB_OK) {
                rc = MYDB_ERR;
                goto out;
            }
            /* mark_done may have just handed a flush to worker — from
             * here on, bailing out without draining it would wedge the
             * mailbox, hence goto rather than return. */
            if (wal_segment_pool_claim_next(pool, slot_index) != MYDB_OK) {
                rc = MYDB_ERR;
                goto out;
            }
            *page_no = 1;
        } else {
            (*page_no)++;
        }
    }

    /* TEMPORARY: fdatasync every call, since there's no Flusher/group-commit
     * yet to batch many writes behind one flush (design doc §11).
     * fdatasync() flushes every dirty page of this fd, not just what this
     * call wrote, so this is correct even after a mid-call segment
     * rollover (the finalized old segment was already fdatasync'd by
     * mark_done() itself; this covers whatever landed in the current
     * *slot_index since). Segment files are posix_fallocate'd to their
     * full fixed size once at creation and never resized again, so
     * fdatasync (which skips flushing metadata not needed for correct
     * data retrieval, e.g. timestamps) is strictly cheaper than fsync
     * here with no durability difference for our data — avoids dragging
     * the filesystem journal into every WAL flush on filesystems like
     * ext4. Remove this one line once the Flusher lands and does its own
     * batched flush at the end of each flush cycle instead. */
    if (fdatasync(pool->slots[*slot_index].fd) < 0) rc = MYDB_ERR;

out:
    /* If this call rolled over, mark_done() may have handed its fsync to
     * worker instead of waiting for it (see mark_done's own doc comment)
     * — confirm it's actually durable before telling our own caller this
     * call succeeded. Cheap no-op if it didn't. Runs unconditionally,
     * including on the error paths above: draining is what keeps the
     * shared mailbox usable, and the worker's own result still counts
     * toward what we report. */
    if (worker && wal_worker_wait(worker, flush_ticket) != MYDB_OK) rc = MYDB_ERR;

    return rc;
}

/* ------------------------------------------------------------------
 * Finding the end of the log (see wal_segment_pool.h)
 * ------------------------------------------------------------------ */

/* The slot holding segment seg (any state but FREE), or -1. */
static int slot_of_segment(const WalSegmentPool *pool, uint64_t seg)
{
    for (uint32_t i = 0; i < WAL_SEGMENT_POOL_SLOTS; i++)
        if (pool->slots[i].header.state != SEG_FREE && pool->slots[i].header.segment_no == seg)
            return (int)i;
    return -1;
}

/* Reads the log page holding lsn, from whichever slot holds its segment,
 * and checks it is the page that belongs at that position: a valid header
 * (magic, checksum) whose start_lsn is the page's own LSN. An unwritten
 * page fails the magic check, and a stale page from an earlier pass
 * through the slot carries another segment's start_lsn (PostgreSQL's
 * xlp_pageaddr check). Returns MYDB_OK, MYDB_ERR_NOT_FOUND when the page
 * is not there (the log ends before it), or MYDB_ERR when the read itself
 * fails: an I/O error must not pass for the end of the log. */
static int read_log_page(WalSegmentPool *pool, uint64_t lsn, uint8_t *page, WalPageHeader *ph)
{
    int slot = slot_of_segment(pool, wal_lsn_segment(lsn));
    if (slot < 0) return MYDB_ERR_NOT_FOUND;
    /* File page 0 is the segment header, outside the LSN. */
    if (wal_segment_pool_read_page(pool, (uint32_t)slot, wal_lsn_page(lsn) + 1, page) != MYDB_OK) return MYDB_ERR;
    if (wal_page_header_deserialize(page, ph) != MYDB_OK) return MYDB_ERR_NOT_FOUND;
    if (ph->start_lsn != (lsn & ~(uint64_t)(WAL_PAGE_SIZE - 1))) return MYDB_ERR_NOT_FOUND;
    return MYDB_OK;
}

/* Reads the record that should start at data position lsn into *buf
 * (grown as needed) and checks it, the way PostgreSQL's reader does: its
 * lsn field is its own position (catches stale bytes in a page that is
 * otherwise current), total_len is at least a header and fits before
 * log_end, every page it continues onto is valid and carries the
 * INCOMING flag, and its CRC holds. A record may cross into the next
 * segment. Returns MYDB_OK with *out_total, MYDB_ERR_NOT_FOUND when there
 * is no valid record there (the end of the log), or MYDB_ERR when a read
 * or an allocation fails. */
static int read_record(WalSegmentPool *pool, uint64_t log_end, uint64_t lsn,
                       uint8_t **buf, size_t *cap, uint32_t *out_total)
{
    uint8_t       page[WAL_PAGE_SIZE];
    WalPageHeader ph;
    uint64_t      page_base = UINT64_MAX;   /* page currently in `page` */
    uint64_t      pos   = lsn;
    size_t        got   = 0;
    size_t        need  = WAL_RECORD_HEADER_SIZE;
    uint32_t      total = 0;

    /* Record bytes left from lsn to log_end: the most a record starting
     * here can hold, so a garbage total_len never makes us allocate more
     * than the log that is there. */
    uint64_t avail = (log_end >> WAL_PAGE_SHIFT) * WAL_PAGE_USABLE - wal_lsn_to_byte(lsn);

    while (got < need) {
        if (wal_lsn_offset(pos) == 0) pos += WAL_PAGE_HEADER_SIZE;   /* into the next page */
        if (pos >= log_end) return MYDB_ERR_NOT_FOUND;

        uint64_t base = pos & ~(uint64_t)(WAL_PAGE_SIZE - 1);
        if (base != page_base) {
            int rc = read_log_page(pool, pos, page, &ph);
            if (rc != MYDB_OK) return rc;
            /* A page the record continues onto must say so. */
            if (got > 0 && !(ph.flags & WAL_PAGE_INCOMING_CONTINUATION)) return MYDB_ERR_NOT_FOUND;
            page_base = base;
        }

        uint32_t off = wal_lsn_offset(pos);
        size_t   c   = need - got;
        if (c > WAL_PAGE_SIZE - off) c = WAL_PAGE_SIZE - off;
        if (*cap < need) {
            uint8_t *grown = realloc(*buf, need);
            if (!grown) return MYDB_ERR;
            *buf = grown;
            *cap = need;
        }
        memcpy(*buf + got, page + off, c);
        got += c;
        pos += c;

        /* Once the whole header is in, check it and learn the full size. */
        if (got == WAL_RECORD_HEADER_SIZE && total == 0) {
            uint64_t rec_lsn;
            memcpy(&rec_lsn, *buf + offsetof(WalRecordHeader, lsn),       sizeof(rec_lsn));
            memcpy(&total,   *buf + offsetof(WalRecordHeader, total_len), sizeof(total));
            if (rec_lsn != lsn) return MYDB_ERR_NOT_FOUND;
            if (total < WAL_RECORD_HEADER_SIZE || total > avail) return MYDB_ERR_NOT_FOUND;
            need = total;
        }
    }

    WalRecordHeader hdr;
    if (wal_record_header_deserialize(*buf, total - WAL_RECORD_HEADER_SIZE, &hdr) != MYDB_OK)
        return MYDB_ERR_NOT_FOUND;
    *out_total = total;
    return MYDB_OK;
}

int wal_segment_pool_tail_scan(WalSegmentPool *pool, uint32_t *out_slot,
                                uint64_t *out_end_lsn)
{
    if (!pool || !out_slot || !out_end_lsn) return MYDB_ERR;

    /* The active segment: the ACTIVE slot with the highest segment_no. */
    int      found = 0;
    uint32_t best  = 0;
    for (uint32_t i = 0; i < WAL_SEGMENT_POOL_SLOTS; i++) {
        if (pool->slots[i].header.state != SEG_ACTIVE) continue;
        if (!found || pool->slots[i].header.segment_no > pool->slots[best].header.segment_no)
            best = i;
        found = 1;
    }
    if (!found) return MYDB_ERR_NOT_FOUND;   /* brand-new log */

    uint64_t active  = pool->slots[best].header.segment_no;
    uint64_t log_end = (active + 1) << WAL_SEG_SHIFT;   /* nothing exists past the active segment */

    /* Where the walk starts: the previous segment, when a slot still holds
     * it, since the end of the log can lie there (a record whose tail in
     * the active segment was never written, or a torn tail in the previous
     * segment whose pages reached disk after the active segment's). The
     * checkpoint will give this start point once it exists (V2 §13). */
    uint64_t start_seg = (active > 0 && slot_of_segment(pool, active - 1) >= 0) ? active - 1 : active;

    uint8_t       page[WAL_PAGE_SIZE];
    WalPageHeader ph;

    /* The first record of the start segment: past any bytes at its start
     * that continue a record from the segment before it (whole pages of
     * it, then continuation_len bytes of a partly continued page). If a
     * page is not there, nothing past it was written. */
    uint64_t pos = start_seg << WAL_SEG_SHIFT;
    for (;;) {
        if (pos >= log_end) break;
        int rc = read_log_page(pool, pos, page, &ph);
        if (rc == MYDB_ERR_NOT_FOUND) break;
        if (rc != MYDB_OK) return MYDB_ERR;
        uint32_t skip = (ph.flags & WAL_PAGE_INCOMING_CONTINUATION) ? ph.continuation_len : 0;
        if (skip < WAL_PAGE_USABLE) {
            pos += WAL_PAGE_HEADER_SIZE + skip;
            break;
        }
        pos += WAL_PAGE_SIZE;   /* the whole page continues that record */
    }

    /* Walk records, across segments, until the first one that does not
     * check out. pos is just past the last valid record (a page end when
     * it filled its page exactly); a record that starts at a page end
     * starts past the next page's header. */
    if (wal_lsn_offset(pos) != 0) {
        uint8_t *buf = NULL;
        size_t   cap = 0;
        int rc = MYDB_OK;
        for (;;) {
            uint64_t rec = (wal_lsn_offset(pos) == 0) ? pos + WAL_PAGE_HEADER_SIZE : pos;
            uint32_t total;
            if (rec >= log_end) break;
            rc = read_record(pool, log_end, rec, &buf, &cap, &total);
            if (rc != MYDB_OK) break;
            pos = wal_lsn_advance(rec, total);
        }
        free(buf);
        if (rc != MYDB_OK && rc != MYDB_ERR_NOT_FOUND) return MYDB_ERR;   /* a read or allocation failed */
    }

    /* The slot of the segment the end is in. An end exactly at the active
     * segment's end has no segment after it yet, so it stays with the
     * active one. */
    int end_slot = slot_of_segment(pool, wal_lsn_segment(pos));
    *out_slot    = (end_slot >= 0) ? (uint32_t)end_slot : best;
    *out_end_lsn = pos;
    return MYDB_OK;
}

/* ------------------------------------------------------------------
 * Cutting the log at its end (see wal_segment_pool.h)
 * ------------------------------------------------------------------ */

/* Writes hdr as slot's page-0 header and, once written, makes it the
 * cached copy (same order as claim_next: memory follows disk). */
static int write_header(WalSegmentPool *pool, uint32_t slot, const WalSegmentHeader *hdr)
{
    uint8_t page_buf[WAL_PAGE_SIZE];
    memset(page_buf, 0, WAL_PAGE_SIZE);
    wal_segment_header_serialize(hdr, page_buf);
    if (pwrite_all(pool->slots[slot].fd, page_buf, WAL_PAGE_SIZE, 0) != MYDB_OK) return MYDB_ERR;
    wal_segment_header_deserialize(page_buf, &pool->slots[slot].header);
    return MYDB_OK;
}

/* Zeroes slot's log from segment offset `from` (bytes into its 4MB of
 * log) to the segment's end: the rest of the page `from` falls in, then
 * every later page. */
static int zero_log_from(WalSegmentPool *pool, uint32_t slot, uint64_t from)
{
    uint8_t page[WAL_PAGE_SIZE];
    uint32_t p   = (uint32_t)(from >> WAL_PAGE_SHIFT);   /* log page within the segment */
    uint32_t off = (uint32_t)(from & (WAL_PAGE_SIZE - 1));
    if (off != 0 && p < WAL_SEG_PAGES) {
        if (wal_segment_pool_read_page(pool, slot, p + 1, page) != MYDB_OK) return MYDB_ERR;
        memset(page + off, 0, WAL_PAGE_SIZE - off);
        if (wal_segment_pool_write_page(pool, slot, p + 1, page) != MYDB_OK) return MYDB_ERR;
        p++;
    }
    memset(page, 0, WAL_PAGE_SIZE);
    for (; p < WAL_SEG_PAGES; p++)
        if (wal_segment_pool_write_page(pool, slot, p + 1, page) != MYDB_OK) return MYDB_ERR;
    return MYDB_OK;
}

int wal_segment_pool_truncate(WalSegmentPool *pool, uint32_t slot_index, uint64_t end_lsn)
{
    if (!pool || slot_index >= WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;
    WalSlot *slot = &pool->slots[slot_index];
    if (slot->header.state == SEG_FREE) return MYDB_ERR;
    uint64_t seg = slot->header.segment_no;

    /* 1. Discard every segment after the end's: its pages are zeroed and
     *    its header goes back to FREE with segment_no and start_lsn
     *    cleared, so its number is reused (the log continues from
     *    end_lsn) and init's reseed does not count it. */
    for (uint32_t i = 0; i < WAL_SEGMENT_POOL_SLOTS; i++) {
        WalSegmentHeader h = pool->slots[i].header;
        if (h.state == SEG_FREE || h.segment_no <= seg) continue;
        if (zero_log_from(pool, i, 0) != MYDB_OK) return MYDB_ERR;
        h.state = SEG_FREE; h.segment_no = 0; h.start_lsn = 0; h.end_lsn = 0; h.data_pages = 0;
        if (write_header(pool, i, &h) != MYDB_OK) return MYDB_ERR;
    }

    /* 2. The end's segment is where the log goes on: ACTIVE again if it
     *    had been finished. */
    if (slot->header.state != SEG_ACTIVE) {
        WalSegmentHeader h = slot->header;
        h.state = SEG_ACTIVE; h.end_lsn = 0; h.data_pages = 0;
        if (write_header(pool, slot_index, &h) != MYDB_OK) return MYDB_ERR;
    }

    /* 3. Nothing after end_lsn may look like log. An end exactly at the
     *    segment's end leaves nothing to zero. */
    if (wal_lsn_segment(end_lsn) == seg &&
        zero_log_from(pool, slot_index, end_lsn & WAL_SEG_MASK) != MYDB_OK)
        return MYDB_ERR;

    /* 4. What the scan read, and what was just zeroed, is on disk. */
    for (uint32_t i = 0; i < WAL_SEGMENT_POOL_SLOTS; i++)
        if (pool->slots[i].header.state != SEG_FREE && fdatasync(pool->slots[i].fd) < 0)
            return MYDB_ERR;

    /* 5. The next segment to claim is the one after the end's. */
    pool->next_segment_no = seg + 1;
    return MYDB_OK;
}

