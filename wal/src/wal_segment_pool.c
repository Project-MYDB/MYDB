#include "wal_segment_pool.h"
#include "wal_page.h"
#include "wal_types.h"
#include "common.h"

/* wal_page.h's LSN arithmetic assumes this file layout. */
_Static_assert(WAL_SEGMENT_FILE_SIZE == WAL_PAGE_SIZE + WAL_SEG_SIZE,
               "a segment file must be one header page plus WAL_SEG_SIZE bytes of log");

#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>

/* One pwrite of n bytes; a short write is an error. */
static int pwrite_all(int fd, const void *buf, size_t n, off_t offset)
{
    ssize_t written = pwrite(fd, buf, n, offset);
    return (written == (ssize_t)n) ? MYDB_OK : MYDB_ERR;
}

/* One pread of n bytes; a short read is an error. */
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

/* A log page's file page number, 1..4096 (0 is the header). */
static int page_no_valid(uint32_t page_no)
{
    return page_no >= 1 && page_no < WAL_SEGMENT_PAGES_PER_FILE;
}

/* Opens slot_no's file and loads its header, or creates it: fallocated
 * to full size, with a FREE header in page 0, synced. File page p holds
 * log page p - 1 of the segment. */
static int open_existing_or_create_slot(WalSegmentPool *pool, uint32_t slot_no)
{
    char path[300];
    snprintf(path, sizeof(path), "%s/wal_%u.mydb", pool->wal_dir, slot_no);

    WalSlot *slot = &pool->slots[slot_no];
    slot->slot_no = (uint8_t)slot_no;
    slot->fd = -1;

    int fd = open(path, O_RDWR);
    if (fd >= 0) {
        /* A file from an earlier run: load its header. */
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

    /* New file: reserve all its blocks now, so a write never hits ENOSPC. */
    fd = open(path, O_RDWR | O_CREAT | O_EXCL, 0644);
    if (fd < 0) return MYDB_ERR;

    if (posix_fallocate(fd, 0, WAL_SEGMENT_FILE_SIZE) != 0) {
        close(fd);
        return MYDB_ERR;
    }

    WalSegmentHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.id.file_type = FILETYPE_WAL_SEGMENT;
    hdr.segment_no   = 0;   /* set when claimed */
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

    /* Read back, to get the checksum serialize stamped. */
    wal_segment_header_deserialize(page_buf, &slot->header);
    slot->fd = fd;
    return MYDB_OK;
}

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
            /* Close what was opened. */
            for (uint32_t j = 0; j < i; j++) {
                if (pool->slots[j].fd >= 0) close(pool->slots[j].fd);
            }
            return rc;
        }
    }

    /* next_segment_no = one past the highest segment ever claimed
     * (start_lsn != 0), FREE slots included: segment numbers are part of
     * the LSN and must never go back. */
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

int wal_segment_pool_claim_next(WalSegmentPool *pool, uint32_t *out_slot_index)
{
    if (!pool || !out_slot_index) return MYDB_ERR;

    uint32_t target = (uint32_t)(pool->next_segment_no % WAL_SEGMENT_POOL_SLOTS);
    WalSlot *slot = &pool->slots[target];

    if (slot->header.state != SEG_FREE) return MYDB_ERR;

    /* A copy: slot->header changes only once the write succeeded. */
    WalSegmentHeader next = slot->header;
    next.segment_no   = pool->next_segment_no;
    next.start_lsn    = wal_segment_first_lsn(pool->next_segment_no);
    next.end_lsn      = 0;
    next.data_pages   = 0;
    next.state        = SEG_ACTIVE;
    next.partition_id = pool->partition_id;

    uint8_t page_buf[WAL_PAGE_SIZE];
    memset(page_buf, 0, WAL_PAGE_SIZE);
    wal_segment_header_serialize(&next, page_buf);

    if (pwrite_all(slot->fd, page_buf, WAL_PAGE_SIZE, 0) != MYDB_OK) return MYDB_ERR;
    /* No fdatasync: see the header. */

    wal_segment_header_deserialize(page_buf, &slot->header);

    pool->next_segment_no++;
    *out_slot_index = target;
    return MYDB_OK;
}

int wal_segment_pool_mark_done(WalSegmentPool *pool, uint32_t slot_index,
                               uint64_t end_lsn, uint32_t data_pages)
{
    if (!pool || slot_index >= WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;

    WalSlot *slot = &pool->slots[slot_index];
    if (slot->header.state != SEG_ACTIVE) return MYDB_ERR;

    /* A copy: slot->header changes only once the write succeeded. */
    WalSegmentHeader next = slot->header;
    next.end_lsn    = end_lsn;
    next.data_pages = data_pages;
    next.state      = SEG_DONE;

    uint8_t page_buf[WAL_PAGE_SIZE];
    memset(page_buf, 0, WAL_PAGE_SIZE);
    wal_segment_header_serialize(&next, page_buf);

    if (pwrite_all(slot->fd, page_buf, WAL_PAGE_SIZE, 0) != MYDB_OK) return MYDB_ERR;

    wal_segment_header_deserialize(page_buf, &slot->header);
    return MYDB_OK;
}

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

int wal_segment_pool_pwrite(WalSegmentPool *pool, uint32_t slot_index,
                            uint64_t lsn, const uint8_t *buf, size_t len)
{
    if (!pool || slot_index >= WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;
    if (len == 0) return MYDB_OK;
    if (!buf) return MYDB_ERR;

    const WalSlot *slot = &pool->slots[slot_index];
    if (slot->header.state != SEG_ACTIVE) return MYDB_ERR;
    if (slot->header.segment_no != wal_lsn_segment(lsn)) return MYDB_ERR;

    uint64_t seg_off = lsn & WAL_SEG_MASK;               /* into the segment's log */
    if (len > WAL_SEG_SIZE - seg_off) return MYDB_ERR;   /* past the segment's end */

    return pwrite_all(slot->fd, buf, len, (off_t)(WAL_PAGE_SIZE + seg_off));
}

uint64_t wal_segment_pool_segment_no(const WalSegmentPool *pool, uint32_t slot_index)
{
    assert(pool && slot_index < WAL_SEGMENT_POOL_SLOTS);
    return pool->slots[slot_index].header.segment_no;
}

int wal_segment_pool_fdatasync_segment(WalSegmentPool *pool, uint64_t seg)
{
    if (!pool) return MYDB_ERR;
    int fd = pool->slots[seg % WAL_SEGMENT_POOL_SLOTS].fd;
    return fdatasync(fd) == 0 ? MYDB_OK : MYDB_ERR;
}


/* The slot holding segment seg (any state but FREE), or -1. */
static int slot_of_segment(const WalSegmentPool *pool, uint64_t seg)
{
    for (uint32_t i = 0; i < WAL_SEGMENT_POOL_SLOTS; i++)
        if (pool->slots[i].header.state != SEG_FREE && pool->slots[i].header.segment_no == seg)
            return (int)i;
    return -1;
}

/* Reads the log page holding lsn and checks it belongs there: a valid
 * header whose start_lsn is the page's own LSN (an unwritten page fails
 * the magic check, a stale one has another start_lsn). Returns MYDB_OK,
 * MYDB_ERR_NOT_FOUND if the page is not there, or MYDB_ERR if the read
 * fails. */
static int read_log_page(WalSegmentPool *pool, uint64_t lsn, uint8_t *page, WalPageHeader *ph)
{
    int slot = slot_of_segment(pool, wal_lsn_segment(lsn));
    if (slot < 0) return MYDB_ERR_NOT_FOUND;
    if (wal_segment_pool_read_page(pool, (uint32_t)slot, wal_lsn_page(lsn) + 1, page) != MYDB_OK) return MYDB_ERR;
    if (wal_page_header_deserialize(page, ph) != MYDB_OK) return MYDB_ERR_NOT_FOUND;
    if (ph->start_lsn != (lsn & ~(uint64_t)(WAL_PAGE_SIZE - 1))) return MYDB_ERR_NOT_FOUND;
    return MYDB_OK;
}

/* Reads the record at lsn into *buf (grown as needed) and checks it, as
 * PostgreSQL's reader does: its lsn field is its position, total_len is
 * at least a header and fits before log_end, every page it continues
 * onto is valid and flagged INCOMING, and its CRC holds. Returns MYDB_OK
 * with *out_total, MYDB_ERR_NOT_FOUND if no valid record is there, or
 * MYDB_ERR if a read or an allocation fails. */
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

    /* Record bytes from lsn to log_end: a bad total_len above it is
     * rejected before anything is allocated. */
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
    uint64_t log_end = (active + 1) << WAL_SEG_SHIFT;

    /* Start one segment early when it is still there: the end can lie in
     * it (see the header). */
    uint64_t start_seg = (active > 0 && slot_of_segment(pool, active - 1) >= 0) ? active - 1 : active;

    uint8_t       page[WAL_PAGE_SIZE];
    WalPageHeader ph;

    /* Skip to the start segment's first record, past bytes continuing a
     * record from the segment before. */
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

    /* Walk records until the first that does not check out; pos ends
     * just past the last valid one. */
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
        if (rc != MYDB_OK && rc != MYDB_ERR_NOT_FOUND) return MYDB_ERR;
    }

    /* An end exactly at the active segment's end stays with that slot. */
    int end_slot = slot_of_segment(pool, wal_lsn_segment(pos));
    *out_slot    = (end_slot >= 0) ? (uint32_t)end_slot : best;
    *out_end_lsn = pos;
    return MYDB_OK;
}

/* Writes hdr as slot's header, then caches it. */
static int write_header(WalSegmentPool *pool, uint32_t slot, const WalSegmentHeader *hdr)
{
    uint8_t page_buf[WAL_PAGE_SIZE];
    memset(page_buf, 0, WAL_PAGE_SIZE);
    wal_segment_header_serialize(hdr, page_buf);
    if (pwrite_all(pool->slots[slot].fd, page_buf, WAL_PAGE_SIZE, 0) != MYDB_OK) return MYDB_ERR;
    wal_segment_header_deserialize(page_buf, &pool->slots[slot].header);
    return MYDB_OK;
}

/* Zeroes slot's log from byte `from` of the segment to its end. */
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

    /* 1. Discard every later segment: zeroed, FREE, number cleared. */
    for (uint32_t i = 0; i < WAL_SEGMENT_POOL_SLOTS; i++) {
        WalSegmentHeader h = pool->slots[i].header;
        if (h.state == SEG_FREE || h.segment_no <= seg) continue;
        if (zero_log_from(pool, i, 0) != MYDB_OK) return MYDB_ERR;
        h.state = SEG_FREE; h.segment_no = 0; h.start_lsn = 0; h.end_lsn = 0; h.data_pages = 0;
        if (write_header(pool, i, &h) != MYDB_OK) return MYDB_ERR;
    }

    /* 2. The end's segment is ACTIVE again. */
    if (slot->header.state != SEG_ACTIVE) {
        WalSegmentHeader h = slot->header;
        h.state = SEG_ACTIVE; h.end_lsn = 0; h.data_pages = 0;
        if (write_header(pool, slot_index, &h) != MYDB_OK) return MYDB_ERR;
    }

    /* 3. Zero everything after end_lsn. */
    if (wal_lsn_segment(end_lsn) == seg &&
        zero_log_from(pool, slot_index, end_lsn & WAL_SEG_MASK) != MYDB_OK)
        return MYDB_ERR;

    /* 4. Sync it all. */
    for (uint32_t i = 0; i < WAL_SEGMENT_POOL_SLOTS; i++)
        if (pool->slots[i].header.state != SEG_FREE && fdatasync(pool->slots[i].fd) < 0)
            return MYDB_ERR;

    /* 5. The next claim is the segment after the end's. */
    pool->next_segment_no = seg + 1;
    return MYDB_OK;
}

