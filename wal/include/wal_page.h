#ifndef WAL_PAGE_H
#define WAL_PAGE_H

#include <stdint.h>
#include "common.h"
#include "file_header.h"

/*
 * wal_page.h — WAL page header format; it frames the WAL's 4KB segment
 * pages and ring-buffer frames.
 * WalPageHeader is a plain struct (not __attribute__((packed))) — the
 * on-disk byte layout is produced/consumed field-by-field in wal_page.c
 * via explicit offsets, not via this struct's in-memory layout, matching
 * the rest of the codebase's convention.
 */

/* ------------------------------------------------------------------
 * Page flags. They describe how this page's content relates to record
 * boundaries. Records span pages and segments; the copier that opens a
 * page for the rest of a record sets the IN flag (V1's
 * wal_ring_buffer_append did this until it was retired; V2's wal_insert
 * takes over).
 *
 * There is no outgoing flag: page k runs into page k + 1 exactly when
 * page k + 1 has IN set.
 * ------------------------------------------------------------------ */
#define WAL_PAGE_INCOMING_CONTINUATION 0x01   /* this page starts mid-record:
     its first continuation_len bytes continue a record whose header is
     on an earlier page */


/* ------------------------------------------------------------------
 * WalPageHeader: 24 bytes on disk. Leads every 4KB WAL segment page and
 * every ring frame in memory (the two are byte-identical).
 *
 *   id                0-7    magic, version, file_type
 *   start_lsn         8-15   LSN of this page's own byte 0
 *   continuation_len  16-17  leading bytes that continue an earlier record
 *   flags             18     WAL_PAGE_INCOMING_CONTINUATION
 *   reserved          19
 *   checksum          20-23  CRC-32C over bytes 0-19
 *
 * start_lsn is fixed by where the page sits, so its only job is the
 * stale check: a page left over from an earlier lap of a reused segment
 * carries a different start_lsn than the position it is read at.
 *
 * The first record header on a page is at WAL_PAGE_HEADER_SIZE +
 * continuation_len, so a reader can start from any page.
 *
 * There is no data_len. A reader finds where the log stops by walking
 * records: the first record whose header does not check out (lsn field
 * not equal to its position, a bad total_len, or a failed CRC) is the
 * end. That also catches bytes left in a ring frame or a reused segment
 * from an earlier lap, since their lsn field is wrong.
 * ------------------------------------------------------------------ */
typedef struct {
    FileHeaderId id;            /* magic, version, file_type — 8 bytes    */
    uint64_t     start_lsn;     /* LSN of this page's own byte 0           */
    uint16_t     continuation_len;      /* leading bytes continuing an earlier record */
    uint8_t      flags;         /* WAL_PAGE_INCOMING_CONTINUATION          */
    uint8_t      reserved[1];
    uint32_t     checksum;      /* CRC-32C over the preceding 20 bytes    */
} WalPageHeader;

/* ------------------------------------------------------------------
 * Serialize/deserialize + checksum.
 *
 * wal_page_header_serialize fills buf[0..WAL_PAGE_HEADER_SIZE) from *hdr
 * and stamps buf's checksum field (CRC-32C over the bytes preceding it).
 * Caller sets every field of *hdr except checksum first.
 *
 * wal_page_header_deserialize validates the FileHeaderId (magic/version/
 * file_type) via file_header_check_id, then the checksum, before filling
 * *out. Returns MYDB_OK, or MYDB_ERR_BAD_MAGIC/BAD_VERSION/BAD_FILE_TYPE/
 * BAD_CHECKSUM.
 * ------------------------------------------------------------------ */
void wal_page_header_serialize(const WalPageHeader *hdr, uint8_t *buf);
int  wal_page_header_deserialize(const uint8_t *buf, WalPageHeader *out);

/* ------------------------------------------------------------------
 * LSN arithmetic (MYDB_WAL_DESIGN_V2.md §4). The LSN is a position in
 * the log. A segment file is one 4KB header page (its WalSegmentHeader)
 * followed by exactly 4MB of log (1024 pages), and the header page is
 * NOT part of the LSN (V2 §4, rule 6):
 *
 *     segment N covers LSN [N << 22, (N + 1) << 22)
 *     byte offset in its file = 4096 + (lsn & WAL_SEG_MASK)
 *
 * (The write step in wal_flush.c computes that file offset itself; the
 * mask matters, since 4096 + lsn is only right for segment 0.)
 *
 * So every 4KB of LSN is a log page with a 24-byte page header, the
 * first record of the log is at LSN 24, and page k of the log (counted
 * across the whole log) starts at k << 12. Finding the segment, page,
 * offset, file offset or ring frame of an LSN is shifts and masks.
 *
 * ------------------------------------------------------------------ */
#define WAL_SEG_SHIFT         22
#define WAL_SEG_SIZE          (1u << WAL_SEG_SHIFT)            /* 4MB of log per segment */
#define WAL_SEG_MASK          ((uint64_t)WAL_SEG_SIZE - 1)
#define WAL_PAGE_SHIFT        12
#define WAL_SEG_PAGES         (WAL_SEG_SIZE / WAL_PAGE_SIZE)   /* 1024 */
#define WAL_FIRST_RECORD_LSN  ((uint64_t)WAL_PAGE_HEADER_SIZE)  /* 24 */

static inline uint64_t wal_lsn_segment(uint64_t lsn) { return lsn >> WAL_SEG_SHIFT; }

/* Page within the segment, 0..1023. */
static inline uint32_t wal_lsn_page(uint64_t lsn)
{ return (uint32_t)((lsn >> WAL_PAGE_SHIFT) & (WAL_SEG_PAGES - 1)); }

static inline uint32_t wal_lsn_offset(uint64_t lsn) { return (uint32_t)(lsn & (WAL_PAGE_SIZE - 1)); }

/* First record position of a segment: its page 0, past the page header. */
static inline uint64_t wal_segment_first_lsn(uint64_t seg)
{ return (seg << WAL_SEG_SHIFT) + WAL_FIRST_RECORD_LSN; }

/* (wal_data_page_base, wal_lsn_data_page and wal_lsn_next_data used to
 * sit here. Under rule 6 the first two are a bare << 12 and >> 12, written
 * inline below, and the third, which stepped the V1 ring buffer from a
 * page end past the next page's header, has no user: V2's insert path
 * converts from a pos instead.) */

/* Record bytes that come before a data position, counted over the whole
 * log (V2's pos). Was wal_lsn_data_ord. */
static inline uint64_t wal_lsn_to_byte(uint64_t lsn)
{ return (lsn >> WAL_PAGE_SHIFT) * WAL_PAGE_USABLE + (wal_lsn_offset(lsn) - WAL_PAGE_HEADER_SIZE); }

/* Inverse of wal_lsn_to_byte: the LSN of record byte pos. The one
 * division in the LSN math (V2 §4). Was wal_data_ord_lsn. */
static inline uint64_t wal_byte_to_lsn(uint64_t byte_pos)
{ return ((byte_pos / WAL_PAGE_USABLE) << WAL_PAGE_SHIFT) + WAL_PAGE_HEADER_SIZE + byte_pos % WAL_PAGE_USABLE; }

/* The LSN just past n record bytes placed from data position lsn. When
 * they end exactly at a page end the result is that page end (the next
 * page's base, or the next segment's start), not the next page's offset
 * 24 — the writer opens the next page only when it has bytes for it. */
static inline uint64_t wal_lsn_advance(uint64_t lsn, uint64_t n)
{
    if (n == 0) return lsn;
    uint64_t d_end = wal_lsn_to_byte(lsn) + n;
    uint64_t k     = (d_end - 1) / WAL_PAGE_USABLE;         /* page of the last byte */
    return (k << WAL_PAGE_SHIFT) + WAL_PAGE_HEADER_SIZE + ((d_end - 1) % WAL_PAGE_USABLE) + 1;
}

#endif /* WAL_PAGE_H */
