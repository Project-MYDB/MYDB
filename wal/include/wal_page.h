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
 * boundaries. Records span pages and segments; wal_ring_buffer_append
 * sets the IN flag when it opens a page for the rest of a record.
 *
 * There is no outgoing flag: page k runs into page k + 1 exactly when
 * page k + 1 has IN set.
 * ------------------------------------------------------------------ */
#define WAL_PAGE_INCOMING_CONTINUATION 0x01   /* this page starts mid-record:
     its first continuation_len bytes continue a record whose header is
     on an earlier page */


/* ------------------------------------------------------------------
 * WalPageHeader: 24 bytes on disk. Leads every 4KB WAL segment page and
 * every ring buffer frame (the two are byte-identical).
 *
 *   id                0-7    magic, version, file_type
 *   start_lsn         8-15   LSN of this page's own byte 0
 *   continuation_len  16-17  leading bytes that continue an earlier record
 *   flags             18     WAL_PAGE_INCOMING_CONTINUATION
 *   reserved          19
 *   checksum          20-23  CRC32 over bytes 0-19
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
    uint32_t     checksum;      /* CRC32 over the preceding 20 bytes      */
} WalPageHeader;

/* ------------------------------------------------------------------
 * Serialize/deserialize + checksum.
 *
 * wal_page_header_serialize fills buf[0..WAL_PAGE_HEADER_SIZE) from *hdr
 * and stamps buf's checksum field (CRC32 over the bytes preceding it).
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
 * LSN arithmetic (T-039). The LSN is a position in the log:
 *
 *     LSN = segment_no * WAL_SEG_SIZE + byte offset in that segment file
 *
 * Page headers and each segment's page 0 (its WalSegmentHeader) count,
 * so the first record of segment 0 is at 4096 + 24 = 4120. The segment
 * and page sizes are powers of two, so finding the segment, page and
 * offset of an LSN is shifts and masks.
 *
 * Data pages are pages 1..511 of each segment. Numbering them across
 * the whole log, k = segment * 511 + (page - 1), is what maps a page to
 * a ring frame (k % WAL_RING_BUFFER_FRAMES): page 0 never enters the
 * ring, so counting it would leave a hole in the ring every 512 pages.
 *
 * A "data position" is an LSN at or past a page's header (offset >= 24).
 * A page end (offset 0) is also a valid place for the writer to stop
 * after a page filled exactly; the next byte then goes at the next data
 * page's offset 24 (wal_lsn_next_data). Offsets 1..23 never occur.
 * ------------------------------------------------------------------ */
#define WAL_SEG_SIZE          (2u * 1024u * 1024u)    /* = WAL_SEGMENT_FILE_SIZE */
#define WAL_SEG_SHIFT         21
#define WAL_PAGE_SHIFT        12
#define WAL_SEG_PAGES         (WAL_SEG_SIZE / WAL_PAGE_SIZE)   /* 512 */
#define WAL_SEG_DATA_PAGES    (WAL_SEG_PAGES - 1)              /* 511 */
#define WAL_FIRST_RECORD_LSN  ((uint64_t)WAL_PAGE_SIZE + WAL_PAGE_HEADER_SIZE)   /* 4120 */

static inline uint64_t wal_lsn_segment(uint64_t lsn) { return lsn >> WAL_SEG_SHIFT; }

static inline uint32_t wal_lsn_page(uint64_t lsn)
{ return (uint32_t)((lsn >> WAL_PAGE_SHIFT) & (WAL_SEG_PAGES - 1)); }

static inline uint32_t wal_lsn_offset(uint64_t lsn) { return (uint32_t)(lsn & (WAL_PAGE_SIZE - 1)); }

/* First record position of a segment: its page 1, past the page header. */
static inline uint64_t wal_segment_first_lsn(uint64_t seg)
{ return seg * WAL_SEG_SIZE + WAL_FIRST_RECORD_LSN; }

/* LSN of data page k's byte 0. */
static inline uint64_t wal_data_page_base(uint64_t k)
{
    return (k / WAL_SEG_DATA_PAGES) * WAL_SEG_SIZE
         + (uint64_t)(k % WAL_SEG_DATA_PAGES + 1) * WAL_PAGE_SIZE;
}

/* Data page number of a data position (offset >= 24, page >= 1). */
static inline uint64_t wal_lsn_data_page(uint64_t lsn)
{ return wal_lsn_segment(lsn) * WAL_SEG_DATA_PAGES + (wal_lsn_page(lsn) - 1); }

/* The data position where the next byte goes: lsn itself, or, at a page
 * end, the next data page's offset 24 (stepping over page 0 when the end
 * is a segment end). */
static inline uint64_t wal_lsn_next_data(uint64_t lsn)
{
    if (wal_lsn_offset(lsn) >= WAL_PAGE_HEADER_SIZE) return lsn;
    if (wal_lsn_page(lsn) == 0) return lsn + WAL_FIRST_RECORD_LSN;   /* segment start */
    return lsn + WAL_PAGE_HEADER_SIZE;
}

/* Record bytes that come before a data position, counted over the whole log. */
static inline uint64_t wal_lsn_data_ord(uint64_t lsn)
{ return wal_lsn_data_page(lsn) * WAL_PAGE_USABLE + (wal_lsn_offset(lsn) - WAL_PAGE_HEADER_SIZE); }

/* Inverse of wal_lsn_data_ord. */
static inline uint64_t wal_data_ord_lsn(uint64_t d)
{ return wal_data_page_base(d / WAL_PAGE_USABLE) + WAL_PAGE_HEADER_SIZE + d % WAL_PAGE_USABLE; }

/* The LSN just past n record bytes placed from data position lsn. When
 * they end exactly at a page end the result is that page end (the next
 * page's base, or the next segment's start), not the next page's offset
 * 24 — the writer opens the next page only when it has bytes for it. */
static inline uint64_t wal_lsn_advance(uint64_t lsn, uint64_t n)
{
    if (n == 0) return lsn;
    uint64_t d_end = wal_lsn_data_ord(lsn) + n;
    uint64_t k     = (d_end - 1) / WAL_PAGE_USABLE;         /* page of the last byte */
    return wal_data_page_base(k) + WAL_PAGE_HEADER_SIZE + ((d_end - 1) % WAL_PAGE_USABLE) + 1;
}

#endif /* WAL_PAGE_H */
