#ifndef WAL_PAGE_H
#define WAL_PAGE_H

#include <stdint.h>
#include "common.h"
#include "file_header.h"

/*
 * wal_page.h — the 24-byte header that starts every 4KB log page, and
 * the LSN arithmetic (MYDB_WAL_DESIGN_V2.md §4).
 *
 * The LSN is a byte position in the log. A segment file is one 4KB
 * header page followed by 16MB of log, and the header page is not part of
 * the LSN: segment N holds LSN [N << 24, (N + 1) << 24), at file offset
 * 4096 + (lsn & WAL_SEG_MASK). Every 4KB of LSN is one log page, which
 * starts with a page header, so the first record of the log is at LSN 24.
 */

/* The page starts in the middle of a record: its first continuation_len
 * bytes belong to a record whose header is on an earlier page. */
#define WAL_PAGE_INCOMING_CONTINUATION 0x01

/* The header of one log page, 24 bytes on disk, written field by field
 * at fixed offsets by wal_page.c (the struct's own layout is not used):
 *
 *   id                0-7    magic, version, file_type
 *   start_lsn         8-15   LSN of the page's byte 0
 *   continuation_len  16-17  leading bytes that continue an earlier record
 *   flags             18     WAL_PAGE_INCOMING_CONTINUATION
 *   reserved          19
 *   checksum          20-23  CRC-32C over bytes 0-19
 *
 * start_lsn lets a reader reject a stale page left from an earlier pass
 * through a reused segment. The first record header on a page is at 24 +
 * continuation_len, so a reader can start at any page. */
typedef struct {
    FileHeaderId id;                /* magic, version, file_type: 8 bytes    */
    uint64_t     start_lsn;         /* LSN of the page's byte 0              */
    uint16_t     continuation_len;  /* leading bytes continuing a record     */
    uint8_t      flags;             /* WAL_PAGE_INCOMING_CONTINUATION        */
    uint8_t      reserved[1];
    uint32_t     checksum;          /* CRC-32C over the preceding 20 bytes   */
} WalPageHeader;

/* Writes *hdr into buf[0..24) and stamps the checksum. The caller sets
 * every field but checksum. */
void wal_page_header_serialize(const WalPageHeader *hdr, uint8_t *buf);

/* Checks the id and the checksum, then fills *out. Returns MYDB_OK, or
 * MYDB_ERR_BAD_MAGIC / BAD_VERSION / BAD_FILE_TYPE / BAD_CHECKSUM. */
int  wal_page_header_deserialize(const uint8_t *buf, WalPageHeader *out);

#define WAL_SEG_SHIFT         24
#define WAL_SEG_SIZE          (1u << WAL_SEG_SHIFT)            /* 16MB of log per segment */
#define WAL_SEG_MASK          ((uint64_t)WAL_SEG_SIZE - 1)
#define WAL_PAGE_SHIFT        12
#define WAL_SEG_PAGES         (WAL_SEG_SIZE / WAL_PAGE_SIZE)   /* 4096 */
#define WAL_FIRST_RECORD_LSN  ((uint64_t)WAL_PAGE_HEADER_SIZE)  /* 24 */

/* The segment holding lsn. */
static inline uint64_t wal_lsn_segment(uint64_t lsn) { return lsn >> WAL_SEG_SHIFT; }

/* The page within its segment, 0..1023. */
static inline uint32_t wal_lsn_page(uint64_t lsn)
{ return (uint32_t)((lsn >> WAL_PAGE_SHIFT) & (WAL_SEG_PAGES - 1)); }

/* The offset within its page, 0..4095. */
static inline uint32_t wal_lsn_offset(uint64_t lsn) { return (uint32_t)(lsn & (WAL_PAGE_SIZE - 1)); }

/* A segment's first record position: its first page, past the header. */
static inline uint64_t wal_segment_first_lsn(uint64_t seg)
{ return (seg << WAL_SEG_SHIFT) + WAL_FIRST_RECORD_LSN; }

/* Record bytes in the whole log before lsn, page headers not counted.
 * lsn must be a record position (page offset >= 24). */
static inline uint64_t wal_lsn_to_byte(uint64_t lsn)
{ return (lsn >> WAL_PAGE_SHIFT) * WAL_PAGE_USABLE + (wal_lsn_offset(lsn) - WAL_PAGE_HEADER_SIZE); }

/* The LSN of record byte byte_pos: the inverse of wal_lsn_to_byte. */
static inline uint64_t wal_byte_to_lsn(uint64_t byte_pos)
{ return ((byte_pos / WAL_PAGE_USABLE) << WAL_PAGE_SHIFT) + WAL_PAGE_HEADER_SIZE + byte_pos % WAL_PAGE_USABLE; }

/* The LSN just past n record bytes placed from lsn. When they end exactly
 * at a page end, the result is that page end, not the next page's offset
 * 24: the next page is opened only when there are bytes for it. */
static inline uint64_t wal_lsn_advance(uint64_t lsn, uint64_t n)
{
    if (n == 0) return lsn;
    uint64_t d_end = wal_lsn_to_byte(lsn) + n;
    uint64_t k     = (d_end - 1) / WAL_PAGE_USABLE;         /* page of the last byte */
    return (k << WAL_PAGE_SHIFT) + WAL_PAGE_HEADER_SIZE + ((d_end - 1) % WAL_PAGE_USABLE) + 1;
}

#endif /* WAL_PAGE_H */
