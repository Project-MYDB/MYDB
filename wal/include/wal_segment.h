#ifndef WAL_SEGMENT_H
#define WAL_SEGMENT_H

#include <stdint.h>
#include "common.h"
#include "file_header.h"

/*
 * wal_segment.h — WAL segment header format only (MYDB_WAL_DESIGN.md
 * §8.1, MYDB_WAL_IMPLEMENTATION.md §8.4). wal/'s own struct — see
 * wal_types.h for why (system-wide constants/enums live in common.h; a
 * subsystem's own record/container layout does not).
 *
 * Page-level format (WalPageHeader) lives in wal_page.h, not here; the
 * in-memory runtime handle (WalSlot) and the pool of 10 physical segment
 * files (fallocate, rotation, raw page I/O) live in wal_segment_pool.h —
 * each file owns only its own concern, the same single-responsibility
 * split every other module in this codebase follows.
 *
 * WalSegmentHeader is a plain struct (not __attribute__((packed))) — see
 * wal_types.h's header comment for why: the on-disk byte layout is
 * produced/consumed field-by-field in wal_segment.c via explicit
 * offsets, not via this struct's in-memory layout, matching the rest of
 * the codebase's convention.
 */

/* ------------------------------------------------------------------
 * Segment lifecycle: SEG_FREE -> SEG_ACTIVE -> SEG_DONE -> SEG_ARCHIVING
 * -> SEG_FREE. The header is rewritten only at these state transitions,
 * never per-page-append (impl doc §8.4) — recovery tail-scans a
 * SEG_ACTIVE segment rather than trusting its on-disk end_lsn/data_pages.
 * ------------------------------------------------------------------ */
typedef enum {
    SEG_FREE      = 0,
    SEG_ACTIVE    = 1,
    SEG_DONE      = 2,
    SEG_ARCHIVING = 3
} WalSegState;

/* ------------------------------------------------------------------
 * WalSegmentHeader — on-disk shape is 48 bytes. Occupies page 0 of each
 * segment file, padded to a 64-byte on-disk footprint (16 bytes reserved
 * for future extension) — see WAL_SEGMENT_HEADER_ON_DISK_SIZE below.
 * ------------------------------------------------------------------ */
typedef struct {
    FileHeaderId id;
    uint64_t segment_no;       /* monotonic, never reused                */
    uint64_t start_lsn;
    uint64_t end_lsn;
    uint32_t partition_id;
    uint32_t data_pages;       /* valid 4KB pages written                */
    uint8_t  state;            /* WalSegState                            */
    uint8_t  reserved[3];
    uint32_t checksum;         /* CRC32 over the preceding 44 bytes      */
} WalSegmentHeader;

#define WAL_SEGMENT_HEADER_ON_DISK_SIZE  64   /* on-disk footprint: 48 documented bytes + 16 padding */

/* ------------------------------------------------------------------
 * Serialize/deserialize + checksum.
 *
 * wal_segment_header_serialize fills a WAL_SEGMENT_HEADER_ON_DISK_SIZE
 * (64-byte) buffer from *hdr — the trailing 16 padding bytes are
 * zeroed, matching the on-disk footprint the segment-file-I/O phase will
 * actually write — and stamps its checksum field (CRC32 over the bytes
 * preceding it). Caller sets every field of *hdr except checksum first.
 *
 * wal_segment_header_deserialize validates the FileHeaderId (magic/
 * version/file_type) via file_header_check_id, then the checksum, before
 * filling *out. Returns MYDB_OK, or MYDB_ERR_BAD_MAGIC/BAD_VERSION/
 * BAD_FILE_TYPE/BAD_CHECKSUM.
 * ------------------------------------------------------------------ */
void wal_segment_header_serialize(const WalSegmentHeader *hdr, uint8_t *buf);
int  wal_segment_header_deserialize(const uint8_t *buf, WalSegmentHeader *out);

#endif /* WAL_SEGMENT_H */
