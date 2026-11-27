#ifndef WAL_SEGMENT_H
#define WAL_SEGMENT_H

#include <stdint.h>
#include "common.h"
#include "file_header.h"

/*
 * wal_segment.h — the header stored in page 0 of every segment file.
 * Written field by field at fixed offsets by wal_segment.c (the struct's
 * own layout is not used). It is rewritten only when the segment changes
 * state, never per page written: the end of an ACTIVE segment is found
 * by walking its records (wal_segment_pool_tail_scan).
 */

/* A segment's life: FREE -> ACTIVE -> DONE -> ARCHIVING -> FREE. */
typedef enum {
    SEG_FREE      = 0,
    SEG_ACTIVE    = 1,
    SEG_DONE      = 2,
    SEG_ARCHIVING = 3
} WalSegState;

/* 48 bytes, written into a 64-byte area whose last 16 bytes are zero. */
typedef struct {
    FileHeaderId id;
    uint64_t segment_no;       /* the segment this file holds now       */
    uint64_t start_lsn;        /* its first record position             */
    uint64_t end_lsn;          /* where its log ends, set when DONE     */
    uint32_t partition_id;
    uint32_t data_pages;       /* log pages written, set when DONE      */
    uint8_t  state;            /* WalSegState                           */
    uint8_t  reserved[3];
    uint32_t checksum;         /* CRC-32C over the preceding 44 bytes   */
} WalSegmentHeader;

#define WAL_SEGMENT_HEADER_ON_DISK_SIZE  64

/* Writes *hdr into the 64-byte buf (padding zeroed) and stamps the
 * checksum. The caller sets every field but checksum. */
void wal_segment_header_serialize(const WalSegmentHeader *hdr, uint8_t *buf);

/* Checks the id and the checksum, then fills *out. Returns MYDB_OK, or
 * MYDB_ERR_BAD_MAGIC / BAD_VERSION / BAD_FILE_TYPE / BAD_CHECKSUM. */
int  wal_segment_header_deserialize(const uint8_t *buf, WalSegmentHeader *out);

#endif /* WAL_SEGMENT_H */
