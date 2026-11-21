#ifndef PAGE_H
#define PAGE_H

#include "common.h"
#include <assert.h>
/*
 * page.h — InnoDB-style slotted page format
 *
 * Operates entirely on an in-memory 16KB buffer (uint8_t[PAGE_SIZE]).
 * No disk I/O here — that is the buffer pool's job.
 *
 * Page memory layout:
 *
 *   [0     .. 37 ]  PageHeader   (38 bytes)
 *   [38    .. 50 ]  Infimum record  (5B header + 8B data = 13B)
 *   [51    .. 63 ]  Supremum record (5B header + 8B data = 13B)
 *   [64    .. ->  ]  User records, grow toward higher offsets
 *   [           ]  Free space in the middle
 *   [ <-  .. end]  Page directory, 2-byte slots grow from the bottom up
 *   [16376 ..16383]  PageTrailer (8 bytes)
 *
 * Records are linked in key order via the next_offset field in each
 * record header, forming a singly-linked list: Infimum → r1 → r2 → … → Supremum.
 *
 * Page directory: one 2-byte slot per record, stored as the byte offset
 * of that record's data (i.e., just after its 5-byte record header).
 * Slot 0 is at offset PAGE_SIZE-8-2, slot 1 at PAGE_SIZE-8-4, etc.
 */

/* ------------------------------------------------------------------ */
/*  Page header — 38 bytes at offset 0                                 */
/* ------------------------------------------------------------------ */
typedef struct {
    uint32_t checksum;       /* CRC-32C checksum over bytes [4 .. PAGE_SIZE-9] */
    uint32_t page_no;        /* this page's number in the file */
    uint32_t prev_page;      /* previous leaf page (doubly-linked leaf chain) */
    uint32_t next_page;      /* next leaf page */
    uint64_t lsn;            /* log sequence number (0 in Phase 1 — no WAL) */
    uint16_t page_type;      /* PageType enum */
    uint16_t num_records;    /* count of non-deleted user records */
    uint16_t free_offset;    /* byte offset where the next record will be written */
    uint16_t garbage_offset; /* head of the deleted-record free list (0 = none) */
    uint16_t num_dir_slots;  /* number of entries in the page directory */
} PageHeader;

/* ------------------------------------------------------------------ */
/*  Page trailer — 8 bytes at PAGE_SIZE - 8                            */
/* ------------------------------------------------------------------ */
typedef struct {
    uint32_t old_checksum;  /* duplicate of header checksum (InnoDB convention) */
    uint32_t lsn_low32;     /* low 32 bits of lsn */
} PageTrailer;

/*
 * Record header — 5 bytes stored immediately before each record's data.
 *
 * MyDB record header (5 bytes):
 *   Byte 0   : info_flags
 *   Bytes 1-2: (heap_no << 3) | rec_type  as big-endian uint16
 *   Bytes 3-4: next_data_offset as big-endian uint16
 */
typedef struct {
    uint8_t  info_flags;     /* bit 0 = deleted, bit 1 = min_rec */
    uint16_t heap_type;      /* bits[15:3] = heap_no, bits[2:0] = RecordType */
    uint16_t next_offset;    /* absolute byte offset of next record's data in this page */
} RecordHeader;

/* Offsets of fixed structures within the page */
#define INFIMUM_OFFSET   PAGE_HEADER_SIZE                          /* 38 */
#define SUPREMUM_OFFSET  (PAGE_HEADER_SIZE + 13)                   /* 51 */
#define INFIMUM_DATA     (INFIMUM_OFFSET  + RECORD_HEADER_SIZE)    /* 43 */
#define SUPREMUM_DATA    (SUPREMUM_OFFSET + RECORD_HEADER_SIZE)    /* 56 */

/* Page directory slot i is at this byte offset inside the page */
#define DIR_SLOT_OFFSET(i)  (PAGE_SIZE - PAGE_TRAILER_SIZE - 2 - (i) * 2)

/* ------------------------------------------------------------------ */
/*  Functions                                                           */
/* ------------------------------------------------------------------ */

/*
 * Initialize a fresh 16KB page buffer.
 * Sets up the page header, infimum, and supremum records.
 * The caller has already zero-filled the buffer (disk_alloc_page does this).
 */
int page_init(uint8_t *page, uint32_t page_no, PageType type);

/* Read the page header fields from the raw buffer into a PageHeader struct. */
void page_read_header(const uint8_t *page, PageHeader *hdr);

/* Write a PageHeader struct back into the raw buffer. */
void page_write_header(uint8_t *page, const PageHeader *hdr);

/*
 * Insert a record into the page at a specific slot index.
 *
 * The page directory is maintained in KEY ORDER: slot 0 holds the
 * smallest key, slot num_dir_slots-1 holds the largest. Callers
 * (typically the B+ tree) determine the correct insertion slot via a
 * key-ordered search and pass it here.
 *
 *   record_data : the payload bytes (NOT including the 5-byte record header)
 *   record_size : number of payload bytes
 *   at_slot     : insert here. Existing slots [at_slot .. n-1] are shifted
 *                 right by one. Pass 0 to insert before the current first
 *                 record, num_dir_slots to append.
 *
 * The linked list is also maintained: the new record is spliced after
 * the record at slot (at_slot - 1), or after Infimum if at_slot == 0.
 *
 * Returns MYDB_ERR_FULL if there is not enough free space.
 * Returns MYDB_ERR if at_slot > num_dir_slots.
 */
int page_insert_record(uint8_t *page,
                       const uint8_t *record_data, uint16_t record_size,
                       uint16_t at_slot);

/*
 * Mark the record at slot_no as deleted (lazy deletion).
 * Space is NOT reclaimed immediately; call page_compact() to do that.
 */
int page_delete_record(uint8_t *page, uint16_t slot_no);

/*
 * Get the byte offset and size of the record data at slot_no.
 * Returns MYDB_ERR_NOT_FOUND if the slot is deleted or out of range.
 */
int page_get_record(const uint8_t *page, uint16_t slot_no,
                    uint16_t *data_offset, uint16_t *data_size);

/* Return the number of bytes of free space currently available in the page. */
uint16_t page_free_space(const uint8_t *page);

/*
 * Compact the page: physically remove all deleted records, reclaim space.
 * Rewrites the page in-place. The page directory is rebuilt from scratch.
 * All live records retain their data but get new slot numbers.
 */
int page_compact(uint8_t *page);

/* Read the absolute data offset stored in page directory slot i. */
uint16_t page_dir_get(const uint8_t *page, uint16_t i);

/* Return the number of page directory slots (same as num_records). */
uint16_t page_dir_count(const uint8_t *page);

/* Encode a RecordHeader into exactly 5 bytes at dst. */
void rec_hdr_encode(const RecordHeader *rh, uint8_t *dst);

/* Decode a RecordHeader from 5 bytes at src. */
void rec_hdr_decode(const uint8_t *src, RecordHeader *rh);

/*
 * Compute and write the CRC-32C checksum for the page.
 * Call this before writing a page to disk.
 */
void page_set_checksum(uint8_t *page);

/*
 * Verify the checksum stored in the page header.
 * Returns MYDB_OK if valid, MYDB_ERR if corrupted.
 */
int page_verify_checksum(const uint8_t *page);



void page_check_invariants(const uint8_t *page);

#endif /* PAGE_H */
