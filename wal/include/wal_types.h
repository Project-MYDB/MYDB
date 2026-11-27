#ifndef WAL_TYPES_H
#define WAL_TYPES_H

#include <stddef.h>
#include <stdint.h>
#include "common.h"

/*
 * wal_types.h — the log record header and the fixed-shape record bodies.
 *
 * Every field of WalRecordHeader sits at an offset that is a multiple of
 * its size, so the struct's first WAL_RECORD_HEADER_SIZE (44) bytes are
 * the on-disk bytes and are copied with one memcpy, as PostgreSQL does
 * with XLogRecord. The _Static_asserts below pin every offset.
 * INSERT and DELETE bodies are variable-length and packed by hand.
 */

/* The header in front of every log record, 44 bytes on disk:
 *
 *   lsn        0-7     prev_lsn   8-15    txn_id     16-23
 *   total_len  24-27   table_id   28-31   page_no    32-35
 *   rec_type   36      flags      37      reserved   38-39
 *   checksum   40-43
 *
 * sizeof is 48 (end padding to 8), so use WAL_RECORD_HEADER_SIZE for the
 * on-disk size. Host byte order. */
typedef struct {
    uint64_t lsn;              /* this record's LSN                       */
    uint64_t prev_lsn;         /* previous LSN for this txn (0 = first)   */
    uint64_t txn_id;
    uint32_t total_len;        /* full record length including header     */
    uint32_t table_id;         /* persistent, on-disk-stable identifier   */
    uint32_t page_no;
    uint8_t  rec_type;         /* WalRecType, stored as a byte            */
    uint8_t  flags;            /* WAL_RECORD_FLAG_*                       */
    uint8_t  reserved[2];
    uint32_t checksum;         /* CRC-32C over body + header[0..40)       */
} WalRecordHeader;

/* Moving or resizing a field fails the build instead of changing the
 * on-disk format. */
_Static_assert(offsetof(WalRecordHeader, lsn)       == 0,  "lsn at 0");
_Static_assert(offsetof(WalRecordHeader, prev_lsn)  == 8,  "prev_lsn at 8");
_Static_assert(offsetof(WalRecordHeader, txn_id)    == 16, "txn_id at 16");
_Static_assert(offsetof(WalRecordHeader, total_len) == 24, "total_len at 24");
_Static_assert(offsetof(WalRecordHeader, table_id)  == 28, "table_id at 28");
_Static_assert(offsetof(WalRecordHeader, page_no)   == 32, "page_no at 32");
_Static_assert(offsetof(WalRecordHeader, rec_type)  == 36, "rec_type at 36");
_Static_assert(offsetof(WalRecordHeader, flags)     == 37, "flags at 37");
_Static_assert(offsetof(WalRecordHeader, reserved)  == 38, "reserved at 38");
_Static_assert(offsetof(WalRecordHeader, checksum)  == 40, "checksum at 40");
_Static_assert(offsetof(WalRecordHeader, checksum) + sizeof(uint32_t) == WAL_RECORD_HEADER_SIZE,
               "the on-disk header is the struct's first WAL_RECORD_HEADER_SIZE bytes");

/* The record does not fit on the page its header is on and continues on
 * the next page. Not set for a record that ends exactly at the page end. */
#define WAL_RECORD_FLAG_CONTINUES_ON_NEW_PAGE 0x01

/* The checksum is CRC-32C over the body, then the header's first 40
 * bytes. Body first, so the body's part can be computed before the record
 * has an LSN (V2 §8.3). */

/* Writes the header and the body into buf and stamps hdr->checksum. The
 * caller sets every field but checksum. */
void wal_record_header_serialize(WalRecordHeader *hdr, const void *body,
                                  size_t body_len, uint8_t *buf);

/* Writes only the 44-byte header into out, finishing the checksum from
 * body_checksum, and stamps hdr->checksum. body_checksum is the running
 * CRC state over the body, crc32c_update(CRC32C_INIT, body, len), or
 * CRC32C_INIT for no body: the session computes it before wal_insert. */
void wal_record_header_pack_crc(WalRecordHeader *hdr, uint32_t body_checksum,
                                 uint8_t out[WAL_RECORD_HEADER_SIZE]);

/* Checks the checksum over the header and the body_len bytes after it,
 * then fills *out. Returns MYDB_OK or MYDB_ERR_BAD_CHECKSUM. */
int  wal_record_header_deserialize(const uint8_t *buf, size_t body_len,
                                    WalRecordHeader *out);

/* ------------------------------------------------------------------
 * Fixed-shape record bodies.
 * ------------------------------------------------------------------ */

/* WAL_REC_COMMIT body */
typedef struct {
    uint64_t commit_timestamp;
} WalRecCommitBody;

/* WAL_REC_CLR body: makes undo resumable after a crash.
 * next_undo_lsn == 0 means the transaction is fully undone. */
typedef struct {
    uint64_t undone_lsn;
    uint64_t next_undo_lsn;
} WalRecClrBody;


/* WAL_REC_FILE_HEADER_UPDATE body: a change to a table file header's
 * page count or free-page bitmap, chosen by field. */
typedef enum {
    WAL_FHU_FIELD_NUM_PAGES        = 0,
    WAL_FHU_FIELD_FREE_PAGE_BITMAP = 1
} WalFileHeaderUpdateField;

typedef struct {
    uint8_t field;              /* WalFileHeaderUpdateField */
    uint8_t reserved[3];
    union {
        struct {                /* WAL_FHU_FIELD_NUM_PAGES */
            uint32_t old_val;
            uint32_t new_val;
        } num_pages;
        struct {                /* WAL_FHU_FIELD_FREE_PAGE_BITMAP */
            uint32_t page_no;
            uint8_t  old_bit;
            uint8_t  new_bit;
        } free_bitmap;
    } u;
} WalRecFileHeaderUpdateBody;

/* WAL_REC_CHECKPOINT body: a snapshot of the active transaction table
 * and the dirty page table. Only the leading fields are a struct
 * (WalRecCheckpointHeader); the entries follow, packed by hand:
 * att_count WalAttEntry, then uint16_t dpt_count, then dpt_count
 * WalDptEntry. */
typedef struct {
    uint64_t txn_id;
    uint64_t last_lsn;
    uint64_t first_lsn;
} WalAttEntry;

typedef struct {
    uint32_t table_id;
    uint32_t page_no;
    uint64_t rec_lsn;
} WalDptEntry;

typedef struct {
    uint64_t checkpoint_lsn;
    uint64_t min_rec_lsn;
    uint16_t att_count;
} WalRecCheckpointHeader;

#endif /* WAL_TYPES_H */
