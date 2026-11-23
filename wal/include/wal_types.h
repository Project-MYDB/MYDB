#ifndef WAL_TYPES_H
#define WAL_TYPES_H

#include <stddef.h>
#include <stdint.h>
#include "common.h"

/*
 * wal_types.h — WAL record header + fixed-shape record body layouts
 * (MYDB_WAL_DESIGN.md §8.3/§8.4, MYDB_WAL_IMPLEMENTATION.md §8.2/§8.5,
 * §8.7, §10.5).
 *
 * WalRecordHeader is wal/'s own struct (unlike WalRecType, which is
 * system-wide and lives in common.h — see the comment there for why the
 * split falls where it does).
 *
 * These structs are plain (not __attribute__((packed))). Most on-disk
 * structs in this codebase are only a convenience for callers to fill in
 * field by field, and their wire format is written at explicit byte
 * offsets (the *_OFFSET-constant pattern database_file.c/partition.c/
 * schema_file.c use, and wal_segment.c / wal_page.c still do).
 *
 * WalRecordHeader is the exception: its on-disk layout was reordered so
 * every field sits at an offset that is a multiple of its own size, which
 * is exactly where the compiler puts it anyway. So the struct's first
 * WAL_RECORD_HEADER_SIZE (44) bytes ARE the on-disk bytes, and
 * wal_types.c copies them with one memcpy, as PostgreSQL does with
 * XLogRecord. The _Static_asserts below the struct pin every offset.
 *
 * WAL_REC_INSERT / WAL_REC_DELETE bodies are genuinely variable-length
 * (slot_no, row_len, row_bytes) and are not given a struct here — they're
 * packed by hand at the call site, the same way every other
 * variable-length row body in this codebase is.
 */

/* ------------------------------------------------------------------
 * WalRecordHeader — on-disk shape is 44 bytes (MYDB_WAL_IMPLEMENTATION.md
 * §8.2), the struct's own first 44 bytes:
 *
 *   lsn        0-7     prev_lsn   8-15    txn_id     16-23
 *   total_len  24-27   table_id   28-31   page_no    32-35
 *   rec_type   36      flags      37      reserved   38-39
 *   checksum   40-43
 *
 * (The disk order used to put total_len at 16 and txn_id at 20, an
 * 8-byte field at an offset that is not a multiple of 8, which no C
 * struct can match without packing; so it was written field by field.)
 *
 * sizeof(WalRecordHeader) is 48, not 44: the struct's alignment is 8, so
 * the compiler pads its end to a multiple of 8. Use WAL_RECORD_HEADER_SIZE
 * (offsetof(checksum) + 4, like PostgreSQL's SizeOfXLogRecord) for the
 * on-disk size, never sizeof. The bytes are in host byte order
 * (little-endian on x86-64), as they already were with the per-field
 * memcpy.
 * ------------------------------------------------------------------ */
typedef struct {
    uint64_t lsn;              /* this record's LSN                       */
    uint64_t prev_lsn;         /* previous LSN for this txn (0 = first)   */
    uint64_t txn_id;
    uint32_t total_len;        /* full record length including header     */
    uint32_t table_id;         /* persistent, on-disk-stable identifier   */
    uint32_t page_no;
    uint8_t  rec_type;         /* WalRecType, stored as a byte            */
    uint8_t  flags;            /* WAL_RECORD_FLAG_*                       */
    uint8_t  reserved[2];      /* was reserved[3]; one byte moved to flags */
    uint32_t checksum;         /* CRC-32C over body + header[0..40)       */
} WalRecordHeader;

/* The on-disk layout is the struct's layout: pin every offset, so moving
 * or resizing a field fails the build instead of silently changing the
 * format. */
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

/* Set when this record's bytes do not all fit on the page its header
 * landed on — the remainder continues on the next page The ring buffer knows where a
 * record lands before it packs the header, so it sets the flag directly
 */
#define WAL_RECORD_FLAG_CONTINUES_ON_NEW_PAGE 0x01

/* ------------------------------------------------------------------
 * Serialize/deserialize + checksum.
 * WalRecordHeader.checksum covers header AND body,
 * two regions that are not contiguous in the final on-disk layout
 * (checksum itself sits between them, at header offset 40). The CRC
 * runs over the body first and then the header's leading 40 bytes, so
 * the body's part can be computed before the record has an LSN
 *
 * wal_record_header_serialize fills buf and stamps hdr->checksum too, so
 * callers who need to inspect what was written (e.g. its checksum or
 * lsn; it used to be a frame's new end_lsn, a page header field dropped in
 * T-039) can read it straight back off *hdr without re-parsing buf.
 * Caller sets every field of *hdr except checksum first.
 *
 * wal_record_header_deserialize validates the checksum before filling
 * *out. Returns MYDB_OK or MYDB_ERR_BAD_CHECKSUM.
 * ------------------------------------------------------------------ */
void wal_record_header_serialize(WalRecordHeader *hdr, const void *body,
                                  size_t body_len, uint8_t *buf);

/* Writes only the 44-byte header into out, with the checksum finished
 * from body_checksum plus the header's leading 40 bytes, and stamps
 * hdr->checksum. Caller sets every field of *hdr except checksum first.
 *
 * body_checksum is the CRC-32C running state over this record's body,
 * not a finished checksum: crc32c_update(CRC32C_INIT, body, body_len),
 * or CRC32C_INIT for a record with no body. The thread that builds the
 * record computes it before calling insert (V2 §8.3), so the WAL only
 * does the fixed 40 header bytes after reserving. */
void wal_record_header_pack_crc(WalRecordHeader *hdr, uint32_t body_checksum,
                                 uint8_t out[WAL_RECORD_HEADER_SIZE]);



int  wal_record_header_deserialize(const uint8_t *buf, size_t body_len,
                                    WalRecordHeader *out);



/* ------------------------------------------------------------------
 * Fixed-shape record bodies.
 * ------------------------------------------------------------------ */

/* WAL_REC_COMMIT body */
typedef struct {
    uint64_t commit_timestamp;
} WalRecCommitBody;

/* WAL_REC_CLR body — makes Undo crash-safe and resumable.
 * next_undo_lsn == 0 means the transaction is fully undone. */
typedef struct {
    uint64_t undone_lsn;
    uint64_t next_undo_lsn;
} WalRecClrBody;


/* WAL_REC_FILE_HEADER_UPDATE body — one of two field shapes,
 * discriminated by `field`. root_page_no now lives in RelationDef in
 * __schema.mydb (post code-fix); the file header's root_page_no is
 * vestigial, so FIELD_NUM_PAGES / FIELD_FREE_PAGE_BITMAP are the only
 * two fields this record type ever touches. */
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

/* WAL_REC_CHECKPOINT body — ATT/DPT snapshot. Matches the on-disk
 * encoding exactly so a checkpoint snapshot serializes directly from
 * live ATT/DPT state with no translation step (impl doc §8.7). Only the
 * fixed leading fields (up to and including att_count) are a struct
 * here: dpt_count sits AFTER the variable-length att_entry[] array in
 * the actual byte layout, so the full body — like INSERT/DELETE — is
 * packed by hand, not force-fit into one fixed struct. WalAttEntry /
 * WalDptEntry are the fixed-size entry shapes repeated att_count /
 * dpt_count times. */
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
    /* att_count * WalAttEntry follows immediately here, then:
     *   uint16_t dpt_count;
     *   dpt_count * WalDptEntry
     * — both packed by hand at the call site. */
} WalRecCheckpointHeader;

#endif /* WAL_TYPES_H */
