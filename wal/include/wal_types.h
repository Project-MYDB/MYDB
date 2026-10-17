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
 * These structs are plain (not __attribute__((packed))), matching how
 * every other on-disk struct in this codebase works: the struct is a
 * convenience for callers to fill in field-by-field, not a layout the
 * code memcpy's onto disk directly. The real wire format is expressed by
 * wal_segment.c's serialize/deserialize functions, which read/write each
 * field at its own explicit byte offset (WAL_PAGE_HEADER_CHECKSUM_OFFSET
 * etc.) — the same *_OFFSET-constant pattern database_file.c/partition.c/
 * schema_file.c already use. So sizeof(WalRecordHeader) etc. is not
 * asserted anywhere to equal the documented on-disk byte count.
 *
 * WAL_REC_INSERT / WAL_REC_DELETE bodies are genuinely variable-length
 * (slot_no, row_len, row_bytes) and are not given a struct here — they're
 * packed by hand at the call site, the same way every other
 * variable-length row body in this codebase is.
 */

/* ------------------------------------------------------------------
 * WalRecordHeader — on-disk shape is 44 bytes (MYDB_WAL_IMPLEMENTATION.md
 * §8.2); wire format is produced/consumed field-by-field, not via this
 * struct's in-memory layout.
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
    uint32_t checksum;         /* CRC32 over header + body                */
} WalRecordHeader;

/* Set when this record's bytes do not all fit on the page its header
 * landed on — the remainder continues on the next page. LARGE_WAL set
 * it first (moved out of the tree); since T-039 wal_ring_buffer_append
 * sets it, before packing the header, whenever a record's last byte
 * lands on a later page than its first. It was stamped
 * by the LARGE_WAL writer at pack time (not by whoever
 * built the record), because whether a record spans depends on where it
 * lands, which only the writer knows — see
 * wal_record_header_patch_flags below. The ring buffer knows where a
 * record lands before it packs the header, so it sets the flag directly
 * and never needs patch_flags; the page header's OUT flag
 * (WAL_PAGE_OUTGOING_CONTINUATION) carries the same fact page-side. */
#define WAL_RECORD_FLAG_CONTINUES_ON_NEW_PAGE 0x01

/* ------------------------------------------------------------------
 * Serialize/deserialize + checksum. Added in Phase 3 (wal_ring_buffer.c
 * is the first real consumer) — deferred out of Phase 1 since there was
 * no file/frame I/O yet to serialize into.
 *
 * Unlike WalPageHeader/WalSegmentHeader (checksum over the header's own
 * fixed bytes only), WalRecordHeader.checksum covers header AND body —
 * two regions that are not contiguous in the final on-disk layout
 * (checksum itself sits between them, at header offset 40). Both
 * functions used to build a small scratch buffer (header-minus-checksum
 * ++ body) to compute one CRC32 over that combined span; they now fold
 * the two spans in with crc32_update (wal_types.c), so there is no
 * scratch and no size ceiling. serialize writes the real fixed layout
 * (header incl. checksum at its own offset, followed by body) into buf,
 * which must have room for WAL_RECORD_HEADER_SIZE + body_len bytes.
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

/* Writes only the 44-byte header into out, with the checksum computed
 * over header + body, and stamps hdr->checksum. The body is read, never
 * copied. This is what the ring buffer uses (T-039): a record that spans
 * pages is never contiguous in the ring, so it packs the header here and
 * then copies header and body piece by piece across frames.
 * wal_record_header_serialize is this plus a memcpy of the body. */
void wal_record_header_pack(WalRecordHeader *hdr, const void *body,
                             size_t body_len, uint8_t out[WAL_RECORD_HEADER_SIZE]);
int  wal_record_header_deserialize(const uint8_t *buf, size_t body_len,
                                    WalRecordHeader *out);

/* Rewrites the flags byte of an already-serialized 44-byte record
 * header and recomputes its checksum over header+body, so the record
 * still validates afterwards. body must point at that record's own
 * contiguous body bytes (body_len of them); it is read, never written.
 *
 * This existed for the LARGE_WAL writer (moved out; no caller today;
 * kept by decision in T-039), which learned only at pack time
 * whether a record will span a page, and must not mutate the caller's
 * const blob to record that: it patches a 44-byte copy of the header
 * and packs that instead of the original's first 44 bytes. */
void wal_record_header_patch_flags(uint8_t *hdr_buf, const void *body,
                                    size_t body_len, uint8_t flags);

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
