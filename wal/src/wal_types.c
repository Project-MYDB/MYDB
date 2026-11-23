#include "wal_types.h"
#include "checksum.h"

#include <string.h>

/*
 * WalRecordHeader layout: lsn[8] + prev_lsn[8] + txn_id[8] + total_len[4]
 * + table_id[4] + page_no[4] + rec_type[1] + flags[1] + reserved[2] +
 * checksum[4] = 44 bytes, checksum at offset 40, the same as the struct's
 * own first 44 bytes (wal_types.h pins each offset). Body starts
 * immediately after, at offset 44 (WAL_RECORD_HEADER_SIZE). (total_len
 * used to be at 16 and txn_id at 20.)
 */
#define WAL_RECORD_HEADER_CHECKSUM_OFFSET 40

/* The checksum covers the body plus the header's leading 40 bytes — two
 * spans that aren't adjacent in the final layout (the checksum field
 * itself sits between them, at offset 40). crc32c_update lets us fold
 * both in without copying them into one scratch buffer first. The old
 * code did use a scratch, sized `40 + WAL_MAX_ROW_BODY` (4060, a constant
 * since removed) on the stack,
 * which silently capped body_len at the 4020-byte page limit —
 * LARGE_WAL records (and now records that span pages) are precisely
 * the ones that exceed it, so that scratch was a stack overflow waiting
 * for the first large record to pass through here.
 *
 * Order: body first, then the header (V2 §8.3). It used to be header
 * then body. The body is known before the record has a place in the log,
 * the header (its lsn and flags) only after, so this order lets the
 * thread that builds the record run the body's CRC before it calls
 * insert, and the WAL just continue that running state with the 40
 * header bytes afterwards (record_checksum). Header first would need
 * CRC-combine math instead. */
static uint32_t record_checksum(const uint8_t *hdr_buf, uint32_t body_checksum)
{
    return crc32c_final(crc32c_update(body_checksum, hdr_buf, WAL_RECORD_HEADER_CHECKSUM_OFFSET));
}

/* The body's part, for serialize and deserialize, which have the whole
 * body in hand. No body (BEGIN, ABORT): callers pass NULL, and NULL + 0
 * inside crc32c_update is undefined behaviour in C, so skip the call. */
static uint32_t body_checksum_of(const void *body, size_t body_len)
{
    if (body_len == 0) return CRC32C_INIT;
    return crc32c_update(CRC32C_INIT, body, body_len);
}

void wal_record_header_pack_crc(WalRecordHeader *hdr, uint32_t body_checksum,
                                 uint8_t out[WAL_RECORD_HEADER_SIZE])
{
    uint8_t *buf = out;
    /* Bytes 0..39 of the struct are the on-disk bytes 0..39 (wal_types.h).
     * memcpy, not a struct cast: out can sit at any address, e.g. right
     * after the previous record's body in a buffer. */
    memcpy(buf, hdr, WAL_RECORD_HEADER_CHECKSUM_OFFSET);

    uint32_t cs = record_checksum(buf, body_checksum);
    memcpy(buf + WAL_RECORD_HEADER_CHECKSUM_OFFSET, &cs, 4);
    hdr->checksum = cs;
}

void wal_record_header_serialize(WalRecordHeader *hdr, const void *body,
                                  size_t body_len, uint8_t *buf)
{
    wal_record_header_pack_crc(hdr, body_checksum_of(body, body_len), buf);
    if (body_len) memcpy(buf + WAL_RECORD_HEADER_SIZE, body, body_len);
}

int wal_record_header_deserialize(const uint8_t *buf, size_t body_len,
                                   WalRecordHeader *out)
{
    uint32_t stored;
    memcpy(&stored, buf + WAL_RECORD_HEADER_CHECKSUM_OFFSET, 4);

    if (stored != record_checksum(buf, body_checksum_of(buf + WAL_RECORD_HEADER_SIZE, body_len)))
        return MYDB_ERR_BAD_CHECKSUM;

    /* All 44 bytes, checksum included, in one copy (see pack_crc). */
    memcpy(out, buf, WAL_RECORD_HEADER_SIZE);
    return MYDB_OK;
}
