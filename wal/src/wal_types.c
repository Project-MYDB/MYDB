#include "wal_types.h"
#include "checksum.h"

#include <string.h>

/*
 * WalRecordHeader layout: lsn[8] + prev_lsn[8] + total_len[4] + txn_id[8]
 * + table_id[4] + page_no[4] + rec_type[1] + flags[1] + reserved[2] +
 * checksum[4] = 44 bytes, checksum at offset 40. Body starts immediately
 * after, at offset 44 (WAL_RECORD_HEADER_SIZE).
 */
#define WAL_RECORD_HEADER_CHECKSUM_OFFSET 40

/* The checksum covers the header's leading 40 bytes plus the body — two
 * spans that aren't adjacent in the final layout (the checksum field
 * itself sits between them, at offset 40). crc32_update lets us fold
 * both in without copying them into one scratch buffer first. The old
 * code did use a scratch, sized `40 + WAL_MAX_ROW_BODY` (4060, a constant
 * since removed) on the stack,
 * which silently capped body_len at the 4020-byte page limit —
 * LARGE_WAL records (and now records that span pages) are precisely
 * the ones that exceed it, so that scratch was a stack overflow waiting
 * for the first large record to pass through here. */
static uint32_t record_checksum(const uint8_t *hdr_buf, const void *body, size_t body_len)
{
    uint32_t c = CRC32_INIT;
    c = crc32_update(c, hdr_buf, WAL_RECORD_HEADER_CHECKSUM_OFFSET);
    c = crc32_update(c, body, body_len);
    return crc32_final(c);
}

void wal_record_header_pack(WalRecordHeader *hdr, const void *body,
                             size_t body_len, uint8_t out[WAL_RECORD_HEADER_SIZE])
{
    uint8_t *buf = out;
    memcpy(buf + 0,  &hdr->lsn,       8);
    memcpy(buf + 8,  &hdr->prev_lsn,  8);
    memcpy(buf + 16, &hdr->total_len, 4);
    memcpy(buf + 20, &hdr->txn_id,    8);
    memcpy(buf + 28, &hdr->table_id,  4);
    memcpy(buf + 32, &hdr->page_no,   4);
    buf[36] = hdr->rec_type;
    buf[37] = hdr->flags;
    memcpy(buf + 38, hdr->reserved,   2);

    uint32_t cs = record_checksum(buf, body, body_len);
    memcpy(buf + WAL_RECORD_HEADER_CHECKSUM_OFFSET, &cs, 4);
    hdr->checksum = cs;
}

void wal_record_header_serialize(WalRecordHeader *hdr, const void *body,
                                  size_t body_len, uint8_t *buf)
{
    wal_record_header_pack(hdr, body, body_len, buf);
    if (body_len) memcpy(buf + WAL_RECORD_HEADER_SIZE, body, body_len);
}

void wal_record_header_patch_flags(uint8_t *hdr_buf, const void *body,
                                    size_t body_len, uint8_t flags)
{
    hdr_buf[36 + 1] = flags;   /* flags sits right after rec_type at offset 36 */

    uint32_t cs = record_checksum(hdr_buf, body, body_len);
    memcpy(hdr_buf + WAL_RECORD_HEADER_CHECKSUM_OFFSET, &cs, 4);
}

int wal_record_header_deserialize(const uint8_t *buf, size_t body_len,
                                   WalRecordHeader *out)
{
    uint32_t stored;
    memcpy(&stored, buf + WAL_RECORD_HEADER_CHECKSUM_OFFSET, 4);

    if (stored != record_checksum(buf, buf + WAL_RECORD_HEADER_SIZE, body_len))
        return MYDB_ERR_BAD_CHECKSUM;

    memcpy(&out->lsn,       buf + 0,  8);
    memcpy(&out->prev_lsn,  buf + 8,  8);
    memcpy(&out->total_len, buf + 16, 4);
    memcpy(&out->txn_id,    buf + 20, 8);
    memcpy(&out->table_id,  buf + 28, 4);
    memcpy(&out->page_no,   buf + 32, 4);
    out->rec_type = buf[36];
    out->flags    = buf[37];
    memcpy(out->reserved,   buf + 38, 2);
    out->checksum = stored;
    return MYDB_OK;
}
