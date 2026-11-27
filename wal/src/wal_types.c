#include "wal_types.h"
#include "checksum.h"

#include <string.h>

/* The checksum field's offset in the header; the body starts at 44. */
#define WAL_RECORD_HEADER_CHECKSUM_OFFSET 40

/* Finishes the record checksum: the body's running state, continued over
 * the header's first 40 bytes. */
static uint32_t record_checksum(const uint8_t *hdr_buf, uint32_t body_checksum)
{
    return crc32c_final(crc32c_update(body_checksum, hdr_buf, WAL_RECORD_HEADER_CHECKSUM_OFFSET));
}

/* The body's running CRC state. A record with no body may pass NULL, so
 * crc32c_update is not called with it. */
static uint32_t body_checksum_of(const void *body, size_t body_len)
{
    if (body_len == 0) return CRC32C_INIT;
    return crc32c_update(CRC32C_INIT, body, body_len);
}

void wal_record_header_pack_crc(WalRecordHeader *hdr, uint32_t body_checksum,
                                 uint8_t out[WAL_RECORD_HEADER_SIZE])
{
    uint8_t *buf = out;
    /* memcpy, not a struct cast: out can be at any address. */
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

    /* The 44 on-disk bytes are the struct's first 44. */
    memcpy(out, buf, WAL_RECORD_HEADER_SIZE);
    return MYDB_OK;
}
