#include "wal_page.h"
#include "checksum.h"

#include <string.h>

/* Fields are copied one by one at fixed offsets, so buf needs no
 * alignment. serialize writes magic and version itself; of hdr->id it
 * reads only file_type. */

/* The checksum covers the 20 bytes before it. */
#define WAL_PAGE_HEADER_CHECKSUM_OFFSET 20

void wal_page_header_serialize(const WalPageHeader *hdr, uint8_t *buf)
{
    file_header_write_id(buf, hdr->id.file_type);
    memcpy(buf + 8,  &hdr->start_lsn,        8);
    memcpy(buf + 16, &hdr->continuation_len, 2);
    buf[18] = hdr->flags;
    buf[19] = hdr->reserved[0];

    uint32_t cs = crc32c(buf, WAL_PAGE_HEADER_CHECKSUM_OFFSET);
    memcpy(buf + WAL_PAGE_HEADER_CHECKSUM_OFFSET, &cs, 4);
}

int wal_page_header_deserialize(const uint8_t *buf, WalPageHeader *out)
{
    int rc = file_header_check_id(buf, FILETYPE_WAL_PAGE);
    if (rc != MYDB_OK) return rc;

    uint32_t stored;
    memcpy(&stored, buf + WAL_PAGE_HEADER_CHECKSUM_OFFSET, 4);
    if (stored != crc32c(buf, WAL_PAGE_HEADER_CHECKSUM_OFFSET))
        return MYDB_ERR_BAD_CHECKSUM;

    file_header_read_id(buf, &out->id);
    memcpy(&out->start_lsn,        buf + 8,  8);
    memcpy(&out->continuation_len, buf + 16, 2);
    out->flags       = buf[18];
    out->reserved[0] = buf[19];
    out->checksum = stored;
    return MYDB_OK;
}
