#include "wal_page.h"
#include "checksum.h"

#include <string.h>

/*
 * Field-by-field memcpy throughout, not a whole-struct memcpy through a
 * cast buf pointer — same reasoning file_header.c already documents:
 * avoids alignment assumptions about caller-provided buffers.
 *
 * Caller sets every field of *hdr except checksum before calling
 * wal_page_header_serialize(); id.magic/id.version are overwritten
 * internally via file_header_write_id() regardless of what the caller
 * put there (only id.file_type is actually read from *hdr) — the same
 * contract every other on-disk struct in this codebase follows.
 */

/* WalPageHeader — 24 bytes. Checksum covers the 20 bytes preceding it
 * (id[8] + start_lsn[8] + continuation_len[2] + flags[1] + reserved[1]).
 * Was id + start_lsn + end_lsn[8] + data_len + flags + reserved[1] before
 * end_lsn was dropped (T-039), then 32 bytes with data_len[2] and
 * reserved[7] until data_len was dropped too. */
#define WAL_PAGE_HEADER_CHECKSUM_OFFSET 20

void wal_page_header_serialize(const WalPageHeader *hdr, uint8_t *buf)
{
    file_header_write_id(buf, hdr->id.file_type);
    memcpy(buf + 8,  &hdr->start_lsn,        8);
    memcpy(buf + 16, &hdr->continuation_len, 2);
    buf[18] = hdr->flags;
    buf[19] = hdr->reserved[0];

    uint32_t cs = crc32(buf, WAL_PAGE_HEADER_CHECKSUM_OFFSET);
    memcpy(buf + WAL_PAGE_HEADER_CHECKSUM_OFFSET, &cs, 4);
}

int wal_page_header_deserialize(const uint8_t *buf, WalPageHeader *out)
{
    int rc = file_header_check_id(buf, FILETYPE_WAL_PAGE);
    if (rc != MYDB_OK) return rc;

    uint32_t stored;
    memcpy(&stored, buf + WAL_PAGE_HEADER_CHECKSUM_OFFSET, 4);
    if (stored != crc32(buf, WAL_PAGE_HEADER_CHECKSUM_OFFSET))
        return MYDB_ERR_BAD_CHECKSUM;

    file_header_read_id(buf, &out->id);
    memcpy(&out->start_lsn,        buf + 8,  8);
    memcpy(&out->continuation_len, buf + 16, 2);
    out->flags       = buf[18];
    out->reserved[0] = buf[19];
    out->checksum = stored;
    return MYDB_OK;
}
