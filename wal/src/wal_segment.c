#include "wal_segment.h"
#include "checksum.h"

#include <string.h>

/*
 * Field-by-field memcpy throughout, not a whole-struct memcpy through a
 * cast buf pointer — same reasoning file_header.c already documents:
 * avoids alignment assumptions about caller-provided buffers.
 *
 * Caller sets every field of *hdr except checksum before calling
 * wal_segment_header_serialize(); id.magic/id.version are overwritten
 * internally via file_header_write_id() regardless of what the caller
 * put there (only id.file_type is actually read from *hdr) — the same
 * contract every other on-disk struct in this codebase follows.
 */

/* WalSegmentHeader — documented on-disk shape is 48 bytes, serialized
 * into a 64-byte on-disk buffer (16 trailing bytes zeroed, reserved for
 * future extension). Checksum covers the 44 bytes preceding it (id[8] +
 * segment_no[8] + start_lsn[8] + end_lsn[8] + partition_id[4] +
 * data_pages[4] + state[1] + reserved[3]). */
#define WAL_SEGMENT_HEADER_CHECKSUM_OFFSET 44

void wal_segment_header_serialize(const WalSegmentHeader *hdr, uint8_t *buf)
{
    memset(buf, 0, WAL_SEGMENT_HEADER_ON_DISK_SIZE);

    file_header_write_id(buf, hdr->id.file_type);
    memcpy(buf + 8,  &hdr->segment_no,  8);
    memcpy(buf + 16, &hdr->start_lsn,   8);
    memcpy(buf + 24, &hdr->end_lsn,     8);
    memcpy(buf + 32, &hdr->partition_id, 4);
    memcpy(buf + 36, &hdr->data_pages,  4);
    buf[40] = hdr->state;
    /* buf[41..44) reserved, already zeroed by the memset above */

    uint32_t cs = crc32(buf, WAL_SEGMENT_HEADER_CHECKSUM_OFFSET);
    memcpy(buf + WAL_SEGMENT_HEADER_CHECKSUM_OFFSET, &cs, 4);
}

int wal_segment_header_deserialize(const uint8_t *buf, WalSegmentHeader *out)
{
    int rc = file_header_check_id(buf, FILETYPE_WAL_SEGMENT);
    if (rc != MYDB_OK) return rc;

    uint32_t stored;
    memcpy(&stored, buf + WAL_SEGMENT_HEADER_CHECKSUM_OFFSET, 4);
    if (stored != crc32(buf, WAL_SEGMENT_HEADER_CHECKSUM_OFFSET))
        return MYDB_ERR_BAD_CHECKSUM;

    file_header_read_id(buf, &out->id);
    memcpy(&out->segment_no,   buf + 8,  8);
    memcpy(&out->start_lsn,    buf + 16, 8);
    memcpy(&out->end_lsn,      buf + 24, 8);
    memcpy(&out->partition_id, buf + 32, 4);
    memcpy(&out->data_pages,   buf + 36, 4);
    out->state = buf[40];
    memset(out->reserved, 0, sizeof(out->reserved));
    out->checksum = stored;
    return MYDB_OK;
}
