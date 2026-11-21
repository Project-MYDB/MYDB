#include "file_header.h"
#include "common.h"

#include <string.h>

void file_header_read_id(const void *buf, FileHeaderId *out)
{
    /* Use memcpy rather than struct cast to avoid alignment assumptions
     * about caller-provided buffers. */
    memcpy(&out->magic,     (const uint8_t *)buf + 0, 4);
    memcpy(&out->version,   (const uint8_t *)buf + 4, 2);
    memcpy(&out->file_type, (const uint8_t *)buf + 6, 2);
}

void file_header_write_id(void *buf, uint16_t file_type)
{
    uint32_t magic   = MYDB_MAGIC;
    uint16_t version = MYDB_FORMAT_VERSION;
    memcpy((uint8_t *)buf + 0, &magic,     4);
    memcpy((uint8_t *)buf + 4, &version,   2);
    memcpy((uint8_t *)buf + 6, &file_type, 2);
}

int file_header_check_id(const void *buf, uint16_t expected_filetype)
{
    FileHeaderId id;
    file_header_read_id(buf, &id);

    if (id.magic     != MYDB_MAGIC)          return MYDB_ERR_BAD_MAGIC;
    if (id.version   >  MYDB_FORMAT_VERSION) return MYDB_ERR_BAD_VERSION;
    if (id.file_type != expected_filetype)   return MYDB_ERR_BAD_FILE_TYPE;
    return MYDB_OK;
}
