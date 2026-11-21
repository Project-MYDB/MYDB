#ifndef FILE_HEADER_H
#define FILE_HEADER_H

#include <stdint.h>

/* ------------------------------------------------------------------ */
/*  Common 8-byte identifier that begins every MyDB file.             */
/*                                                                    */
/*    Bytes 0..3  : magic     (uint32, little-endian) — MYDB_MAGIC    */
/*    Bytes 4..5  : version   (uint16, little-endian)                 */
/*    Bytes 6..7  : file_type (uint16, little-endian)                 */
/*                                                                    */
/*  All metadata files (__database.mydb, __catalog.mydb,              */
/*  __schema.mydb, system_schema/users.mydb,                          */
/*  system_schema/privileges.mydb) and relation files share this      */
/*  prefix. The full file headers are file-type-specific.             */
/* ------------------------------------------------------------------ */
typedef struct {
    uint32_t magic;
    uint16_t version;
    uint16_t file_type;
} FileHeaderId;

/* Read the 8-byte ID from the start of buf into *out. Pure memory
 * read — never fails. Caller must ensure buf has at least 8 bytes. */
void file_header_read_id(const void *buf, FileHeaderId *out);

/* Write the 8-byte ID at the start of buf. version is set to
 * MYDB_FORMAT_VERSION. Caller must ensure buf has at least 8 bytes. */
void file_header_write_id(void *buf, uint16_t file_type);

/* Validate the 8-byte ID at the start of buf:
 *   magic     == MYDB_MAGIC          else MYDB_ERR_BAD_MAGIC
 *   version   <= MYDB_FORMAT_VERSION else MYDB_ERR_BAD_VERSION
 *   file_type == expected_filetype   else MYDB_ERR_BAD_FILE_TYPE
 * Returns MYDB_OK on success, or one of the error codes above. */
int  file_header_check_id(const void *buf, uint16_t expected_filetype);

#endif /* FILE_HEADER_H */
