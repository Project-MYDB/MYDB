#include "database_file.h"
#include "file_header.h"
#include "checksum.h"

#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <time.h>

/* ------------------------------------------------------------------ */
/*  On-disk layout (per design doc §6, total = DATABASE_FILE_SIZE):   */
/*                                                                    */
/*    0..7    : FileHeaderId (magic + version + file_type)            */
/*    8..39   : engine_name (32 B)                                    */
/*    40..47  : created_at (uint64)                                   */
/*    48..55  : last_opened (uint64)                                  */
/*    56      : num_partitions (uint8)                                */
/*    57..59  : reserved (3 B, zero)                                  */
/*    60..63  : next_partition_id (uint32) — monotonic, never reused  */
/*    64..127 : reserved (64 B, zero) — headroom for future fields    */
/*    128..4607: 16 PartitionEntry records, 280 B each                */
/*    4608..  : reserved block (3580 B, zero)                         */
/*    8188..  : CRC-32C checksum over bytes 0..8187 (4 B)             */
/* ------------------------------------------------------------------ */

#define DB_HEADER_SIZE       128
#define DB_PARTITION_OFFSET  128
#define DB_PARTITION_SIZE    280
#define DB_CHECKSUM_OFFSET  8188

/* PartitionEntry on-disk layout (offsets within the 280-byte slot):
 *    0..3   : partition_id (uint32)
 *    4..259 : path (256 B, NUL-padded)
 *    260..263: owner_id (uint32)
 *    264    : is_active (uint8)
 *    265..279: reserved (15 B) */

/* Current local time as YYYYMMDDHHmmSS packed in a uint64. */
static uint64_t now_yyyymmddhhmmss(void)
{
    time_t t = time(NULL);
    struct tm tm;
    localtime_r(&t, &tm);
    return (uint64_t)(tm.tm_year + 1900) * 10000000000ULL
         + (uint64_t)(tm.tm_mon + 1)     * 100000000ULL
         + (uint64_t)tm.tm_mday          * 1000000ULL
         + (uint64_t)tm.tm_hour          * 10000ULL
         + (uint64_t)tm.tm_min           * 100ULL
         + (uint64_t)tm.tm_sec;
}

/* ------------------------------------------------------------------ */
/*  Marshalling helpers — explicit byte offsets, memcpy only          */
/* ------------------------------------------------------------------ */

static void serialize_header(uint8_t *buf, const DatabaseHeader *h)
{
    file_header_write_id(buf, FILETYPE_DATABASE);
    memcpy(buf + 8,  h->engine_name, 32);
    memcpy(buf + 40, &h->created_at,  8);
    memcpy(buf + 48, &h->last_opened, 8);
    buf[56] = h->num_partitions;
    /* bytes 57..59 zeroed by caller's memset */
    memcpy(buf + 60, &h->next_partition_id, 4);
    /* bytes 64..127 zeroed by caller's memset */
}

static void deserialize_header(const uint8_t *buf, DatabaseHeader *h)
{
    memcpy(h->engine_name,  buf + 8,  32);
    h->engine_name[31] = '\0';                /* defensive NUL */
    memcpy(&h->created_at,  buf + 40, 8);
    memcpy(&h->last_opened, buf + 48, 8);
    h->num_partitions = buf[56];
    memcpy(&h->next_partition_id, buf + 60, 4);
}

static void serialize_partition(uint8_t *buf, const PartitionEntry *p)
{
    memcpy(buf + 0,   &p->partition_id, 4);
    memcpy(buf + 4,    p->path,       256);
    memcpy(buf + 260, &p->owner_id,     4);
    buf[264] = p->is_active;
    /* bytes 265..279 zeroed by caller */
}

static void deserialize_partition(const uint8_t *buf, PartitionEntry *p)
{
    memcpy(&p->partition_id, buf + 0,   4);
    memcpy(p->path,           buf + 4, 256);
    p->path[255] = '\0';                      /* defensive NUL */
    memcpy(&p->owner_id,     buf + 260, 4);
    p->is_active = buf[264];
}

/* Pack a fully-zeroed 8 KB buffer with the in-memory state and write
 * the trailer checksum at DB_CHECKSUM_OFFSET. */
static void pack(const DatabaseFile *db, uint8_t *buf)
{
    memset(buf, 0, DATABASE_FILE_SIZE);
    serialize_header(buf, &db->header);
    for (int i = 0; i < MAX_PARTITIONS; i++) {
        serialize_partition(
            buf + DB_PARTITION_OFFSET + i * DB_PARTITION_SIZE,
            &db->partitions[i]);
    }
    uint32_t cs = crc32c(buf, DB_CHECKSUM_OFFSET);
    memcpy(buf + DB_CHECKSUM_OFFSET, &cs, 4);
}

/* Validate magic/version/file_type and checksum, then unpack into *db.
 * Touches only db->header and db->partitions — not fd or path. */
static int unpack(const uint8_t *buf, DatabaseFile *db)
{
    int rc = file_header_check_id(buf, FILETYPE_DATABASE);
    if (rc != MYDB_OK) return rc;

    uint32_t stored;
    memcpy(&stored, buf + DB_CHECKSUM_OFFSET, 4);
    if (stored != crc32c(buf, DB_CHECKSUM_OFFSET))
        return MYDB_ERR_BAD_CHECKSUM;

    deserialize_header(buf, &db->header);
    for (int i = 0; i < MAX_PARTITIONS; i++) {
        deserialize_partition(
            buf + DB_PARTITION_OFFSET + i * DB_PARTITION_SIZE,
            &db->partitions[i]);
    }
    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  Public API                                                         */
/* ------------------------------------------------------------------ */

int db_create(const char *path, const char *engine_name, DatabaseFile *out)
{
    if (!path || !out) return MYDB_ERR;

    int fd = open(path, O_CREAT | O_EXCL | O_RDWR, 0644);
    if (fd < 0) return MYDB_ERR;

    if (ftruncate(fd, DATABASE_FILE_SIZE) < 0) {
        close(fd);
        unlink(path);
        return MYDB_ERR;
    }

    memset(out, 0, sizeof(*out));
    out->fd = fd;
    strncpy(out->path, path, sizeof(out->path) - 1);

    const char *name = engine_name ? engine_name : "MyDB Engine";
    strncpy(out->header.engine_name, name, sizeof(out->header.engine_name) - 1);
    out->header.created_at  = now_yyyymmddhhmmss();
    out->header.last_opened = out->header.created_at;
    out->header.num_partitions = 0;
    out->header.next_partition_id = 1;

    int rc = db_save(out);
    if (rc != MYDB_OK) {
        close(fd);
        unlink(path);
        out->fd = -1;
        return rc;
    }
    return MYDB_OK;
}

int db_open(const char *path, DatabaseFile *out)
{
    if (!path || !out) return MYDB_ERR;

    int fd = open(path, O_RDWR);
    if (fd < 0) return MYDB_ERR;

    uint8_t buf[DATABASE_FILE_SIZE];
    if (pread(fd, buf, DATABASE_FILE_SIZE, 0) != (ssize_t)DATABASE_FILE_SIZE) {
        close(fd);
        return MYDB_ERR;
    }

    memset(out, 0, sizeof(*out));
    int rc = unpack(buf, out);
    if (rc != MYDB_OK) {
        close(fd);
        return rc;
    }

    out->fd = fd;
    strncpy(out->path, path, sizeof(out->path) - 1);

    /* Refresh last_opened timestamp on every successful open. */
    out->header.last_opened = now_yyyymmddhhmmss();
    rc = db_save(out);
    if (rc != MYDB_OK) {
        close(fd);
        out->fd = -1;
        return rc;
    }
    return MYDB_OK;
}

int db_close(DatabaseFile *db)
{
    if (!db || db->fd < 0) return MYDB_ERR;
    int rc = close(db->fd);
    db->fd = -1;
    return (rc < 0) ? MYDB_ERR : MYDB_OK;
}

int db_save(DatabaseFile *db)
{
    if (!db || db->fd < 0) return MYDB_ERR;

    uint8_t buf[DATABASE_FILE_SIZE];
    pack(db, buf);

    if (pwrite(db->fd, buf, DATABASE_FILE_SIZE, 0) != (ssize_t)DATABASE_FILE_SIZE)
        return MYDB_ERR;
    if (fsync(db->fd) < 0)
        return MYDB_ERR;
    return MYDB_OK;
}

int db_add_partition(DatabaseFile *db, uint32_t owner_id,
                     const char *path, uint32_t *out_partition_id)
{
    if (!db || !path || !out_partition_id) return MYDB_ERR;
    if (strlen(path) >= sizeof(db->partitions[0].path)) return MYDB_ERR;

    int slot = -1;
    for (int i = 0; i < MAX_PARTITIONS; i++) {
        if (!db->partitions[i].is_active) { slot = i; break; }
    }
    if (slot < 0) return MYDB_ERR_FULL;

    PartitionEntry *p = &db->partitions[slot];
    memset(p, 0, sizeof(*p));
    p->partition_id = db->header.next_partition_id++;
    strncpy(p->path, path, sizeof(p->path) - 1);
    p->owner_id  = owner_id;
    p->is_active = 1;

    db->header.num_partitions++;
    *out_partition_id = p->partition_id;
    return db_save(db);
}

int db_remove_partition(DatabaseFile *db, uint32_t partition_id)
{
    if (!db) return MYDB_ERR;

    for (int i = 0; i < MAX_PARTITIONS; i++) {
        if (db->partitions[i].is_active &&
            db->partitions[i].partition_id == partition_id) {
            memset(&db->partitions[i], 0, sizeof(db->partitions[i]));
            if (db->header.num_partitions > 0) db->header.num_partitions--;
            return db_save(db);
        }
    }
    return MYDB_ERR_NOT_FOUND;
}

PartitionEntry *db_find_by_owner(DatabaseFile *db, uint32_t owner_id)
{
    if (!db) return NULL;
    for (int i = 0; i < MAX_PARTITIONS; i++) {
        if (db->partitions[i].is_active &&
            db->partitions[i].owner_id == owner_id)
            return &db->partitions[i];
    }
    return NULL;
}

PartitionEntry *db_find_by_id(DatabaseFile *db, uint32_t partition_id)
{
    if (!db) return NULL;
    for (int i = 0; i < MAX_PARTITIONS; i++) {
        if (db->partitions[i].is_active &&
            db->partitions[i].partition_id == partition_id)
            return &db->partitions[i];
    }
    return NULL;
}
