#include "partition.h"
#include "file_header.h"
#include "checksum.h"

#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <time.h>

/* ------------------------------------------------------------------ */
/*  On-disk layout (per design doc §7, total = CATALOG_FILE_SIZE):    */
/*                                                                    */
/*    0..7    : FileHeaderId                                          */
/*    8..11   : partition_id (uint32)                                 */
/*    12..15  : owner_id (uint32)                                     */
/*    16..23  : created_at (uint64)                                   */
/*    24..31  : last_modified (uint64)                                */
/*    32..39  : quota_bytes (uint64)                                  */
/*    40..47  : used_bytes (uint64)                                   */
/*    48      : num_schemas (uint8)                                   */
/*    49..52  : next_table_id (uint32) — durable table_id generator   */
/*    53..56  : next_schema_id (uint32) — durable schema_id generator */
/*    57..127 : reserved (71 B) — headroom for future fields          */
/*    128..2687: 64 × SchemaEntry, 40 B each                          */
/*    2688..  : reserved block (1400 B)                               */
/*    4088..  : CRC-32C checksum over bytes 0..4087 (4 B)             */
/*    4092..  : reserved (4 B)                                        */
/* ------------------------------------------------------------------ */

#define CAT_HEADER_SIZE      128
#define CAT_SCHEMA_OFFSET    128
#define CAT_SCHEMA_SIZE       40
#define CAT_CHECKSUM_OFFSET 4088

/* SchemaEntry on-disk layout (offsets within the 40-byte slot):
 *    0..31  : schema_name (32 B, NUL-padded)
 *    32     : num_relations (uint8)
 *    33     : is_valid (uint8)
 *    34..37 : schema_id (uint32) — mirrors schema file's SchemaHeader.schema_id
 *    38..39 : reserved (2 B) */

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
/*  Marshalling                                                       */
/* ------------------------------------------------------------------ */

static void serialize_header(uint8_t *buf, const CatalogHeader *h)
{
    file_header_write_id(buf, FILETYPE_CATALOG);
    memcpy(buf + 8,  &h->partition_id,  4);
    memcpy(buf + 12, &h->owner_id,      4);
    memcpy(buf + 16, &h->created_at,    8);
    memcpy(buf + 24, &h->last_modified, 8);
    memcpy(buf + 32, &h->quota_bytes,   8);
    memcpy(buf + 40, &h->used_bytes,    8);
    buf[48] = h->num_schemas;
    memcpy(buf + 49, &h->next_table_id,  4);
    memcpy(buf + 53, &h->next_schema_id, 4);
    /* bytes 57..63 zeroed by caller */
}

static void deserialize_header(const uint8_t *buf, CatalogHeader *h)
{
    memcpy(&h->partition_id,  buf + 8,  4);
    memcpy(&h->owner_id,      buf + 12, 4);
    memcpy(&h->created_at,    buf + 16, 8);
    memcpy(&h->last_modified, buf + 24, 8);
    memcpy(&h->quota_bytes,   buf + 32, 8);
    memcpy(&h->used_bytes,    buf + 40, 8);
    h->num_schemas = buf[48];
    memcpy(&h->next_table_id,  buf + 49, 4);
    memcpy(&h->next_schema_id, buf + 53, 4);
}

static void serialize_schema(uint8_t *buf, const SchemaEntry *s)
{
    memcpy(buf + 0, s->schema_name, 32);
    buf[32] = s->num_relations;
    buf[33] = s->is_valid;
    memcpy(buf + 34, &s->schema_id, 4);
    /* bytes 38..39 zeroed by caller */
}

static void deserialize_schema(const uint8_t *buf, SchemaEntry *s)
{
    memcpy(s->schema_name, buf + 0, 32);
    s->schema_name[31] = '\0';        /* defensive NUL */
    s->num_relations = buf[32];
    s->is_valid      = buf[33];
    memcpy(&s->schema_id, buf + 34, 4);
}

static void pack(const Catalog *cat, uint8_t *buf)
{
    memset(buf, 0, CATALOG_FILE_SIZE);
    serialize_header(buf, &cat->header);
    for (int i = 0; i < MAX_SCHEMAS_PER_PARTITION; i++) {
        serialize_schema(buf + CAT_SCHEMA_OFFSET + i * CAT_SCHEMA_SIZE,
                         &cat->schemas[i]);
    }
    uint32_t cs = crc32c(buf, CAT_CHECKSUM_OFFSET);
    memcpy(buf + CAT_CHECKSUM_OFFSET, &cs, 4);
}

static int unpack(const uint8_t *buf, Catalog *cat)
{
    int rc = file_header_check_id(buf, FILETYPE_CATALOG);
    if (rc != MYDB_OK) return rc;

    uint32_t stored;
    memcpy(&stored, buf + CAT_CHECKSUM_OFFSET, 4);
    if (stored != crc32c(buf, CAT_CHECKSUM_OFFSET))
        return MYDB_ERR_BAD_CHECKSUM;

    deserialize_header(buf, &cat->header);
    for (int i = 0; i < MAX_SCHEMAS_PER_PARTITION; i++) {
        deserialize_schema(buf + CAT_SCHEMA_OFFSET + i * CAT_SCHEMA_SIZE,
                           &cat->schemas[i]);
    }
    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  Public API                                                        */
/* ------------------------------------------------------------------ */

int cat_create(const char *path, uint32_t partition_id, uint32_t owner_id,
               uint64_t quota_bytes, Catalog *out)
{
    if (!path || !out) return MYDB_ERR;

    int fd = open(path, O_CREAT | O_EXCL | O_RDWR, 0644);
    if (fd < 0) return MYDB_ERR;

    if (ftruncate(fd, CATALOG_FILE_SIZE) < 0) {
        close(fd);
        unlink(path);
        return MYDB_ERR;
    }

    memset(out, 0, sizeof(*out));
    out->fd = fd;
    strncpy(out->path, path, sizeof(out->path) - 1);

    out->header.partition_id  = partition_id;
    out->header.owner_id      = owner_id;
    out->header.created_at    = now_yyyymmddhhmmss();
    out->header.last_modified = out->header.created_at;
    out->header.quota_bytes   = quota_bytes;
    out->header.used_bytes    = 0;
    out->header.num_schemas   = 0;
    out->header.next_table_id = 1;   /* 0 stays reserved (matches the
                                       * buffer pool's temp-id convention) */
    out->header.next_schema_id = 1;  /* same reserved-0 convention */

    int rc = cat_save(out);
    if (rc != MYDB_OK) {
        close(fd);
        unlink(path);
        out->fd = -1;
        return rc;
    }
    return MYDB_OK;
}

int cat_open(const char *path, Catalog *out)
{
    if (!path || !out) return MYDB_ERR;

    int fd = open(path, O_RDWR);
    if (fd < 0) return MYDB_ERR;

    uint8_t buf[CATALOG_FILE_SIZE];
    if (pread(fd, buf, CATALOG_FILE_SIZE, 0) != (ssize_t)CATALOG_FILE_SIZE) {
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
    return MYDB_OK;
}

int cat_reload(Catalog *cat)
{
    if (!cat || cat->fd < 0) return MYDB_ERR;

    uint8_t buf[CATALOG_FILE_SIZE];
    if (pread(cat->fd, buf, CATALOG_FILE_SIZE, 0) != (ssize_t)CATALOG_FILE_SIZE)
        return MYDB_ERR;

    return unpack(buf, cat);
}

int cat_close(Catalog *cat)
{
    if (!cat || cat->fd < 0) return MYDB_ERR;
    int rc = close(cat->fd);
    cat->fd = -1;
    return (rc < 0) ? MYDB_ERR : MYDB_OK;
}

int cat_save(Catalog *cat)
{
    if (!cat || cat->fd < 0) return MYDB_ERR;

    cat->header.last_modified = now_yyyymmddhhmmss();

    uint8_t buf[CATALOG_FILE_SIZE];
    pack(cat, buf);

    if (pwrite(cat->fd, buf, CATALOG_FILE_SIZE, 0) != (ssize_t)CATALOG_FILE_SIZE)
        return MYDB_ERR;
    if (fsync(cat->fd) < 0)
        return MYDB_ERR;
    return MYDB_OK;
}

int cat_add_schema(Catalog *cat, const char *schema_name, uint32_t schema_id)
{
    if (!cat || !schema_name) return MYDB_ERR;
    if (strlen(schema_name) >= sizeof(cat->schemas[0].schema_name))
        return MYDB_ERR;

    int free_slot = -1;
    for (int i = 0; i < MAX_SCHEMAS_PER_PARTITION; i++) {
        if (cat->schemas[i].is_valid) {
            if (strncmp(cat->schemas[i].schema_name, schema_name, 32) == 0)
                return MYDB_ERR_DUPLICATE;
        } else if (free_slot < 0) {
            free_slot = i;
        }
    }
    if (free_slot < 0) return MYDB_ERR_FULL;

    SchemaEntry *s = &cat->schemas[free_slot];
    memset(s, 0, sizeof(*s));
    strncpy(s->schema_name, schema_name, sizeof(s->schema_name) - 1);
    s->num_relations = 0;
    s->is_valid      = 1;
    s->schema_id     = schema_id;

    cat->header.num_schemas++;
    return cat_save(cat);
}

int cat_remove_schema(Catalog *cat, const char *schema_name)
{
    if (!cat || !schema_name) return MYDB_ERR;

    for (int i = 0; i < MAX_SCHEMAS_PER_PARTITION; i++) {
        if (cat->schemas[i].is_valid &&
            strncmp(cat->schemas[i].schema_name, schema_name, 32) == 0) {
            memset(&cat->schemas[i], 0, sizeof(cat->schemas[i]));
            if (cat->header.num_schemas > 0) cat->header.num_schemas--;
            return cat_save(cat);
        }
    }
    return MYDB_ERR_NOT_FOUND;
}

int cat_track_alloc(Catalog *cat, int64_t delta_bytes)
{
    if (!cat) return MYDB_ERR;

    if (delta_bytes > 0) {
        uint64_t headroom = cat->header.quota_bytes - cat->header.used_bytes;
        if (cat->header.used_bytes > cat->header.quota_bytes ||
            (uint64_t)delta_bytes > headroom)
            return MYDB_ERR_FULL;
        cat->header.used_bytes += (uint64_t)delta_bytes;
    } else if (delta_bytes < 0) {
        uint64_t freed = (uint64_t)(-delta_bytes);
        if (freed > cat->header.used_bytes) return MYDB_ERR;
        cat->header.used_bytes -= freed;
    } else {
        return MYDB_OK; /* zero delta — nothing to do */
    }

    /* Phase 4: no longer persists here — see the doc comment in
     * partition.h. */
    return MYDB_OK;
}

int cat_alloc_table_id(Catalog *cat, uint32_t *out_id)
{
    if (!cat || !out_id) return MYDB_ERR;

    *out_id = cat->header.next_table_id++;
    return cat_save(cat);
}

int cat_alloc_schema_id(Catalog *cat, uint32_t *out_id)
{
    if (!cat || !out_id) return MYDB_ERR;

    *out_id = cat->header.next_schema_id++;
    return cat_save(cat);
}

SchemaEntry *cat_find_schema(Catalog *cat, const char *schema_name)
{
    if (!cat || !schema_name) return NULL;
    for (int i = 0; i < MAX_SCHEMAS_PER_PARTITION; i++) {
        if (cat->schemas[i].is_valid &&
            strncmp(cat->schemas[i].schema_name, schema_name, 32) == 0)
            return &cat->schemas[i];
    }
    return NULL;
}

/* ------------------------------------------------------------------ */
/*  Quota-aware page allocation                                       */
/* ------------------------------------------------------------------ */

int partition_alloc_page(Catalog *cat, DiskManager *dm,
                         uint32_t current_user_id, uint32_t *out_pno)
{
    if (!cat || !dm || !out_pno) return MYDB_ERR;

    /* Ownership: only the partition's owner may grow user relations.
     * Cheapest gate first — no quota arithmetic for rejected callers. */
    if (cat->header.owner_id != current_user_id) return MYDB_ERR_PERM;

    /* Quota pre-check: cat_track_alloc would also reject, but only after
     * disk_alloc_page has already grown the file (v1 has no per-page free
     * to roll back). Catching the overflow here keeps the file untouched
     * on definite-fail allocations. */
    if (cat->header.used_bytes > cat->header.quota_bytes ||
        (uint64_t)PAGE_SIZE > cat->header.quota_bytes - cat->header.used_bytes)
        return MYDB_ERR_FULL;

    uint32_t pno;
    int rc = disk_alloc_page(dm, &pno);
    if (rc != MYDB_OK) return rc;

    rc = cat_track_alloc(cat, (int64_t)PAGE_SIZE);
    if (rc != MYDB_OK) return rc;

    *out_pno = pno;
    return MYDB_OK;
}
