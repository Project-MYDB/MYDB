#include "schema_file.h"
#include "file_header.h"
#include "checksum.h"

#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

/* ------------------------------------------------------------------ */
/*  Page 0 on-disk layout (per design doc §8.1, §8.3):                */
/*                                                                    */
/*    0..7      : FileHeaderId (magic / version / file_type=3)        */
/*    8..11     : partition_id (uint32)                               */
/*    12..15    : schema_id (uint32) — mirrors catalog SchemaEntry.schema_id */
/*    16..47    : schema_name (32 B)                                  */
/*    48..55    : created_at (uint64)                                 */
/*    56..63    : last_modified (uint64)                              */
/*    64..71    : size_bytes (uint64) — written as 0, recomputed      */
/*                on load (§8 implementation note)                    */
/*    72        : num_relations (uint8)                               */
/*    73..127   : reserved (55 B) — headroom for future fields        */
/*    128..3711 : 64 x RelationEntry, 56 B each                       */
/*    3712..16367: reserved (12656 B)                                 */
/*    16368..16375: reserved (8 B)                                    */
/*    16376..16379: CRC-32C checksum over bytes 0..16375 (4 B)        */
/*    16380..16383: reserved (4 B)                                    */
/*                                                                    */
/*  RelationEntry on-disk (56 B per slot, per design §8.2):           */
/*    0..31     : relation_name (32 B)                                */
/*    32        : is_valid (uint8)                                    */
/*    33        : page_no (uint8) — which page (1..64) holds the def  */
/*    34        : num_columns (uint8)                                 */
/*    35..38    : num_rows (uint32)                                   */
/*    39..42    : num_pages (uint32)                                  */
/*    43        : tree_height (uint8) — B+ tree height, CBO cost input */
/*    44        : reserved (uint8)                                    */
/*    45..48    : table_id (uint32) — mirrors relation FileHeader.table_id */
/*    49..55    : reserved (7 B)                                      */
/* ------------------------------------------------------------------ */

#define SF_HEADER_SIZE        128
#define SF_RELATION_OFFSET    128
#define SF_RELATION_SIZE       56
#define SF_CHECKSUM_OFFSET  16376

/* RelationDef def-page magic (kept identical to v1 format per design §8.4) */
#define SCHEMA_PAGE_MAGIC      0x53434D41u   /* "SCMA" */

/* DEFAULT value blob fits VARCHAR(150): 2-byte len + 150 bytes payload */
#define DEFAULT_DATA_SIZE      152

/* ------------------------------------------------------------------ */
/*  Time helper                                                       */
/* ------------------------------------------------------------------ */
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
/*  Little-endian byte cursors — used by RelationDef def-page codec.  */
/*  Ported verbatim from v1 schema.c so the def-page binary format    */
/*  stays identical (design doc §8.4).                                */
/* ------------------------------------------------------------------ */

static void put_u8(uint8_t *buf, uint16_t *off, uint8_t v)
{
    buf[(*off)++] = v;
}

static void put_u16(uint8_t *buf, uint16_t *off, uint16_t v)
{
    buf[*off]     = (uint8_t)(v & 0xFF);
    buf[*off + 1] = (uint8_t)(v >> 8);
    *off += 2;
}

static void put_u32(uint8_t *buf, uint16_t *off, uint32_t v)
{
    buf[*off]     = (uint8_t)(v & 0xFF);
    buf[*off + 1] = (uint8_t)((v >>  8) & 0xFF);
    buf[*off + 2] = (uint8_t)((v >> 16) & 0xFF);
    buf[*off + 3] = (uint8_t)((v >> 24) & 0xFF);
    *off += 4;
}

static void put_bytes(uint8_t *buf, uint16_t *off, const void *src, uint16_t len)
{
    memcpy(buf + *off, src, len);
    *off += len;
}

static uint8_t get_u8(const uint8_t *buf, uint16_t *off)
{
    return buf[(*off)++];
}

static uint16_t get_u16(const uint8_t *buf, uint16_t *off)
{
    uint16_t v = (uint16_t)buf[*off] | ((uint16_t)buf[*off + 1] << 8);
    *off += 2;
    return v;
}

static uint32_t get_u32(const uint8_t *buf, uint16_t *off)
{
    uint32_t v = (uint32_t) buf[*off]
               | ((uint32_t)buf[*off + 1] <<  8)
               | ((uint32_t)buf[*off + 2] << 16)
               | ((uint32_t)buf[*off + 3] << 24);
    *off += 4;
    return v;
}

static void get_bytes(const uint8_t *buf, uint16_t *off, void *dst, uint16_t len)
{
    memcpy(dst, buf + *off, len);
    *off += len;
}

/* ------------------------------------------------------------------ */
/*  DEFAULT value blob (152 B): packs the active union field by type. */
/*  Ported from v1 schema.c.                                          */
/* ------------------------------------------------------------------ */

static void serialize_default(const ColumnDef *col, uint8_t *out)
{
    memset(out, 0, DEFAULT_DATA_SIZE);
    if (!col->has_default || col->default_value.is_null) return;

    const Value *v = &col->default_value;
    switch (col->type) {
        case TYPE_INT:      memcpy(out, &v->v.int_val, 4); break;
        case TYPE_DECIMAL:  memcpy(out, &v->v.decimal_val, 8); break;
        case TYPE_VARCHAR: {
            uint16_t len = v->v.varchar_val.len;
            memcpy(out, &len, 2);
            memcpy(out + 2, v->v.varchar_val.data, len);
            break;
        }
        case TYPE_ENUM:     out[0] = v->v.enum_val; break;
        case TYPE_BOOL:     out[0] = v->v.bool_val; break;
        case TYPE_DATE:     memcpy(out, &v->v.date_val, 4); break;
        case TYPE_DATETIME: memcpy(out, &v->v.datetime_val, 8); break;
    }
}

static void deserialize_default(ColumnDef *col, const uint8_t *in)
{
    /* When the column has no default, leave default_value as the caller
     * left it (all zero from the enclosing relation_def_deserialize's
     * memset). This keeps Value's type/union fields zero, which matches
     * how callers build columns (memset + selectively-set fields). */
    if (!col->has_default) return;

    Value *v = &col->default_value;
    uint8_t was_null = v->is_null;     /* set by caller before this call */
    memset(v, 0, sizeof(Value));
    v->type    = col->type;
    v->is_null = was_null;

    if (v->is_null) return;

    switch (col->type) {
        case TYPE_INT:      memcpy(&v->v.int_val, in, 4); break;
        case TYPE_DECIMAL:  memcpy(&v->v.decimal_val, in, 8); break;
        case TYPE_VARCHAR: {
            uint16_t len;
            memcpy(&len, in, 2);
            v->v.varchar_val.len = len;
            memcpy(v->v.varchar_val.data, in + 2, len);
            break;
        }
        case TYPE_ENUM:     v->v.enum_val = in[0]; break;
        case TYPE_BOOL:     v->v.bool_val = in[0]; break;
        case TYPE_DATE:     memcpy(&v->v.date_val, in, 4); break;
        case TYPE_DATETIME: memcpy(&v->v.datetime_val, in, 8); break;
    }
}

/* ------------------------------------------------------------------ */
/*  RelationDef → 16 KB def page                                      */
/*  Format unchanged from v1 (design doc §8.4).                       */
/* ------------------------------------------------------------------ */

static int relation_def_serialize(const RelationDef *r, uint8_t *page)
{
    memset(page, 0, PAGE_SIZE);
    uint16_t off = 0;

    /* fixed header */
    put_u32(page, &off, SCHEMA_PAGE_MAGIC);
    put_bytes(page, &off, r->relation_name, MAX_TABLE_NAME);
    put_u8 (page, &off, r->num_columns);
    put_u8 (page, &off, r->pk_col_idx);
    put_u8 (page, &off, r->num_foreign_keys);
    put_u8 (page, &off, r->num_secondary_indexes);
    put_u32(page, &off, r->auto_incr_counter);
    put_u32(page, &off, r->root_page_no);
    put_u32(page, &off, r->table_id);

    for (int i = 0; i < MAX_SECONDARY_IDX; i++)
        put_u8(page, &off, r->secondary_col_idx[i]);
    for (int i = 0; i < MAX_SECONDARY_IDX; i++)
        put_u32(page, &off, r->secondary_root_page_no[i]);

    /* columns */
    for (int i = 0; i < r->num_columns; i++) {
        const ColumnDef *c = &r->columns[i];

        put_bytes(page, &off, c->name, MAX_COLUMN_NAME);
        put_u8 (page, &off, (uint8_t)c->type);
        put_u16(page, &off, c->max_len);
        put_u8 (page, &off, c->scale);

        uint8_t flags = (uint8_t)(
            (c->is_not_null        & 1)       |
            ((c->is_primary_key    & 1) << 1) |
            ((c->is_unique         & 1) << 2) |
            ((c->is_auto_increment & 1) << 3) |
            ((c->has_default       & 1) << 4)
        );
        put_u8(page, &off, flags);
        put_u8(page, &off, c->default_value.is_null);

        uint8_t def_blob[DEFAULT_DATA_SIZE];
        serialize_default(c, def_blob);
        put_bytes(page, &off, def_blob, DEFAULT_DATA_SIZE);

        put_u8(page, &off, c->num_enum_values);
        for (int j = 0; j < c->num_enum_values; j++)
            put_bytes(page, &off, c->enum_values[j], MAX_ENUM_STR_LEN);

        if (off > PAGE_SIZE - 256) return MYDB_ERR;     /* leave room for FKs */
    }

    /* foreign keys */
    for (int i = 0; i < r->num_foreign_keys; i++) {
        const ForeignKey *fk = &r->foreign_keys[i];
        put_bytes(page, &off, fk->constraint_name,    MAX_COLUMN_NAME);
        put_bytes(page, &off, fk->column_name,        MAX_COLUMN_NAME);
        put_bytes(page, &off, fk->ref_relation_name,  MAX_TABLE_NAME);
        put_bytes(page, &off, fk->ref_column_name,    MAX_COLUMN_NAME);
        put_u8   (page, &off, fk->on_delete_action);

        if (off > PAGE_SIZE) return MYDB_ERR;
    }

    return MYDB_OK;
}

static int relation_def_deserialize(RelationDef *r, const uint8_t *page)
{
    uint16_t off = 0;

    uint32_t magic = get_u32(page, &off);
    if (magic != SCHEMA_PAGE_MAGIC) return MYDB_ERR;

    memset(r, 0, sizeof(*r));
    get_bytes(page, &off, r->relation_name, MAX_TABLE_NAME);
    r->num_columns           = get_u8 (page, &off);
    r->pk_col_idx            = get_u8 (page, &off);
    r->num_foreign_keys      = get_u8 (page, &off);
    r->num_secondary_indexes = get_u8 (page, &off);
    r->auto_incr_counter     = get_u32(page, &off);
    r->root_page_no          = get_u32(page, &off);
    r->table_id              = get_u32(page, &off);

    for (int i = 0; i < MAX_SECONDARY_IDX; i++)
        r->secondary_col_idx[i] = get_u8(page, &off);
    for (int i = 0; i < MAX_SECONDARY_IDX; i++)
        r->secondary_root_page_no[i] = get_u32(page, &off);

    for (int i = 0; i < r->num_columns; i++) {
        ColumnDef *c = &r->columns[i];

        get_bytes(page, &off, c->name, MAX_COLUMN_NAME);
        c->type    = (DataType)get_u8(page, &off);
        c->max_len = get_u16(page, &off);
        c->scale   = get_u8 (page, &off);

        uint8_t flags        = get_u8(page, &off);
        c->is_not_null       = (flags >> 0) & 1;
        c->is_primary_key    = (flags >> 1) & 1;
        c->is_unique         = (flags >> 2) & 1;
        c->is_auto_increment = (flags >> 3) & 1;
        c->has_default       = (flags >> 4) & 1;

        c->default_value.is_null = get_u8(page, &off);

        uint8_t def_blob[DEFAULT_DATA_SIZE];
        get_bytes(page, &off, def_blob, DEFAULT_DATA_SIZE);
        deserialize_default(c, def_blob);

        c->num_enum_values = get_u8(page, &off);
        for (int j = 0; j < c->num_enum_values; j++)
            get_bytes(page, &off, c->enum_values[j], MAX_ENUM_STR_LEN);
    }

    for (int i = 0; i < r->num_foreign_keys; i++) {
        ForeignKey *fk = &r->foreign_keys[i];
        get_bytes(page, &off, fk->constraint_name,   MAX_COLUMN_NAME);
        get_bytes(page, &off, fk->column_name,       MAX_COLUMN_NAME);
        get_bytes(page, &off, fk->ref_relation_name, MAX_TABLE_NAME);
        get_bytes(page, &off, fk->ref_column_name,   MAX_COLUMN_NAME);
        fk->on_delete_action = get_u8(page, &off);
    }

    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  Page 0 marshalling                                                */
/* ------------------------------------------------------------------ */

static void serialize_header(uint8_t *buf, const SchemaHeader *h)
{
    file_header_write_id(buf, FILETYPE_SCHEMA);
    memcpy(buf + 8,  &h->partition_id,  4);
    memcpy(buf + 12, &h->schema_id,     4);
    memcpy(buf + 16, h->schema_name,   32);
    memcpy(buf + 48, &h->created_at,    8);
    memcpy(buf + 56, &h->last_modified, 8);
    /* size_bytes (64..71) is intentionally zeroed — recomputed on load */
    buf[72] = h->num_relations;
    /* bytes 73..75 zeroed by caller */
}

static void deserialize_header(const uint8_t *buf, SchemaHeader *h)
{
    memcpy(&h->partition_id,  buf + 8,  4);
    memcpy(&h->schema_id,     buf + 12, 4);
    memcpy(h->schema_name,    buf + 16, 32);
    h->schema_name[31] = '\0';                /* defensive NUL */
    memcpy(&h->created_at,    buf + 48, 8);
    memcpy(&h->last_modified, buf + 56, 8);
    h->size_bytes    = 0;                     /* will be recomputed */
    h->num_relations = buf[72];
}

static void serialize_relation_entry(uint8_t *buf, const RelationEntry *e)
{
    memcpy(buf + 0, e->relation_name, 32);
    buf[32] = e->is_valid;
    buf[33] = e->page_no;
    buf[34] = e->num_columns;
    memcpy(buf + 35, &e->num_rows,     4);
    memcpy(buf + 39, &e->num_pages,    4);
    buf[43] = e->tree_height;
    buf[44] = e->reserved;
    memcpy(buf + 45, &e->table_id, 4);
    /* bytes 49..55 zeroed by caller */
}

static void deserialize_relation_entry(const uint8_t *buf, RelationEntry *e)
{
    memcpy(e->relation_name, buf + 0, 32);
    e->relation_name[31] = '\0';
    e->is_valid     = buf[32];
    e->page_no      = buf[33];
    e->num_columns  = buf[34];
    memcpy(&e->num_rows,     buf + 35, 4);
    memcpy(&e->num_pages,    buf + 39, 4);
    e->tree_height = buf[43];
    e->reserved    = buf[44];
    memcpy(&e->table_id, buf + 45, 4);
}

static void pack_page0(const SchemaFile *sf, uint8_t *buf)
{
    memset(buf, 0, PAGE_SIZE);
    serialize_header(buf, &sf->header);
    for (int i = 0; i < MAX_RELATIONS_PER_SCHEMA; i++) {
        serialize_relation_entry(
            buf + SF_RELATION_OFFSET + i * SF_RELATION_SIZE,
            &sf->relations[i]);
    }
    uint32_t cs = crc32c(buf, SF_CHECKSUM_OFFSET);
    memcpy(buf + SF_CHECKSUM_OFFSET, &cs, 4);
}

static int unpack_page0(const uint8_t *buf, SchemaFile *sf)
{
    int rc = file_header_check_id(buf, FILETYPE_SCHEMA);
    if (rc != MYDB_OK) return rc;

    uint32_t stored;
    memcpy(&stored, buf + SF_CHECKSUM_OFFSET, 4);
    if (stored != crc32c(buf, SF_CHECKSUM_OFFSET))
        return MYDB_ERR_BAD_CHECKSUM;

    deserialize_header(buf, &sf->header);
    for (int i = 0; i < MAX_RELATIONS_PER_SCHEMA; i++) {
        deserialize_relation_entry(
            buf + SF_RELATION_OFFSET + i * SF_RELATION_SIZE,
            &sf->relations[i]);
    }
    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  Internal helpers                                                  */
/* ------------------------------------------------------------------ */

/* Linear scan for a valid slot by name. Returns index in relations[],
 * or -1 if not found. */
static int find_slot(const SchemaFile *sf, const char *name)
{
    for (int i = 0; i < MAX_RELATIONS_PER_SCHEMA; i++) {
        if (sf->relations[i].is_valid &&
            strncmp(sf->relations[i].relation_name, name, 32) == 0)
            return i;
    }
    return -1;
}

/* Lowest unused slot index, or -1 if all are valid. */
static int first_free_slot(const SchemaFile *sf)
{
    for (int i = 0; i < MAX_RELATIONS_PER_SCHEMA; i++) {
        if (!sf->relations[i].is_valid) return i;
    }
    return -1;
}

/* Lowest unused def-page number in 1..SCHEMA_FILE_PAGES-1, or 0 if none.
 * Page 0 is the slot directory; page numbers in valid entries are taken. */
static uint8_t first_free_def_page(const SchemaFile *sf)
{
    uint8_t taken[SCHEMA_FILE_PAGES];
    memset(taken, 0, sizeof(taken));
    for (int i = 0; i < MAX_RELATIONS_PER_SCHEMA; i++) {
        if (sf->relations[i].is_valid && sf->relations[i].page_no < SCHEMA_FILE_PAGES)
            taken[sf->relations[i].page_no] = 1;
    }
    for (uint8_t p = 1; p < SCHEMA_FILE_PAGES; p++) {
        if (!taken[p]) return p;
    }
    return 0;
}

/* Read one 16 KB page at byte offset page_no * PAGE_SIZE. */
static int read_page(int fd, uint8_t page_no, uint8_t *buf)
{
    off_t off = (off_t)page_no * PAGE_SIZE;
    if (pread(fd, buf, PAGE_SIZE, off) != (ssize_t)PAGE_SIZE)
        return MYDB_ERR;
    return MYDB_OK;
}

static int write_page(int fd, uint8_t page_no, const uint8_t *buf)
{
    off_t off = (off_t)page_no * PAGE_SIZE;
    if (pwrite(fd, buf, PAGE_SIZE, off) != (ssize_t)PAGE_SIZE)
        return MYDB_ERR;
    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  Public API                                                        */
/* ------------------------------------------------------------------ */

int schema_create(const char *path, uint32_t partition_id, uint32_t schema_id,
                  const char *schema_name, SchemaFile *out)
{
    if (!path || !schema_name || !out) return MYDB_ERR;
    if (strlen(schema_name) >= sizeof(out->header.schema_name))
        return MYDB_ERR;

    int fd = open(path, O_CREAT | O_EXCL | O_RDWR, 0644);
    if (fd < 0) return MYDB_ERR;

    if (ftruncate(fd, SCHEMA_FILE_SIZE) < 0) {
        close(fd);
        unlink(path);
        return MYDB_ERR;
    }

    memset(out, 0, sizeof(*out));
    out->fd = fd;
    strncpy(out->path, path, sizeof(out->path) - 1);

    out->header.partition_id  = partition_id;
    out->header.schema_id     = schema_id;
    strncpy(out->header.schema_name, schema_name,
            sizeof(out->header.schema_name) - 1);
    out->header.created_at    = now_yyyymmddhhmmss();
    out->header.last_modified = out->header.created_at;
    out->header.size_bytes    = 0;
    out->header.num_relations = 0;

    int rc = schema_save_page0(out);
    if (rc != MYDB_OK) {
        close(fd);
        unlink(path);
        out->fd = -1;
        return rc;
    }
    return MYDB_OK;
}

int schema_open(const char *path, SchemaFile *out)
{
    if (!path || !out) return MYDB_ERR;

    int fd = open(path, O_RDWR);
    if (fd < 0) return MYDB_ERR;

    uint8_t page0[PAGE_SIZE];
    if (read_page(fd, 0, page0) != MYDB_OK) {
        close(fd);
        return MYDB_ERR;
    }

    memset(out, 0, sizeof(*out));
    int rc = unpack_page0(page0, out);
    if (rc != MYDB_OK) {
        close(fd);
        return rc;
    }

    /* Phase 2: RelationDef pages are no longer eagerly loaded here — only
     * Page 0 (header + relations[] directory). Each relation's def page is
     * lazy-loaded on first access into PartitionBuffer's inner cache (see
     * pm_find_relation / partition_buffer.c's pb_get_relation_frame). */

    out->fd = fd;
    strncpy(out->path, path, sizeof(out->path) - 1);
    out->header.size_bytes = schema_compute_size_bytes(out);
    return MYDB_OK;
}

int schema_close(SchemaFile *sf)
{
    if (!sf || sf->fd < 0) return MYDB_ERR;
    int rc = close(sf->fd);
    sf->fd = -1;
    return (rc < 0) ? MYDB_ERR : MYDB_OK;
}

int schema_save_page0(SchemaFile *sf)
{
    if (!sf || sf->fd < 0) return MYDB_ERR;

    sf->header.last_modified = now_yyyymmddhhmmss();

    uint8_t buf[PAGE_SIZE];
    pack_page0(sf, buf);

    if (write_page(sf->fd, 0, buf) != MYDB_OK) return MYDB_ERR;
    return MYDB_OK;
}

int schema_reload_page0(SchemaFile *sf)
{
    if (!sf || sf->fd < 0) return MYDB_ERR;

    uint8_t buf[PAGE_SIZE];
    if (read_page(sf->fd, 0, buf) != MYDB_OK) return MYDB_ERR;

    int rc = unpack_page0(buf, sf);
    if (rc != MYDB_OK) return rc;

    sf->header.size_bytes = schema_compute_size_bytes(sf);
    return MYDB_OK;
}

int schema_add_relation(SchemaFile *sf, const RelationDef *def)
{
    if (!sf || !def || sf->fd < 0) return MYDB_ERR;
    if (strlen(def->relation_name) >= 32) return MYDB_ERR;
    if (find_slot(sf, def->relation_name) >= 0) return MYDB_ERR_DUPLICATE;

    int slot = first_free_slot(sf);
    if (slot < 0) return MYDB_ERR_FULL;

    uint8_t pno = first_free_def_page(sf);
    if (pno == 0) return MYDB_ERR_FULL;

    /* serialize def to its page first; only commit slot on success */
    uint8_t page[PAGE_SIZE];
    if (relation_def_serialize(def, page) != MYDB_OK) return MYDB_ERR;
    if (write_page(sf->fd, pno, page) != MYDB_OK)     return MYDB_ERR;

    RelationEntry *e = &sf->relations[slot];
    memset(e, 0, sizeof(*e));
    strncpy(e->relation_name, def->relation_name, sizeof(e->relation_name) - 1);
    e->is_valid     = 1;
    e->page_no      = pno;
    e->num_columns  = def->num_columns;
    e->num_rows    = 0;
    e->num_pages   = 0;
    e->tree_height = 1;   /* root page exists; tree starts at height 1 */
    e->reserved    = 0;
    e->table_id    = def->table_id;

    sf->header.num_relations++;
    sf->header.size_bytes = schema_compute_size_bytes(sf);

    return schema_save_page0(sf);
}

int schema_remove_relation(SchemaFile *sf, const char *relation_name)
{
    if (!sf || !relation_name || sf->fd < 0) return MYDB_ERR;

    int slot = find_slot(sf, relation_name);
    if (slot < 0) return MYDB_ERR_NOT_FOUND;

    uint8_t pno = sf->relations[slot].page_no;

    /* zero the def page on disk so the slot's page becomes reusable */
    uint8_t zero[PAGE_SIZE];
    memset(zero, 0, PAGE_SIZE);
    if (pno > 0 && pno < SCHEMA_FILE_PAGES) {
        if (write_page(sf->fd, pno, zero) != MYDB_OK) return MYDB_ERR;
    }

    memset(&sf->relations[slot], 0, sizeof(sf->relations[slot]));
    if (sf->header.num_relations > 0) sf->header.num_relations--;
    sf->header.size_bytes = schema_compute_size_bytes(sf);

    return schema_save_page0(sf);
}

RelationEntry *schema_find_relation_stat(SchemaFile *sf, const char *relation_name)
{
    if (!sf || !relation_name) return NULL;
    int slot = find_slot(sf, relation_name);
    return (slot < 0) ? NULL : &sf->relations[slot];
}

int schema_load_relation_page(SchemaFile *sf, uint8_t page_no, RelationDef *out)
{
    if (!sf || !out || sf->fd < 0) return MYDB_ERR;
    if (page_no == 0 || page_no >= SCHEMA_FILE_PAGES) return MYDB_ERR;

    uint8_t buf[PAGE_SIZE];
    if (read_page(sf->fd, page_no, buf) != MYDB_OK) return MYDB_ERR;
    if (relation_def_deserialize(out, buf) != MYDB_OK) return MYDB_ERR;

    /* Stamp the owning schema (in-memory only, not serialized) so the
     * storage engine can build the table's path and key its open-table
     * cache by (owner_schema, relation_name) without holding any
     * active-schema state of its own. */
    strncpy(out->owner_schema, sf->header.schema_name,
            sizeof(out->owner_schema) - 1);
    return MYDB_OK;
}

int schema_flush_relation(SchemaFile *sf, const RelationDef *def)
{
    if (!sf || !def || sf->fd < 0) return MYDB_ERR;

    int slot = find_slot(sf, def->relation_name);
    if (slot < 0) return MYDB_ERR_NOT_FOUND;

    uint8_t page[PAGE_SIZE];
    if (relation_def_serialize(def, page) != MYDB_OK)
        return MYDB_ERR;
    if (write_page(sf->fd, sf->relations[slot].page_no, page) != MYDB_OK)
        return MYDB_ERR;
    return MYDB_OK;
}

int schema_update_stats(SchemaFile *sf, const char *relation_name,
                        uint32_t num_rows, uint32_t num_pages,
                        uint8_t tree_height)
{
    if (!sf || !relation_name || sf->fd < 0) return MYDB_ERR;

    int slot = find_slot(sf, relation_name);
    if (slot < 0) return MYDB_ERR_NOT_FOUND;

    RelationEntry *e = &sf->relations[slot];
    e->num_rows    = num_rows;
    e->num_pages   = num_pages;
    e->tree_height = tree_height;
    sf->header.size_bytes = schema_compute_size_bytes(sf);

    /* Phase 3: no longer persisted here — caller marks page0_dirty via
     * pb_mark_page0_dirty (partition_buffer.h); pb_flush_slot_dirty
     * writes it out later, on COMMIT/eviction/shutdown. */
    return MYDB_OK;
}

int schema_bump_relation_pages(SchemaFile *sf, const char *relation_name,
                               int32_t delta)
{
    if (!sf || !relation_name || sf->fd < 0) return MYDB_ERR;

    int slot = find_slot(sf, relation_name);
    if (slot < 0) return MYDB_ERR_NOT_FOUND;

    RelationEntry *e = &sf->relations[slot];

    /* Clamp at zero — a v1 storage layer with no per-page free path can
     * still call us with delta<0 on file-level free, but driving the
     * counter negative would mis-report the partition's used_bytes. */
    if (delta < 0 && (uint32_t)(-delta) > e->num_pages) return MYDB_ERR;

    e->num_pages = (uint32_t)((int64_t)e->num_pages + delta);
    sf->header.size_bytes = schema_compute_size_bytes(sf);

    /* Phase 3: no longer persisted here — see schema_update_stats's
     * comment above. */
    return MYDB_OK;
}

int schema_bump_relation_rows(SchemaFile *sf, const char *relation_name,
                              int32_t delta)
{
    if (!sf || !relation_name || sf->fd < 0) return MYDB_ERR;

    int slot = find_slot(sf, relation_name);
    if (slot < 0) return MYDB_ERR_NOT_FOUND;

    RelationEntry *e = &sf->relations[slot];

    /* Clamp at zero — a DELETE on a table that already reports 0 rows
     * (e.g. after a crash during a previous bump) must not underflow. */
    if (delta < 0 && (uint32_t)(-delta) > e->num_rows) return MYDB_ERR;

    e->num_rows = (uint32_t)((int64_t)e->num_rows + delta);

    /* Phase 3: no longer persisted here — see schema_update_stats's
     * comment above. */
    return MYDB_OK;
}

uint64_t schema_compute_size_bytes(const SchemaFile *sf)
{
    if (!sf) return 0;
    uint64_t total = 0;
    for (int i = 0; i < MAX_RELATIONS_PER_SCHEMA; i++) {
        if (sf->relations[i].is_valid)
            total += (uint64_t)sf->relations[i].num_pages * PAGE_SIZE;
    }
    return total;
}
