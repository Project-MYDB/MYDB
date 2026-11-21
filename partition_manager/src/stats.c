/*
 * stats.c — optimizer statistics file I/O for __stats.mydb.
 *
 * This file owns ONLY the file format: create, open, close, load a
 * relation stats page, save it back.  The computation (scanning rows,
 * building MCVs / histograms) lives in storage.c (storage_analyze_table).
 *
 * File layout recap (from stats.h):
 *
 *   Page 0 (16 KB):
 *     0..7   : FileHeaderId (MYDB_MAGIC / version / FILETYPE_STATS)
 *     8..39  : schema_name[32]
 *     40..43 : CRC-32C checksum over bytes 0..39
 *     44..   : zeroed
 *
 *   Pages 1..64 (relation stats pages, allocated lazily on ANALYZE):
 *     0..7   : FileHeaderId
 *     8..11  : slot_idx    (uint32, LE)
 *     12..15 : blob_used   (uint32, LE)
 *     16..2063: ColumnStats[MAX_COLUMNS] (32 × 64 = 2048 B, raw memcpy)
 *     2064..2067: CRC-32C checksum over bytes 0..2063
 *     2068..2071: padding (keeps blob pool 8-byte aligned at offset 2072)
 *     2072..16383: blob pool (MCVEntry / HistBucket raw bytes)
 */

#include "stats.h"
#include "file_header.h"
#include "checksum.h"

#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

/* ------------------------------------------------------------------ */
/*  Page 0 layout constants                                            */
/* ------------------------------------------------------------------ */
#define P0_SCHEMA_OFF   8    /* schema_name[32] at offset 8  */
#define P0_CKSUM_OFF   40    /* checksum covers bytes 0..39  */

/* ------------------------------------------------------------------ */
/*  Tiny LE helpers (consistent with the rest of the storage engine)  */
/* ------------------------------------------------------------------ */
static uint32_t rd_u32(const uint8_t *b, int off)
{
    return (uint32_t)b[off]
         | ((uint32_t)b[off+1] << 8)
         | ((uint32_t)b[off+2] << 16)
         | ((uint32_t)b[off+3] << 24);
}

static void wr_u32(uint8_t *b, int off, uint32_t v)
{
    b[off]   = (uint8_t)(v);
    b[off+1] = (uint8_t)(v >> 8);
    b[off+2] = (uint8_t)(v >> 16);
    b[off+3] = (uint8_t)(v >> 24);
}

/* ------------------------------------------------------------------ */
/*  stats_create                                                       */
/* ------------------------------------------------------------------ */
int stats_create(const char *path, const char *schema_name, StatsFile *out)
{
    int fd = open(path, O_RDWR | O_CREAT | O_EXCL, 0644);
    if (fd < 0)
        return MYDB_ERR;

    /* Pre-allocate the full file so relation pages exist as zeroed space. */
    off_t total = (off_t)(MAX_RELATIONS_PER_SCHEMA + 1) * PAGE_SIZE;
    if (ftruncate(fd, total) < 0) {
        close(fd);
        return MYDB_ERR;
    }

    /* Build and write page 0. */
    uint8_t buf[PAGE_SIZE];
    memset(buf, 0, PAGE_SIZE);
    file_header_write_id(buf, FILETYPE_STATS);
    strncpy((char *)(buf + P0_SCHEMA_OFF), schema_name, 31);
    uint32_t cksum = crc32c(buf, (size_t)P0_CKSUM_OFF);
    wr_u32(buf, P0_CKSUM_OFF, cksum);

    if (pwrite(fd, buf, PAGE_SIZE, 0) != PAGE_SIZE) {
        close(fd);
        return MYDB_ERR;
    }
    fsync(fd);

    memset(out, 0, sizeof(*out));
    out->fd = fd;
    strncpy(out->path, path, sizeof(out->path) - 1);
    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  stats_open                                                         */
/* ------------------------------------------------------------------ */
int stats_open(const char *path, StatsFile *out)
{
    int fd = open(path, O_RDWR);
    if (fd < 0)
        return (errno == ENOENT) ? MYDB_ERR_NOT_FOUND : MYDB_ERR;

    /* Read and validate page 0. */
    uint8_t buf[PAGE_SIZE];
    if (pread(fd, buf, PAGE_SIZE, 0) != PAGE_SIZE) {
        close(fd);
        return MYDB_ERR;
    }

    int rc = file_header_check_id(buf, FILETYPE_STATS);
    if (rc != MYDB_OK) {
        close(fd);
        return rc;
    }

    uint32_t stored = rd_u32(buf, P0_CKSUM_OFF);
    uint32_t calc   = crc32c(buf, (size_t)P0_CKSUM_OFF);
    if (stored != calc) {
        close(fd);
        return MYDB_ERR_BAD_CHECKSUM;
    }

    memset(out, 0, sizeof(*out));
    out->fd = fd;
    strncpy(out->path, path, sizeof(out->path) - 1);
    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  stats_close                                                        */
/* ------------------------------------------------------------------ */
int stats_close(StatsFile *sf)
{
    if (sf && sf->fd >= 0) {
        close(sf->fd);
        sf->fd = -1;
    }
    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  stats_load_relation                                                */
/*                                                                     */
/*  The on-disk page format is validated in two steps:                 */
/*    1. file_header_check_id: if the magic is zero, the page was      */
/*       never written → MYDB_ERR_NOT_FOUND (graceful, not an error).  */
/*    2. CRC-32C checksum over the fixed header + ColumnStats array    */
/*       (bytes 0..2063): guards against truncated writes.             */
/* ------------------------------------------------------------------ */
int stats_load_relation(StatsFile *sf, int slot_idx)
{
    if (slot_idx < 0 || slot_idx >= MAX_RELATIONS_PER_SCHEMA) return MYDB_ERR;
    if (sf->slot_loaded[slot_idx]) return MYDB_OK;

    uint8_t buf[PAGE_SIZE];
    off_t disk_off = (off_t)(slot_idx + 1) * PAGE_SIZE;
    if (pread(sf->fd, buf, PAGE_SIZE, disk_off) != PAGE_SIZE)
        return MYDB_ERR;

    /*
     * A zeroed page means ANALYZE has never run for this relation.
     * file_header_check_id catches the all-zero case (magic mismatch)
     * and returns MYDB_ERR_BAD_MAGIC.  Map that to NOT_FOUND so callers
     * can distinguish "missing stats" from a real corruption.
     */
    int rc = file_header_check_id(buf, FILETYPE_STATS);
    if (rc != MYDB_OK)
        return MYDB_ERR_NOT_FOUND;

    /* Verify the checksum before trusting any data. */
    uint32_t stored = rd_u32(buf, STATS_REL_CKSUM_OFF);
    uint32_t calc   = crc32c(buf, (size_t)STATS_REL_CKSUM_OFF);
    if (stored != calc)
        return MYDB_ERR_BAD_CHECKSUM;

    /* Deserialise into the in-memory RelationStats. */
    RelationStats *rs = &sf->pages[slot_idx];
    memset(rs, 0, sizeof(*rs));

    rs->blob_used = rd_u32(buf, STATS_REL_BLOB_USED);
    /* ColumnStats array is plain data — no padding anywhere inside it. */
    memcpy(rs->cols, buf + STATS_REL_COLS_OFF, sizeof(rs->cols));

    /*
     * Copy only the live portion of the blob pool to avoid a 14 KB memcpy
     * when blob_used is small (the common case: one or two columns with stats).
     */
    if (rs->blob_used > 0 && rs->blob_used <= STATS_BLOB_POOL_SIZE)
        memcpy(rs->blob_pool.b, buf + STATS_BLOB_START, rs->blob_used);

    rs->dirty = 0;
    sf->slot_loaded[slot_idx] = 1;
    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  stats_save_relation                                                */
/* ------------------------------------------------------------------ */
int stats_save_relation(StatsFile *sf, int slot_idx)
{
    if (slot_idx < 0 || slot_idx >= MAX_RELATIONS_PER_SCHEMA) return MYDB_ERR;
    if (!sf->slot_loaded[slot_idx]) return MYDB_ERR;

    RelationStats *rs = &sf->pages[slot_idx];

    uint8_t buf[PAGE_SIZE];
    memset(buf, 0, PAGE_SIZE);

    file_header_write_id(buf, FILETYPE_STATS);
    wr_u32(buf, STATS_REL_SLOT_OFF,  (uint32_t)slot_idx);
    wr_u32(buf, STATS_REL_BLOB_USED, rs->blob_used);
    memcpy(buf + STATS_REL_COLS_OFF, rs->cols, sizeof(rs->cols));

    /* Checksum covers the fixed header + ColumnStats (bytes 0..2063). */
    uint32_t cksum = crc32c(buf, (size_t)STATS_REL_CKSUM_OFF);
    wr_u32(buf, STATS_REL_CKSUM_OFF, cksum);

    /* Blob pool data follows after the padding gap. */
    if (rs->blob_used > 0)
        memcpy(buf + STATS_BLOB_START, rs->blob_pool.b, rs->blob_used);

    off_t disk_off = (off_t)(slot_idx + 1) * PAGE_SIZE;
    if (pwrite(sf->fd, buf, PAGE_SIZE, disk_off) != PAGE_SIZE)
        return MYDB_ERR;
    fsync(sf->fd);

    rs->dirty = 0;
    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  stats_get_column                                                   */
/* ------------------------------------------------------------------ */
ColumnStats *stats_get_column(StatsFile *sf, int slot_idx, int col_idx)
{
    if (slot_idx < 0 || slot_idx >= MAX_RELATIONS_PER_SCHEMA) return NULL;
    if (col_idx  < 0 || col_idx  >= MAX_COLUMNS)              return NULL;
    if (!sf->slot_loaded[slot_idx])                            return NULL;
    return &sf->pages[slot_idx].cols[col_idx];
}

/* ------------------------------------------------------------------ */
/*  stats_get_mcv                                                      */
/* ------------------------------------------------------------------ */
MCVEntry *stats_get_mcv(StatsFile *sf, int slot_idx, int col_idx)
{
    ColumnStats *cs = stats_get_column(sf, slot_idx, col_idx);
    if (!cs || cs->stats_type != STATS_TYPE_MCV || cs->blob_count == 0)
        return NULL;
    /* blob_pool.b is 8-byte aligned (union with uint64_t[]).
     * blob_offset is always a multiple of sizeof(MCVEntry)=16,
     * so the cast to MCVEntry* is valid. */
    return (MCVEntry *)(sf->pages[slot_idx].blob_pool.b + cs->blob_offset);
}

/* ------------------------------------------------------------------ */
/*  stats_get_hist                                                     */
/* ------------------------------------------------------------------ */
HistBucket *stats_get_hist(StatsFile *sf, int slot_idx, int col_idx)
{
    ColumnStats *cs = stats_get_column(sf, slot_idx, col_idx);
    if (!cs || cs->stats_type != STATS_TYPE_HISTOGRAM || cs->blob_count == 0)
        return NULL;
    return (HistBucket *)(sf->pages[slot_idx].blob_pool.b + cs->blob_offset);
}

/* ------------------------------------------------------------------ */
/*  stats_reset_relation                                               */
/* ------------------------------------------------------------------ */
void stats_reset_relation(StatsFile *sf, int slot_idx)
{
    if (slot_idx < 0 || slot_idx >= MAX_RELATIONS_PER_SCHEMA) return;
    RelationStats *rs = &sf->pages[slot_idx];
    memset(rs, 0, sizeof(*rs));
    rs->dirty = 1;
    sf->slot_loaded[slot_idx] = 1;
}

/* ------------------------------------------------------------------ */
/*  stats_write_mcv                                                    */
/* ------------------------------------------------------------------ */
int stats_write_mcv(StatsFile *sf, int slot_idx, int col_idx,
                    const MCVEntry *entries, uint16_t count)
{
    if (slot_idx < 0 || slot_idx >= MAX_RELATIONS_PER_SCHEMA) return MYDB_ERR;
    if (col_idx  < 0 || col_idx  >= MAX_COLUMNS)              return MYDB_ERR;
    if (!sf->slot_loaded[slot_idx])                            return MYDB_ERR;
    if (count > STATS_MAX_ENTRIES)                             return MYDB_ERR_FULL;

    RelationStats *rs = &sf->pages[slot_idx];
    size_t needed = (size_t)count * sizeof(MCVEntry);
    if ((size_t)rs->blob_used + needed > STATS_BLOB_POOL_SIZE)
        return MYDB_ERR_FULL;

    ColumnStats *cs = &rs->cols[col_idx];
    cs->blob_offset = rs->blob_used;
    cs->blob_count  = count;
    cs->stats_type  = STATS_TYPE_MCV;
    cs->has_stats   = 1;

    memcpy(rs->blob_pool.b + rs->blob_used, entries, needed);
    rs->blob_used += (uint32_t)needed;
    rs->dirty = 1;
    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  stats_write_hist                                                   */
/* ------------------------------------------------------------------ */
int stats_write_hist(StatsFile *sf, int slot_idx, int col_idx,
                     const HistBucket *buckets, uint16_t count)
{
    if (slot_idx < 0 || slot_idx >= MAX_RELATIONS_PER_SCHEMA) return MYDB_ERR;
    if (col_idx  < 0 || col_idx  >= MAX_COLUMNS)              return MYDB_ERR;
    if (!sf->slot_loaded[slot_idx])                            return MYDB_ERR;
    if (count > STATS_MAX_ENTRIES)                             return MYDB_ERR_FULL;

    RelationStats *rs = &sf->pages[slot_idx];
    size_t needed = (size_t)count * sizeof(HistBucket);
    if ((size_t)rs->blob_used + needed > STATS_BLOB_POOL_SIZE)
        return MYDB_ERR_FULL;

    ColumnStats *cs = &rs->cols[col_idx];
    cs->blob_offset = rs->blob_used;
    cs->blob_count  = count;
    cs->stats_type  = STATS_TYPE_HISTOGRAM;
    cs->has_stats   = 1;

    memcpy(rs->blob_pool.b + rs->blob_used, buckets, needed);
    rs->blob_used += (uint32_t)needed;
    rs->dirty = 1;
    return MYDB_OK;
}
