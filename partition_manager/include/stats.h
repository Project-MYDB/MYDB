#ifndef STATS_H
#define STATS_H

#include "common.h"
#include "relation_def.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * stats.h — optimizer statistics for the cost-based planner.
 *
 * __stats.mydb lives alongside __schema.mydb in every schema directory:
 *
 *   <partition>/<schema>/__stats.mydb
 *
 * File layout (all pages are PAGE_SIZE = 16 KB):
 *   Page 0        : file header (magic, schema_name, checksum)
 *   Pages 1..64   : one relation stats page per RelationDef slot
 *                   (allocated lazily on the first ANALYZE TABLE run)
 *
 * Relation stats page layout:
 *   Bytes  0..7   : FileHeaderId  (magic / version / FILETYPE_STATS)
 *   Bytes  8..11  : slot_idx      (uint32, LE)
 *   Bytes 12..15  : blob_used     (uint32, LE — bytes consumed in blob pool)
 *   Bytes 16..2063: ColumnStats[MAX_COLUMNS] (32 × 64 = 2048 B, raw)
 *   Bytes 2064..2067: CRC-32C checksum over bytes 0..2063
 *   Bytes 2068..2071: padding (to align blob pool to 8 B)
 *   Bytes 2072..16383: blob pool (14312 B — MCV/histogram entries)
 *
 * Stats are written by storage_analyze_table and read by planner_choose_path.
 * Both functions are in the storage_engine library.  All I/O bypasses the
 * buffer pool (direct pread/pwrite, like all other metadata files).
 */

/* ------------------------------------------------------------------ */
/*  Constants                                                          */
/* ------------------------------------------------------------------ */

#define STATS_TYPE_NONE       0   /* no stats collected for this column yet */
#define STATS_TYPE_MCV        1   /* Most Common Values list in blob pool    */
#define STATS_TYPE_HISTOGRAM  2   /* equi-height histogram in blob pool      */

/*
 * Maximum entries per column (both MCV list and histogram buckets).
 * Worst-case storage: STATS_MAX_ENTRIES × 16 B × MAX_COLUMNS
 *                   = 16 × 16 × 32 = 8192 B ≤ 14312 B (blob pool).
 */
#define STATS_MAX_ENTRIES     16

/* Relation stats page offsets */
#define STATS_REL_SLOT_OFF    8     /* slot_idx:  4 B at offset 8            */
#define STATS_REL_BLOB_USED   12    /* blob_used: 4 B at offset 12           */
#define STATS_REL_COLS_OFF    16    /* ColumnStats array: bytes 16..2063     */
#define STATS_REL_CKSUM_OFF   2064  /* CRC-32C checksum: 4 B at offset 2064 */
#define STATS_BLOB_START      2072  /* blob pool: 8-byte aligned start       */
#define STATS_BLOB_POOL_SIZE  (PAGE_SIZE - STATS_BLOB_START)  /* 14312 B    */
#define STATS_BLOB_POOL_WORDS (STATS_BLOB_POOL_SIZE / 8)      /* 1789       */

/* ------------------------------------------------------------------ */
/*  ColumnStats — 64 bytes, one per column, stored in the stats page  */
/* ------------------------------------------------------------------ */
typedef struct {
    uint8_t  has_stats;       /* 1 once ANALYZE TABLE has populated this column */
    uint8_t  stats_type;      /* STATS_TYPE_*                                   */
    uint16_t blob_count;      /* number of MCVEntry or HistBucket entries        */
    uint32_t num_distinct;    /* NDV — distinct non-null values seen             */
    uint32_t num_nulls;       /* count of NULL values                            */
    uint32_t total_rows;      /* total rows at the time ANALYZE ran              */
    int64_t  min_numeric;     /* min value encoded as int64 (INT/DECIMAL/DATE/  */
                              /* DATETIME); 0 for VARCHAR, BOOL, ENUM           */
    int64_t  max_numeric;     /* max value (same encoding)                       */
    uint32_t blob_offset;     /* byte offset in this relation's blob pool where  */
                              /* this column's entries start                     */
    uint8_t  pad[28];         /* pad to exactly 64 bytes                         */
} ColumnStats;

/* ------------------------------------------------------------------ */
/*  Blob pool entry types (16 bytes each, 8-byte aligned)             */
/* ------------------------------------------------------------------ */

/* One entry in a Most Common Values list. */
typedef struct {
    int64_t  value;       /* column value, same int64 encoding as min/max   */
    uint32_t frequency;   /* how many rows carry this value                  */
    uint32_t pad;
} MCVEntry;

/* One bucket in an equi-height histogram. */
typedef struct {
    int64_t  upper_bound; /* inclusive upper bound of the bucket             */
    uint32_t row_count;   /* rows that fall in this bucket                   */
    uint32_t pad;
} HistBucket;

/* ------------------------------------------------------------------ */
/*  In-memory stats page for one relation                             */
/*                                                                    */
/*  blob_pool uses a union so that its first member (the uint64_t     */
/*  array) forces 8-byte alignment on the byte view.  This makes     */
/*  casting blob_pool.b + blob_offset to MCVEntry* / HistBucket*     */
/*  valid, because all offsets are multiples of 16 (entry size).     */
/* ------------------------------------------------------------------ */
typedef union {
    uint8_t  b[STATS_BLOB_POOL_SIZE];
    uint64_t _align[STATS_BLOB_POOL_WORDS];   /* alignment anchor, never read */
} StatsBlobPool;

typedef struct {
    ColumnStats  cols[MAX_COLUMNS];   /* 32 × 64 = 2048 B */
    uint32_t     blob_used;           /* bytes consumed in blob_pool */
    uint8_t      dirty;               /* 1 if modified since last save */
    uint8_t      _pad[3];
    StatsBlobPool blob_pool;          /* MCV / histogram raw bytes */
} RelationStats;

/* ------------------------------------------------------------------ */
/*  StatsFile handle                                                   */
/* ------------------------------------------------------------------ */
typedef struct {
    int     fd;                                         /* open fd, -1 when closed */
    uint8_t _fdpad[4];
    char    path[256];
    uint8_t slot_loaded[MAX_RELATIONS_PER_SCHEMA];      /* 1 if page is in memory */
    uint8_t _slpad[4];
    RelationStats pages[MAX_RELATIONS_PER_SCHEMA];      /* lazy-loaded stats pages */
} StatsFile;

/* ------------------------------------------------------------------ */
/*  Lifecycle                                                          */
/* ------------------------------------------------------------------ */

/*
 * Create a new __stats.mydb for a schema.
 * Pre-allocates (MAX_RELATIONS_PER_SCHEMA + 1) × PAGE_SIZE zeroed bytes,
 * writes page 0 (header + checksum), and leaves all relation pages zeroed
 * (they are written lazily by stats_save_relation).
 *
 * Fails with MYDB_ERR if the file already exists.
 */
int stats_create(const char *path, const char *schema_name, StatsFile *out);

/*
 * Open an existing __stats.mydb.
 * Reads page 0 and validates magic, version, file_type, and checksum.
 * Relation pages are NOT loaded here; use stats_load_relation on demand.
 *
 * Returns MYDB_ERR_NOT_FOUND if the file does not exist yet (ANALYZE has
 * never been run for this schema).  Callers must treat this gracefully by
 * falling back to no-stats / default selectivity.
 */
int stats_open(const char *path, StatsFile *out);

/* Close the file descriptor. Does NOT flush dirty pages. */
int stats_close(StatsFile *sf);

/* ------------------------------------------------------------------ */
/*  Relation page I/O                                                  */
/* ------------------------------------------------------------------ */

/*
 * Load the stats page for relation slot `slot_idx` into memory.
 * No-op (returns MYDB_OK) if the page is already loaded.
 *
 * Returns MYDB_ERR_NOT_FOUND if the page has never been written
 * (ANALYZE has not run for this relation yet).
 */
int stats_load_relation(StatsFile *sf, int slot_idx);

/*
 * Serialize and write the in-memory stats page for `slot_idx` to disk.
 * The page must have been initialised via stats_reset_relation (and
 * populated via stats_write_mcv / stats_write_hist) before calling this.
 */
int stats_save_relation(StatsFile *sf, int slot_idx);

/* ------------------------------------------------------------------ */
/*  Column stats access (read)                                         */
/* ------------------------------------------------------------------ */

/*
 * Return a pointer to the ColumnStats for column `col_idx` in relation
 * slot `slot_idx`.  Returns NULL if the page is not loaded or the
 * indices are out of range.  Pointer is valid until stats_close.
 */
ColumnStats *stats_get_column(StatsFile *sf, int slot_idx, int col_idx);

/*
 * Return a pointer to the first MCVEntry in the blob pool for this
 * column.  Read `col_stats->blob_count` entries from the returned
 * pointer.  Returns NULL if the column's stats_type != STATS_TYPE_MCV
 * or the page is not loaded.
 */
MCVEntry *stats_get_mcv(StatsFile *sf, int slot_idx, int col_idx);

/*
 * Return a pointer to the first HistBucket for this column.  Read
 * `col_stats->blob_count` buckets.  Returns NULL if
 * stats_type != STATS_TYPE_HISTOGRAM or the page is not loaded.
 */
HistBucket *stats_get_hist(StatsFile *sf, int slot_idx, int col_idx);

/* ------------------------------------------------------------------ */
/*  Column stats population (write, called by storage_analyze_table)  */
/* ------------------------------------------------------------------ */

/*
 * Prepare a relation's stats page for a new ANALYZE run.
 * Zeroes the ColumnStats array, blob pool, and blob_used counter,
 * marks the page dirty and loaded.  Does NOT write to disk.
 */
void stats_reset_relation(StatsFile *sf, int slot_idx);

/*
 * Write the MCV list for column `col_idx` into the blob pool.
 * Sets has_stats, stats_type, blob_count, and blob_offset on the
 * ColumnStats entry.  Marks the page dirty.
 *
 * Returns MYDB_ERR_FULL if `count` exceeds STATS_MAX_ENTRIES or the
 * blob pool cannot accommodate the entries.
 */
int stats_write_mcv(StatsFile *sf, int slot_idx, int col_idx,
                    const MCVEntry *entries, uint16_t count);

/*
 * Write the histogram buckets for column `col_idx` into the blob pool.
 * Same contract as stats_write_mcv.
 */
int stats_write_hist(StatsFile *sf, int slot_idx, int col_idx,
                     const HistBucket *buckets, uint16_t count);

#ifdef __cplusplus
}
#endif

#endif /* STATS_H */
