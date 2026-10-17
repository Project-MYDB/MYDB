#ifndef COMMON_H
#define COMMON_H

#include <stdint.h>


/* ------------------------------------------------------------------ */
/*  Page layout constants                                             */
/* ------------------------------------------------------------------ */
#define PAGE_SIZE               16384   /* 16 KB per page, matches InnoDB */
#define PAGE_HEADER_SIZE        38      /* bytes at start of every page */
#define INFIMUM_SUPREMUM_SIZE   26      /* two fixed boundary records (13B each) */
#define USER_RECORDS_OFFSET     64      /* first byte available for user records */
#define PAGE_TRAILER_SIZE       8       /* checksum + LSN at end of page */
#define RECORD_HEADER_SIZE      5       /* 5-byte header preceding every record */

/* ------------------------------------------------------------------ */
/*  WAL format constants (MYDB_WAL_IMPLEMENTATION.md §8.3). System-    */
/*  wide because every WAL consumer (record segments, and later the   */
/*  ring buffer / Flusher / recovery) needs the same page/record size */
/*  budget — unlike WalRecordHeader/WalPageHeader/WalSegmentHeader     */
/*  themselves, which are wal/'s own structs, not declared here.      */
/* ------------------------------------------------------------------ */
#define WAL_PAGE_SIZE           4096
#define WAL_PAGE_HEADER_SIZE      24   /* was 32 (36 before that) -- data_len and 6 reserved
                                         bytes dropped from WalPageHeader in T-039 */
#define WAL_RECORD_HEADER_SIZE    44
#define WAL_PAGE_USABLE         4072   /* WAL_PAGE_SIZE - WAL_PAGE_HEADER_SIZE (was 4064, 4060 before that) */
/* WAL_MAX_RECORD_SIZE (4064, was 4060 -- records above it used to redirect
 * to LARGE_WAL) and WAL_MAX_ROW_BODY (4020, was 4016 -- WAL_MAX_RECORD_SIZE -
 * WAL_RECORD_HEADER_SIZE) were removed with T-039: records span pages and
 * segments now, so the only limit on a record is the ring buffer's size. */

/* ------------------------------------------------------------------ */
/*  Size limits                                                       */
/* ------------------------------------------------------------------ */
#define MAX_TABLE_NAME      64
#define MAX_COLUMN_NAME     64
#define MAX_COLUMNS         32
#define MAX_VARCHAR_LEN     150
#define MAX_ENUM_VALUES     16
#define MAX_ENUM_STR_LEN    32
#define MAX_TABLES          64
#define BUFFER_POOL_SIZE    4096      /* number of frames in the buffer pool, 64MB buffer pool size per partition */

/* ------------------------------------------------------------------ */
/*  Hidden system column sizes (InnoDB-style, present in every row)   */
/* ------------------------------------------------------------------ */
#define DB_TRX_ID_SIZE      6   /* transaction ID that last modified the row */
#define DB_ROLL_PTR_SIZE    7   /* rollback pointer (unused in Phase 1, stored as 0) */
#define DB_ROW_ID_SIZE      6   /* synthetic row ID, only when table has no PK */

/* ------------------------------------------------------------------ */
/*  Special values                                                    */
/* ------------------------------------------------------------------ */
#define INVALID_PAGE        UINT32_MAX  /* sentinel for "no page" */
#define MYDB_MAGIC          0x4D594442  /* "MYDB" — written to every MyDB file */

/* ------------------------------------------------------------------ */
/*  File type constants (all files share MYDB_MAGIC; file_type field  */
/*  at byte offset 6 of every file header distinguishes them)         */
/* ------------------------------------------------------------------ */
#define FILETYPE_DATABASE            1   /* __database.mydb        — engine registry */
#define FILETYPE_CATALOG             2   /* __catalog.mydb         — partition catalog */
#define FILETYPE_SCHEMA              3   /* __schema.mydb          — schema definition */
#define FILETYPE_RELATION            4   /* <relation>.mydb        — B+ tree data file */
#define FILETYPE_USERS               5   /* system_schema/users.mydb */
#define FILETYPE_PRIVILEGES          6   /* system_schema/privileges.mydb */
#define FILETYPE_STATS               7   /* __stats.mydb           — CBO optimizer stats */
#define FILETYPE_WAL_PAGE            8   /* wal/wal_<N>.mydb page format */
#define FILETYPE_WAL_SEGMENT         9   /* wal/wal_<N>.mydb segment header */
/* 10-13 belonged to large_wal, moved out to ../large_wal_stash. Kept
 * defined so the numbers are never handed to another file type. */
#define FILETYPE_LARGE_WAL_PAGE     10   /* large_wal/lw_<N>.mydb page format */
#define FILETYPE_LARGE_WAL_SEGMENT  11   /* large_wal/lw_<N>.mydb segment header */
#define FILETYPE_LARGE_WAL_INDEX    12   /* wal/large_wal_index.mydb */
#define FILETYPE_LARGE_WAL_STATE    13   /* wal/large_wal_state.mydb */

/* ------------------------------------------------------------------ */
/*  On-disk format version. Bumped when any file layout changes in    */
/*  a way that older binaries can't read.                             */
/*  v3: bumped 2 -> 3 when every metadata-file checksum moved from    */
/*  FNV-1a to CRC32 (WAL Phase 1) — old files fail MYDB_ERR_BAD_      */
/*  CHECKSUM under the new algorithm, not a version mismatch, but the */
/*  on-disk format genuinely changed.                                 */
/* ------------------------------------------------------------------ */
#define MYDB_FORMAT_VERSION  3

/* ------------------------------------------------------------------ */
/*  v2 four-level hierarchy capacities                                */
/* ------------------------------------------------------------------ */
#define MAX_PARTITIONS               16   /* per __database.mydb */
#define MAX_SCHEMAS_PER_PARTITION    64   /* per __catalog.mydb */
#define MAX_RELATIONS_PER_SCHEMA     64   /* per __schema.mydb */

/* ------------------------------------------------------------------ */
/*  Fixed-size metadata file sizes                                    */
/* ------------------------------------------------------------------ */
#define DATABASE_FILE_SIZE   8192                          /* 8 KB */
#define CATALOG_FILE_SIZE    4096                          /* 4 KB */
#define SCHEMA_FILE_PAGES    65                            /* 1 dir + 64 RelationDef */
#define SCHEMA_FILE_SIZE    (SCHEMA_FILE_PAGES * PAGE_SIZE) /* ~1 MB */

/* system_schema/users.mydb: 32 user slots, ~8 KB total */
#define USERS_FILE_SIZE        8192
#define USERS_MAX_SLOTS          32

/* system_schema/privileges.mydb: 256 grant slots, ~16 KB total */
#define PRIVILEGES_FILE_SIZE   16384
#define PRIVILEGES_MAX_SLOTS     256

/* Identity / credential field widths (raw bytes on disk) */
#define MAX_USERNAME             32   /* VARCHAR(32) — design doc §3.1 */
#define USER_PASSWORD_HASH_LEN   32   /* SHA-256 raw output */
#define USER_PASSWORD_SALT_LEN   16   /* per-user random salt */

/* ------------------------------------------------------------------ */
/*  Return codes                                                      */
/* ------------------------------------------------------------------ */
#define MYDB_OK                  0
#define MYDB_ERR                (-1)
#define MYDB_ERR_NOT_FOUND      (-2)
#define MYDB_ERR_DUPLICATE      (-3)
#define MYDB_ERR_FULL           (-4)
#define MYDB_ERR_FK_VIOLATION   (-5)
#define MYDB_ERR_NULL_VIOLATION (-6)
#define MYDB_ERR_NO_TXN         (-7)
#define MYDB_ERR_PERM           (-8)   /* user lacks permission for the operation */
#define MYDB_ERR_CROSS_SCHEMA   (-9)   /* query references a schema other than active */
#define MYDB_ERR_BAD_MAGIC     (-10)   /* file header magic mismatch */
#define MYDB_ERR_BAD_FILE_TYPE (-11)   /* file_type field doesn't match expected */
#define MYDB_ERR_BAD_VERSION   (-12)   /* on-disk version newer than this binary */
#define MYDB_ERR_BAD_CHECKSUM  (-13)   /* file or page checksum mismatch */

/* ------------------------------------------------------------------ */
/*  Data types supported by MyDB                                      */
/* ------------------------------------------------------------------ */
typedef enum {
    TYPE_INT      = 0,  /* 4 bytes, signed 32-bit integer */
    TYPE_DECIMAL  = 1,  /* 8 bytes, stored as int64 * 10^scale */
    TYPE_VARCHAR  = 2,  /* variable: 2-byte length prefix + up to 150 bytes */
    TYPE_ENUM     = 3,  /* 1 byte index into the column's enum list */
    TYPE_BOOL     = 4,  /* 1 byte: 0 = false, 1 = true */
    TYPE_DATE     = 5,  /* 4 bytes stored as YYYYMMDD integer */
    TYPE_DATETIME = 6   /* 8 bytes stored as YYYYMMDDHHmmSS integer */
} DataType;

/* ------------------------------------------------------------------ */
/*  Page types                                                        */
/* ------------------------------------------------------------------ */
typedef enum {
    PAGE_TYPE_DATA     = 0,  /* clustered leaf — [klen][key][vlen][val] */
    PAGE_TYPE_INTERNAL = 1,  /* internal B+ Tree node — [klen][key][child_page_no] */
    PAGE_TYPE_META     = 2,  /* page 0 of each file — file header */
    PAGE_TYPE_INDEX    = 3   /* secondary index leaf — [klen][key][page_no][slot_no] */
} PageType;

/* ------------------------------------------------------------------ */
/*  Record types (stored in the 3 low bits of heap_no field)          */
/* ------------------------------------------------------------------ */
typedef enum {
    REC_ORDINARY  = 0,  /* normal user row */
    REC_NODE_PTR  = 1,  /* B+ Tree internal node pointer record */
    REC_INFIMUM   = 2,  /* lower boundary pseudo-record on every page */
    REC_SUPREMUM  = 3   /* upper boundary pseudo-record on every page */
} RecordType;

/* ------------------------------------------------------------------ */
/*  WAL record types (MYDB_WAL_DESIGN.md §8.4 / MYDB_WAL_             */
/*  IMPLEMENTATION.md §8.5). System-wide, not wal/-owned: a change to  */
/*  what kinds of records exist is a change every WAL consumer (data   */
/*  pages, schema/catalog metadata, recovery) needs to agree on — same */
/*  reasoning as PageType/RecordType above, unlike WalRecordHeader     */
/*  itself, which is wal/'s own on-disk struct.                        */
/*  UPDATE = DELETE + INSERT: MyDB does delete-then-reinsert, never     */
/*  in-place row modification, so there is no WAL_REC_UPDATE.          */
/* ------------------------------------------------------------------ */
typedef enum {
    WAL_REC_BEGIN              = 1,  /* body: none */
    WAL_REC_COMMIT             = 2,  /* body: uint64_t commit_timestamp */
    WAL_REC_ABORT              = 3,  /* body: none */
    WAL_REC_INSERT             = 4,
    WAL_REC_DELETE             = 5,  /* carries full before-image */
    WAL_REC_PAGE_INIT          = 6,
    WAL_REC_PAGE_SPLIT         = 7,
    WAL_REC_PAGE_MERGE         = 8,  /* reserved, future */
    WAL_REC_PAGE_COMPACT       = 9,
    WAL_REC_FILE_HEADER_UPDATE = 10,
    WAL_REC_SCHEMA_UPDATE      = 11,
    WAL_REC_CATALOG_UPDATE     = 12,
    WAL_REC_FILE_CREATE        = 13,
    WAL_REC_FILE_DROP          = 14,
    WAL_REC_CHECKPOINT         = 15,
    WAL_REC_CLR                = 16
} WalRecType;

/* ------------------------------------------------------------------ */
/*  Record ID — uniquely identifies a record on disk                  */
/* ------------------------------------------------------------------ */
typedef struct {
    uint32_t page_no;   /* which page */
    uint16_t slot_no;   /* which slot in that page's directory */
} RID;

/* ------------------------------------------------------------------ */
/*  Value — tagged union holding any column value                     */
/*                                                                    */
/*  is_null is a separate flag so we can distinguish NULL from 0/""   */
/* ------------------------------------------------------------------ */
typedef struct {
    DataType type;
    uint8_t  is_null;
    union {
        int32_t  int_val;
        int64_t  decimal_val;   /* value * 10^scale, scale stored in ColumnDef */
        struct {
            uint16_t len;
            char     data[MAX_VARCHAR_LEN];
        } varchar_val;
        uint8_t  enum_val;      /* index into enum list */
        uint8_t  bool_val;
        int32_t  date_val;      /* YYYYMMDD */
        int64_t  datetime_val;  /* YYYYMMDDHHmmSS */
    } v;
} Value;

/* ------------------------------------------------------------------ */
/*  Forward declarations (full definitions in their own headers)      */
/* ------------------------------------------------------------------ */
typedef struct Row         Row;
typedef struct RelationDef RelationDef;
typedef struct Cursor      Cursor;
typedef struct ColumnDef   ColumnDef;

#endif /* COMMON_H */
