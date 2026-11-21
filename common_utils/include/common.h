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
#define FILETYPE_DATABASE    1   /* __database.mydb        — engine registry */
#define FILETYPE_CATALOG     2   /* __catalog.mydb         — partition catalog */
#define FILETYPE_SCHEMA      3   /* __schema.mydb          — schema definition */
#define FILETYPE_RELATION    4   /* <relation>.mydb        — B+ tree data file */
#define FILETYPE_USERS       5   /* system_schema/users.mydb */
#define FILETYPE_PRIVILEGES  6   /* system_schema/privileges.mydb */
#define FILETYPE_STATS       7   /* __stats.mydb           — CBO optimizer stats */

/* ------------------------------------------------------------------ */
/*  On-disk format version. Bumped when any file layout changes in    */
/*  a way that older binaries can't read.                             */
/* ------------------------------------------------------------------ */
#define MYDB_FORMAT_VERSION  2

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
