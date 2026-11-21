#ifndef DATABASE_FILE_H
#define DATABASE_FILE_H

#include "common.h"

/* ------------------------------------------------------------------ */
/*  __database.mydb — engine registry                                 */
/*                                                                    */
/*  One file per engine instance, 8 KB fixed. Holds the partition     */
/*  directory: up to MAX_PARTITIONS partitions, each mapping a        */
/*  numeric partition_id to a filesystem path and an owner user.      */
/*                                                                    */
/*  The file is opened once at engine startup, kept resident in RAM   */
/*  in this DatabaseFile struct, and written back via db_save() on    */
/*  every modification (each save fsyncs — no WAL in Phase 1).        */
/*                                                                    */
/*  Direct pread/pwrite I/O — does NOT go through the buffer pool.    */
/* ------------------------------------------------------------------ */

typedef struct {
    char     engine_name[32];   /* human-readable label */
    uint64_t created_at;        /* YYYYMMDDHHmmSS */
    uint64_t last_opened;       /* updated on every db_open */
    uint8_t  num_partitions;    /* count of active slots */
    uint32_t next_partition_id; /* monotonic counter; never reused */
} DatabaseHeader;

typedef struct {
    uint32_t partition_id;      /* unique, > 0 when active */
    char     path[256];         /* absolute path to partition directory */
    uint32_t owner_id;          /* FK -> system_schema.users.user_id */
    uint8_t  is_active;         /* 1 = slot occupied, 0 = empty */
} PartitionEntry;

typedef struct {
    DatabaseHeader header;
    PartitionEntry partitions[MAX_PARTITIONS];
    int            fd;          /* open fd; -1 when closed */
    char           path[256];   /* path to __database.mydb */
} DatabaseFile;

/* Create a new __database.mydb at `path`. Fails if the file already
 * exists. engine_name may be NULL (defaults to "MyDB Engine"). */
int db_create(const char *path, const char *engine_name, DatabaseFile *out);

/* Open an existing __database.mydb. Verifies magic, version, file
 * type and CRC-32C checksum. Updates last_opened and saves. */
int db_open(const char *path, DatabaseFile *out);

/* Close the file descriptor. Does not flush — call db_save first if
 * you have unsaved changes (every public mutator already saves). */
int db_close(DatabaseFile *db);

/* Pack the in-memory state into an 8 KB buffer, recompute the
 * trailer checksum, pwrite, and fsync. */
int db_save(DatabaseFile *db);

/* Register a new partition. partition_id is allocated from the
 * monotonic header counter `next_partition_id` and never reused, even
 * after db_remove_partition. Returns MYDB_ERR_FULL if all slots are
 * active. Persists immediately. */
int db_add_partition(DatabaseFile *db, uint32_t owner_id,
                     const char *path, uint32_t *out_partition_id);

/* Mark a partition slot inactive. Does NOT delete the partition's
 * directory — that is a higher-layer concern (DROP DATABASE). */
int db_remove_partition(DatabaseFile *db, uint32_t partition_id);

/* Linear-scan lookups. Return NULL if no active match. Pointer is
 * valid for the lifetime of *db. */
PartitionEntry *db_find_by_owner(DatabaseFile *db, uint32_t owner_id);
PartitionEntry *db_find_by_id   (DatabaseFile *db, uint32_t partition_id);

#endif /* DATABASE_FILE_H */
