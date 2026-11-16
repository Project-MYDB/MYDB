# MYDB Database Hierarchy: Design Specification

| | |
|---|---|
| **Document ID** | MYDB-DD-01 |
| **Version** | 3.0 |
| **On-disk format version** | 3 (`MYDB_FORMAT_VERSION`) |
| **Author** | Hasnat Akram, NUM-BSCS-2023-01 |
| **Date** | 2026-10-09 |
| **Supersedes** | *MYDB Storage Engine: Proposed Storage Hierarchy Design* (v2, `MYDB_Storage_Design_v2.docx`); *MYDB v3: Proposed Redesign* (`V3_DESIGN.md`) |
| **Related documents** | MYDB-DD-02 Storage Engine Design · MYDB-DD-03 Server Design · MYDB-DD-04 WAL Design · PartitionBuffer Design · Query Planner Design |

---

## 1. Introduction

### 1.1 Purpose

This document specifies the database hierarchy of the MYDB engine: how installations, partitions, schemas and tables are organised on disk, the binary format of every engine-managed file, the in-memory structures that hold that metadata at runtime, and the rules that govern access control, quota accounting and durability.

### 1.2 Scope

In scope:

- The four-level storage hierarchy (Engine → Partition → Schema → Relation) and its directory layout.
- The byte-level format of all metadata files and of the relation file header.
- The runtime ownership model (engine, connection, partition context, storage engine, caches).
- Lifecycle flows: bootstrap, startup, login, schema activation, DDL, user management, shutdown.
- Access control, quota accounting and the pre-WAL durability model.

Out of scope (specified elsewhere):

| Topic | Document |
|---|---|
| Relation file, page format, clustered and secondary B+ trees, buffer pool | MYDB-DD-02 Storage Engine Design |
| PartitionBuffer (Cache 1 + Cache 2) internals | PartitionBuffer Design |
| Write-ahead log format, recovery | MYDB-DD-04 WAL Design |
| Network protocol, sessions, daemon | MYDB-DD-03 Server Design |
| Statistics use by the optimizer | Query Planner Design |

### 1.3 Definitions

| Term | Definition |
|---|---|
| **Engine** | One MYDB installation rooted at a single directory (`root_dir`, e.g. `~/.mydb/`). |
| **Partition** | A per-user resource boundary: one directory, one owner, one storage quota. |
| **Schema** | A logical namespace inside a partition. The SQL keyword is `DATABASE` (`CREATE DATABASE`, `USE`). |
| **Relation** | A table. One relation file per table. |
| **RelationDef** | The complete in-memory definition of a relation (columns, keys, indexes, root pages). |
| **Owner** | The user whose `user_id` is recorded as a partition's `owner_id`. |
| **Analyst** | A user acting on a partition they do not own, through a SELECT grant. |
| **root** | The user created at bootstrap; always `user_id = 1`. |
| **Cache 1** | The partition catalog, held resident as PartitionBuffer slot PB[0]. |
| **Cache 2** | The LRU cache of open schemas, PartitionBuffer slots PB[1..8]. |
| **Metadata file** | Any engine-managed file other than a relation file. |

### 1.4 Conventions

- Offsets and sizes are in bytes. Ranges are inclusive (`8..11` is four bytes).
- **Byte order:** metadata files and the relation file header are little-endian. The data page header and record header inside relation files are big-endian.
- **Timestamps** are `uint64` values of the form `YYYYMMDDHHmmSS` in the server's local time.
- **Reserved** bytes are written as zero and ignored on read.
- **Identifiers** (`user_id`, `partition_id`, `schema_id`, `table_id`, `privilege_id`) are allocated from monotonic counters, start at 1, and are never reused. The value 0 means "none".
- Names are NUL-padded fixed-width byte arrays. A 32-byte name field holds at most 31 characters.

---

## 2. Design Overview

### 2.1 Design goals

1. **Isolation by partition.** Each user's data lives in its own directory with its own quota and runtime context, so per-user operations never touch another user's files.
2. **Separate I/O paths for metadata and data.** Metadata files are small, fixed-size and read whole with `pread`/`pwrite`; they never enter the buffer pool. The buffer pool caches only relation data and index pages.
3. **Self-describing files.** Every file begins with the same magic number and a file-type code, so any file can be identified and validated on open.
4. **Stable identities.** Partitions, schemas and tables carry persistent numeric IDs so that log records and recovery can refer to them independently of names and paths.
5. **Bounded memory.** All capacities are compile-time constants, so the memory footprint of an engine is known in advance.

### 2.2 Four-level hierarchy

| Level | Unit | Metadata file | Contents |
|---|---|---|---|
| Engine | Installation | `__database.mydb`, `system_schema/*` | Partition registry, users, grants, optimizer statistics |
| Partition | User resource boundary | `__catalog.mydb` | Quota, schema directory, ID counters |
| Schema | Namespace | `__schema.mydb` | Relation directory and relation definitions |
| Relation | Table | `<relation>.mydb` | Clustered B+ tree and secondary index B+ trees |

### 2.3 Directory layout

```
<root_dir>/                                 e.g. ~/.mydb/
├── __database.mydb                         engine registry               8 KB
├── system_schema/
│   ├── users.mydb                          user accounts                 8 KB
│   ├── privileges.mydb                     cross-partition grants       16 KB
│   └── stats/
│       └── stats_<partition_id>_<schema>.mydb   optimizer statistics  1040 KB
├── <root_username>/                        root's partition
│   ├── __catalog.mydb                      partition catalog             4 KB
│   ├── wal/ , fpi/                         write-ahead log (MYDB-DD-04)
│   └── <schema>/
│       ├── __schema.mydb                   schema definition          1040 KB
│       └── <relation>.mydb                 relation data            16 KB × n
└── <partition_name>/                       one directory per partition
    └── ...
```

A partition directory is named after its owner's username unless `CREATE USER` supplies a different partition name. The absolute path of each partition is recorded in `__database.mydb`; the engine never derives it from the username after creation.

### 2.4 File inventory

| File | Type code | Size | Integrity check | Owning module |
|---|---|---|---|---|
| `__database.mydb` | 1 `FILETYPE_DATABASE` | 8 192 B | CRC32 trailer | `engine/database_file.c` |
| `__catalog.mydb` | 2 `FILETYPE_CATALOG` | 4 096 B | CRC32 trailer | `partition_manager/partition.c` |
| `__schema.mydb` | 3 `FILETYPE_SCHEMA` | 65 × 16 KB | CRC32 on page 0; magic on definition pages | `partition_manager/schema_file.c` |
| `<relation>.mydb` | 4 `FILETYPE_RELATION` | (1 + n) × 16 KB | CRC32C per data page | `storage_engine/disk_manager.c` |
| `users.mydb` | 5 `FILETYPE_USERS` | 8 192 B | CRC32 trailer | `system_schema/system_schema.c` |
| `privileges.mydb` | 6 `FILETYPE_PRIVILEGES` | 16 384 B | CRC32 trailer | `system_schema/system_schema.c` |
| `stats_<pid>_<schema>.mydb` | 7 `FILETYPE_STATS` | 65 × 16 KB | CRC32 per page | `partition_manager/stats.c` |

Type codes 8–13 are WAL files and are specified in MYDB-DD-04.

---

## 3. Common File Conventions

### 3.1 File identifier

Every MYDB file begins with an 8-byte identifier (`FileHeaderId`):

| Offset | Size | Field | Value |
|---|---|---|---|
| 0 | 4 | `magic` | `0x4D594442` ("MYDB") |
| 4 | 2 | `version` | `MYDB_FORMAT_VERSION` (3) at write time |
| 6 | 2 | `file_type` | One of the type codes in §2.4 |

### 3.2 Validation on open

Every metadata file is validated in this order. The first failure is returned and the file is not used.

| Check | Failure code |
|---|---|
| `magic == MYDB_MAGIC` | `MYDB_ERR_BAD_MAGIC` (−10) |
| `version <= MYDB_FORMAT_VERSION` | `MYDB_ERR_BAD_VERSION` (−12) |
| `file_type == expected` | `MYDB_ERR_BAD_FILE_TYPE` (−11) |
| Stored checksum == computed checksum | `MYDB_ERR_BAD_CHECKSUM` (−13) |

A file written by an older format version is accepted. A file written by a newer version is rejected.

### 3.3 Checksums

| Scope | Algorithm | Coverage |
|---|---|---|
| Fixed-size metadata files (`__database`, `__catalog`, `__schema` page 0, `users`, `privileges`, stats pages) | CRC32 (IEEE 802.3, reflected polynomial `0xEDB88320`) | All bytes before the checksum field |
| Relation data and index pages | CRC32C (Castagnoli, hardware) | Page bytes `4..16375` (MYDB-DD-02 §4.8) |
| `__schema.mydb` definition pages, relation file page 0 | None | Identified by magic only |

Format version 3 is defined by the move of every metadata checksum from FNV-1a to CRC32.

### 3.4 Metadata I/O model

- A metadata file is read whole on open and held in a fixed-size in-memory mirror.
- Changes are applied to the mirror and written back by re-packing the whole file (or the whole page 0, for `__schema.mydb`). A new checksum is computed on every write.
- Serialisation uses explicit byte offsets (`memcpy` per field), never raw structure dumps. The on-disk layout is therefore independent of compiler padding. The one exception is `ColumnStats` (§7.3), which is copied as a raw 64-byte record.
- Files are created with `O_CREAT | O_EXCL` and pre-sized with `ftruncate`. Creating a file that already exists fails.

---

## 4. Engine-Level Files

### 4.1 `__database.mydb`: engine registry

One per engine, 8 KB. It maps partition IDs to directories and owners. It is opened at startup, held resident in `EngineState.database`, and rewritten and `fsync`ed on every change. Each successful open also refreshes `last_opened`.

**Layout**

| Offset | Size | Field | Description |
|---|---|---|---|
| 0..7 | 8 | `FileHeaderId` | type 1 |
| 8..39 | 32 | `engine_name` | Label; defaults to "MyDB Engine" |
| 40..47 | 8 | `created_at` | Engine creation time |
| 48..55 | 8 | `last_opened` | Last successful open |
| 56 | 1 | `num_partitions` | Count of active entries |
| 57..59 | 3 | reserved | |
| 60..63 | 4 | `next_partition_id` | Next ID to allocate (starts at 1) |
| 64..127 | 64 | reserved | Header growth |
| 128..4607 | 4 480 | `partitions[16]` | 16 × `PartitionEntry` (280 B) |
| 4608..8187 | 3 580 | reserved | |
| 8188..8191 | 4 | checksum | CRC32 over 0..8187 |

**PartitionEntry (280 B).** Slot *N* is at offset `128 + N × 280`.

| Offset | Size | Field | Description |
|---|---|---|---|
| 0..3 | 4 | `partition_id` | Unique; never reused |
| 4..259 | 256 | `path` | Absolute path of the partition directory |
| 260..263 | 4 | `owner_id` | References `users.user_id` |
| 264 | 1 | `is_active` | 1 = slot in use |
| 265..279 | 15 | reserved | |

Removing a partition zeroes its slot. Lookups by `partition_id` and by `owner_id` are linear scans over the 16 slots.

### 4.2 `system_schema/users.mydb`: user accounts

One per engine, 8 KB, 32 user slots. It is the only source of credentials. It is loaded at startup, before any login is accepted, and rewritten and `fsync`ed on every change.

**Layout**

| Offset | Size | Field |
|---|---|---|
| 0..7 | 8 | `FileHeaderId` (type 5) |
| 8..11 | 4 | `next_user_id` |
| 12 | 1 | `num_users` |
| 13..63 | 51 | reserved |
| 64..7743 | 7 680 | `slots[32]` × 240 B |
| 7744..8187 | 444 | reserved |
| 8188..8191 | 4 | CRC32 over 0..8187 |

**UserSlot (240 B)**

| Offset | Size | Field | Description |
|---|---|---|---|
| 0..3 | 4 | `user_id` | |
| 4..35 | 32 | `username` | Unique |
| 36..67 | 32 | `password_hash` | Raw SHA-256 digest |
| 68..83 | 16 | `password_salt` | Raw random bytes |
| 84 | 1 | `hash_algorithm` | 1 = SHA-256; 2 reserved for Argon2id |
| 85 | 1 | `is_active` | 0 = login refused |
| 86 | 1 | `is_valid` | 1 = slot in use |
| 87 | 1 | padding | |
| 88..95 | 8 | `created_at` | |
| 96..103 | 8 | `last_login` | 0 = never |
| 104..239 | 136 | reserved | |

**Credential scheme.** `password_hash = SHA-256(salt || password)`, where the salt comes from `/dev/urandom`. The server never transmits the stored hash. Network login uses a challenge–response: the client proves knowledge of the stored hash by returning `SHA-256(nonce || password_hash)` for a server-issued 32-byte nonce (MYDB-DD-03).

**In-memory index.** An open-addressing hash table of 64 buckets maps usernames to slot indexes. It is keyed by FNV-1a of the username and uses linear probing, with load factor ≤ 0.5. Lookup by `user_id` is a linear scan.

### 4.3 `system_schema/privileges.mydb`: cross-partition grants

One per engine, 16 KB, 256 grant slots. Each grant gives one user SELECT access to one schema in another user's partition.

**Layout**

| Offset | Size | Field |
|---|---|---|
| 0..7 | 8 | `FileHeaderId` (type 6) |
| 8..11 | 4 | `next_privilege_id` |
| 12..13 | 2 | `num_privileges` |
| 14..63 | 50 | reserved |
| 64..15423 | 15 360 | `slots[256]` × 60 B |
| 15424..16379 | 956 | reserved |
| 16380..16383 | 4 | CRC32 over 0..16379 |

**PrivilegeSlot (60 B)**

| Offset | Size | Field | Description |
|---|---|---|---|
| 0..3 | 4 | `privilege_id` | |
| 4..7 | 4 | `grantee_id` | References `users.user_id` |
| 8..11 | 4 | `partition_id` | Partition holding the schema |
| 12..15 | 4 | `granted_by` | References `users.user_id` |
| 16..23 | 8 | `granted_at` | |
| 24..55 | 32 | `schema_name` | |
| 56 | 1 | `is_valid` | |
| 57..59 | 3 | reserved | |

The composite key `(grantee_id, partition_id, schema_name)` is unique. It is indexed in memory by a 512-bucket open-addressing hash table.

### 4.4 `system_schema/stats/`: optimizer statistics

The engine stores statistics in one file per schema, named `stats_<partition_id>_<schema_name>.mydb`. Because `partition_id` is never reused, the name is unique across the installation. The files sit at engine level rather than inside partitions for three reasons:

- they are read-mostly system metadata, written only by `ANALYZE TABLE`;
- the planner belongs to the execution engine and must not reach into partition-private caches;
- the engine can hold every handle in one flat pool (§9.6).

The file format is specified in §7.

---

## 5. Partition-Level File: `__catalog.mydb`

One per partition, 4 KB, at `<partition>/__catalog.mydb`. It holds the partition's identity, quota state, schema directory and persistent ID counters. User identity and grants are not stored here. At runtime the catalog is PartitionBuffer slot PB[0], resident for as long as the partition is loaded.

**Layout**

| Offset | Size | Field | Description |
|---|---|---|---|
| 0..7 | 8 | `FileHeaderId` | type 2 |
| 8..11 | 4 | `partition_id` | |
| 12..15 | 4 | `owner_id` | References `users.user_id` |
| 16..23 | 8 | `created_at` | |
| 24..31 | 8 | `last_modified` | Refreshed on every save |
| 32..39 | 8 | `quota_bytes` | Storage limit |
| 40..47 | 8 | `used_bytes` | Authoritative storage consumption (§11) |
| 48 | 1 | `num_schemas` | Count of valid schema slots |
| 49..52 | 4 | `next_table_id` | Per-partition table ID counter (starts at 1) |
| 53..56 | 4 | `next_schema_id` | Per-partition schema ID counter (starts at 1) |
| 57..127 | 71 | reserved | |
| 128..2687 | 2 560 | `schemas[64]` | 64 × `SchemaEntry` (40 B) |
| 2688..4087 | 1 400 | reserved | |
| 4088..4091 | 4 | checksum | CRC32 over 0..4087 |
| 4092..4095 | 4 | reserved | |

**SchemaEntry (40 B)**

| Offset | Size | Field | Description |
|---|---|---|---|
| 0..31 | 32 | `schema_name` | Unique within the partition |
| 32 | 1 | `num_relations` | Count of relations in the schema |
| 33 | 1 | `is_valid` | 1 = slot in use |
| 34..37 | 4 | `schema_id` | Mirrors `SchemaHeader.schema_id` |
| 38..39 | 2 | reserved | |

**ID allocation.** `table_id` and `schema_id` are allocated per partition. The counter is advanced and the catalog is saved *before* the ID is stamped into any other file, so a crash can skip an ID but can never issue the same ID twice.

---

## 6. Schema-Level File: `__schema.mydb`

One per schema, at `<partition>/<schema>/__schema.mydb`. The file has a fixed size of 65 pages of 16 KB each (1 040 KB):

| Page | Contents |
|---|---|
| 0 | `SchemaHeader` and the relation directory (64 × `RelationEntry`) |
| 1..64 | One serialised `RelationDef` per page, assigned when the relation is created |

### 6.1 Page 0

| Offset | Size | Field | Description |
|---|---|---|---|
| 0..7 | 8 | `FileHeaderId` | type 3 |
| 8..11 | 4 | `partition_id` | Owning partition |
| 12..15 | 4 | `schema_id` | Persistent schema identity |
| 16..47 | 32 | `schema_name` | |
| 48..55 | 8 | `created_at` | |
| 56..63 | 8 | `last_modified` | Refreshed on every page 0 save |
| 64..71 | 8 | `size_bytes` | Always written as 0; recomputed on load |
| 72 | 1 | `num_relations` | Count of valid directory entries |
| 73..127 | 55 | reserved | |
| 128..3711 | 3 584 | `relations[64]` | 64 × `RelationEntry` (56 B) |
| 3712..16375 | 12 664 | reserved | |
| 16376..16379 | 4 | checksum | CRC32 over 0..16375 |
| 16380..16383 | 4 | reserved | |

`size_bytes` is derived, not stored: on load it is computed as the sum of `num_pages × PAGE_SIZE` over valid entries.

### 6.2 RelationEntry (56 B)

The directory is the lightweight index of a schema's relations. It answers existence, listing, size and planner-cost questions without loading any definition page.

| Offset | Size | Field | Description |
|---|---|---|---|
| 0..31 | 32 | `relation_name` | Unique within the schema |
| 32 | 1 | `is_valid` | 1 = slot in use |
| 33 | 1 | `page_no` | Definition page (1..64) |
| 34 | 1 | `num_columns` | |
| 35..38 | 4 | `num_rows` | Maintained on every INSERT (+1) and DELETE (−1) |
| 39..42 | 4 | `num_pages` | Data and index pages; source of truth for relation size |
| 43 | 1 | `tree_height` | Clustered B+ tree height (1 = root only) |
| 44 | 1 | reserved | |
| 45..48 | 4 | `table_id` | Mirrors the relation file's `table_id` |
| 49..55 | 7 | reserved | |

New relations take the lowest free directory slot and the lowest free definition page. Directory slot index and definition page number are independent. Dropping a relation zeroes its definition page and its directory slot.

### 6.3 Definition pages (pages 1..64)

Each definition page holds one `RelationDef`, serialised sequentially in little-endian order. The page has no checksum; it is identified by its magic number. Unused bytes are zero.

**Fixed prefix (124 B)**

| Offset | Size | Field |
|---|---|---|
| 0..3 | 4 | page magic `0x53434D41` ("SCMA") |
| 4..67 | 64 | `relation_name` |
| 68 | 1 | `num_columns` |
| 69 | 1 | `pk_col_idx` |
| 70 | 1 | `num_foreign_keys` |
| 71 | 1 | `num_secondary_indexes` |
| 72..75 | 4 | `auto_incr_counter`: next AUTO_INCREMENT value |
| 76..79 | 4 | `root_page_no`: clustered B+ tree root |
| 80..83 | 4 | `table_id` |
| 84..91 | 8 | `secondary_col_idx[8]` |
| 92..123 | 32 | `secondary_root_page_no[8]` |

**Column records** follow from offset 124. Each column takes 223 bytes plus 32 bytes per ENUM value.

| Size | Field |
|---|---|
| 64 | `name` |
| 1 | `type` (`DataType`) |
| 2 | `max_len` (VARCHAR length; DECIMAL precision) |
| 1 | `scale` (DECIMAL) |
| 1 | flags: bit 0 NOT NULL · bit 1 PRIMARY KEY · bit 2 UNIQUE · bit 3 AUTO_INCREMENT · bit 4 has DEFAULT |
| 1 | default-is-NULL |
| 152 | default value blob, encoded by type (VARCHAR: 2-byte length + data) |
| 1 | `num_enum_values` |
| 32 × n | ENUM value strings |

**Foreign-key records** follow the columns, 257 bytes each:

| Size | Field |
|---|---|
| 64 | `constraint_name` |
| 64 | `column_name` |
| 64 | `ref_relation_name` |
| 64 | `ref_column_name` |
| 1 | `on_delete_action` (0 RESTRICT · 1 CASCADE · 2 SET NULL) |

The whole definition must fit in one 16 KB page. Serialisation fails, and CREATE TABLE is rejected, if it does not.

**In-memory only.** `RelationDef.owner_schema` is not serialised. It is stamped from `SchemaHeader.schema_name` whenever a definition is loaded or created. The storage engine uses it to build file paths and to key its open-table cache.

**Runtime caching.** Page 0 is resident while the schema is in Cache 2. Definition pages are loaded lazily into a 32-frame inner cache per schema, with pinning and LRU eviction (PartitionBuffer Design).

---

## 7. Statistics File Format

Each statistics file is 65 pages of 16 KB. Page 0 is the header. Page *s* + 1 holds the statistics for the relation in directory slot *s* of the matching `__schema.mydb`. The file is created on first access with all relation pages zeroed. A relation page is written only by `ANALYZE TABLE`.

### 7.1 Page 0

| Offset | Size | Field |
|---|---|---|
| 0..7 | 8 | `FileHeaderId` (type 7) |
| 8..39 | 32 | `schema_name` |
| 40..43 | 4 | CRC32 over 0..39 |

### 7.2 Relation statistics page

| Offset | Size | Field |
|---|---|---|
| 0..7 | 8 | `FileHeaderId` |
| 8..11 | 4 | `slot_idx` |
| 12..15 | 4 | `blob_used` (bytes used in the blob pool) |
| 16..2063 | 2 048 | `ColumnStats[32]` × 64 B |
| 2064..2067 | 4 | CRC32 over 0..2063 |
| 2068..2071 | 4 | padding (aligns the blob pool to 8 B) |
| 2072..16383 | 14 312 | blob pool: MCV entries and histogram buckets |

### 7.3 ColumnStats (64 B)

| Offset | Size | Field |
|---|---|---|
| 0 | 1 | `has_stats` |
| 1 | 1 | `stats_type` (0 none · 1 MCV · 2 histogram) |
| 2..3 | 2 | `blob_count` |
| 4..7 | 4 | `num_distinct` |
| 8..11 | 4 | `num_nulls` |
| 12..15 | 4 | `total_rows` |
| 16..23 | 8 | `min_numeric` |
| 24..31 | 8 | `max_numeric` |
| 32..35 | 4 | `blob_offset` |
| 36..63 | 28 | padding |

Blob entries are 16 bytes each: an MCV entry is `{int64 value, uint32 frequency, pad}` and a histogram bucket is `{int64 upper_bound, uint32 row_count, pad}`. A column holds at most 16 entries. INT, DECIMAL, DATE and DATETIME values are encoded as `int64`. VARCHAR, BOOL and ENUM have no numeric range.

---

## 8. Relation File: `<relation>.mydb`

Each table is stored in one file, `<partition>/<schema>/<relation>.mydb` (type 4). The file holds the table's clustered B+ tree and all of its secondary indexes. Page 0 is a file header carrying `num_pages`, the persistent `table_id` and an AUTO_INCREMENT flag. Pages 1..n are slotted B+ tree pages accessed through the partition's buffer pool. Root page numbers and the auto-increment counter are not stored in the file; they live in the table's `RelationDef` (§6.3).

The file layout, page format, key and row encoding, index structures and buffer pool are specified in **MYDB-DD-02 Storage Engine Design**.

---

## 9. Runtime Model

### 9.1 Ownership hierarchy

```
EngineState                                    one per process
├── DatabaseFile          __database.mydb      resident
├── SystemSchema          users + privileges   resident, hash-indexed
├── StatsBuffer           system_schema/stats  lazy handle pool (80)
├── ConnectionPool        32 Connection slots  one per session
└── PartitionCtx*[16]     loaded partitions    lazy, reference-counted
    └── PartitionCtx                           one per active partition
        ├── PartitionBuffer
        │   ├── PB[0]      Catalog             __catalog.mydb, never evicted
        │   └── PB[1..8]   schema slots        LRU of open __schema.mydb
        │       └── inner[32]                  LRU of RelationDef pages
        ├── TransactionManager                 transaction id and state
        └── StorageEngine
            ├── BufferPool     4 096 frames × 16 KB (64 MB)
            └── OpenTable[64]  DiskManager + BTree handles per open relation
```

### 9.2 Engine (`EngineState`)

The engine owns everything that is global to the installation: the registry, users and grants, statistics handles, sessions, and the table of loaded partitions. It is responsible for authentication, routing each connection to its partition, and running the SQL pipeline: parse → build `ExecContext` → execute → free. It holds `PartitionCtx` pointers but never accesses their internals; all partition work goes through `pctx_*` and `pm_*` calls.

### 9.3 Connection

A `Connection` holds per-session state: `user_id`, `username`, `partition_id`, `logged_in`, `partition_open` (1 when the user owns the partition it is attached to) and the current schema name. The connection, not the partition, owns its current schema. Before each statement the engine projects the connection's schema onto the shared partition context. As a result, connections on the same partition can use different schemas.

Execution is single-threaded. The server runs one statement at a time, and the engine identifies the in-flight connection through `EngineState.active_conn`.

### 9.4 Partition context (`PartitionCtx`)

A `PartitionCtx` holds all runtime state for one partition and is shared by every connection attached to it. Its lifetime is controlled by a reference count, `n_refs`, which the engine maintains:

- **Acquire:** on login by the owner, or on USE by an analyst. The engine loads the context on first use (`pctx_init` then `pctx_open_catalog`) and increments `n_refs` for each further connection.
- **Release:** on logout, or when an analyst switches to another partition. At `n_refs = 0` the engine closes the context, which flushes dirty pages and metadata and closes every handle, and frees it.

A partition context does not track which connections use it.

### 9.5 Storage engine (`StorageEngine`)

There is one storage engine per partition and no global singleton. It provides the buffer pool, the open-table cache, B+ tree operations and file-path construction. Paths are built as `<partition_path>/<rel.owner_schema>/<relation>.mydb`. The storage engine holds no schema, catalog, quota, constraint or transaction state; those belong to the partition manager (`pm_*`), which wraps every storage call.

### 9.6 Statistics buffer (`StatsBuffer`)

The statistics buffer is an engine-owned pool of up to 80 `StatsFile` handles keyed by `(partition_id, schema_name)`. A handle is opened, or the file created, on the first statement that runs with that schema active. The engine passes the handle to the executor in `ExecContext.stats`: the planner reads from it and `ANALYZE TABLE` writes through it. DROP DATABASE removes the schema's file. DROP USER removes every file for the partition.

### 9.7 Execution context (`ExecContext`)

The engine builds one execution context per statement:

| Field | Purpose |
|---|---|
| `engine` | Global metadata: registry, users, grants, statistics pool |
| `partition` | The connection's `PartitionCtx`; NULL for an analyst before USE |
| `conn` | The session (authentication state, current schema) |
| `stats` | Statistics handle for the active schema; NULL if no schema is active |

### 9.8 Memory budget

| Component | Per partition |
|---|---|
| `StorageEngine` (buffer pool 4 096 × 16 KB, plus 64 open-table handles) | ≈ 64.2 MB |
| `PartitionBuffer` (8 schema slots × 32 cached `RelationDef` of ≈ 26 KB each, plus catalog) | ≈ 6.5 MB |
| **Address space reserved per loaded partition** | **≈ 71 MB** |

These figures are reserved address space, not resident memory. Buffer-pool page frames and cached `RelationDef` frames are not cleared at initialisation, so physical memory is committed only as pages and definitions are loaded (MYDB-DD-02 §9.2). Sixteen open, idle partitions occupy about 3.8 MB in total. Partitions that are not loaded use no memory. The engine-level mirrors (registry, users, privileges) are each under 100 KB. Each open statistics handle takes about 1 MB.

---

## 10. Lifecycle Flows

### 10.1 Bootstrap (first run)

`engine_bootstrap(root_dir, root_username, root_password)` runs these steps:

1. Refuse to continue if `__database.mydb` already exists.
2. Create `root_dir/`, `system_schema/` and `system_schema/stats/`.
3. Create `__database.mydb`, `users.mydb` and `privileges.mydb`.
4. Insert the root user (`user_id = 1`) with a random salt and a SHA-256 hash.
5. Create `<root_dir>/<root_username>/`, register it as partition 1, owned by root.
6. Create its `__catalog.mydb` with the default quota (1 GB).

### 10.2 Startup

`engine_init` opens and validates `__database.mydb`, then `users.mydb` and `privileges.mydb` (building their hash indexes). It then initialises the statistics buffer. No partition is loaded and no login is accepted until these steps succeed.

### 10.3 Login

1. Look up the username in the users hash index. If the account is inactive, return `MYDB_ERR_PERM`.
2. Verify the credential: plaintext `SHA-256(salt || password)` locally, or challenge–response over the network.
3. Claim a connection slot.
4. If the user owns a partition (`db_find_by_owner`), acquire its `PartitionCtx` and set `partition_open = 1`. Otherwise the user is an analyst with no partition attached.
5. Stamp `last_login` and persist `users.mydb`.

### 10.4 USE `<schema>`

- **Owner:** the schema must exist in the partition's catalog. The schema file is opened into Cache 2, or bumped if already cached, and becomes the connection's current schema.
- **Analyst:** the engine finds a grant whose grantee is the user and whose `schema_name` matches. It resolves the grant's partition through `__database.mydb`, releases any previously attached partition, acquires the target partition, then opens the schema as above.

Switching schemas never closes the previous schema. It stays in Cache 2 until LRU pressure, DROP DATABASE, or partition close evicts it.

### 10.5 Statement execution

For each statement, the engine first resolves the connection and builds the `ExecContext`. It then re-applies the connection's current schema to the partition (a cache bump in the common case) and executes. Finally it checks that every `RelationDef` pinned during the statement has been released.

### 10.6 Schema and relation DDL

| Operation | Sequence |
|---|---|
| CREATE DATABASE | Owner check → allocate `schema_id` (catalog saved) → `mkdir` schema directory → create `__schema.mydb` → add `SchemaEntry` (catalog saved) |
| DROP DATABASE | Reject if it is the current schema → evict from Cache 2 → unlink every relation file → unlink `__schema.mydb` → `rmdir` → credit freed bytes to quota → remove `SchemaEntry` → remove statistics file |
| CREATE TABLE | Quota pre-check (1 + secondary indexes pages) → allocate `table_id` (catalog saved) → create relation file and root pages → write definition page and directory entry → charge pages to quota, increment `num_relations` → flush schema page 0 and catalog |
| DROP TABLE | Credit `num_pages` to quota → close handle and unlink file → zero definition page and directory slot → invalidate cached definition → decrement `num_relations` → flush catalog |
| CREATE INDEX | Quota pre-check (1 page) → allocate secondary root and backfill → update definition → flush |

DDL is not part of a user transaction. Its metadata is flushed before the statement returns.

### 10.7 User management (root only)

| Operation | Effect |
|---|---|
| CREATE USER | Insert user → `mkdir` partition → register partition in `__database.mydb` → create `__catalog.mydb` with the requested quota (default 1 GB, allowed range 100 MB–5 GB). Each failed step undoes the earlier ones. |
| DROP USER | Not permitted for root. Remove every schema's relation files and `__schema.mydb`, the catalog and the partition directory → remove the partition's statistics files → remove the partition entry → delete the user. |
| ALTER USER … PASSWORD | Recompute the hash with the user's existing salt. |
| ALTER USER … QUOTA | New quota must be within 100 MB–5 GB and ≥ current `used_bytes`. |

### 10.8 Logout and shutdown

Logout rolls back any open transaction on the connection's partition, frees the connection slot and releases the partition reference. Engine shutdown closes every loaded partition (flushing data pages and dirty metadata), closes statistics handles, then closes the system schema and registry.

---

## 11. Access Control

### 11.1 Rules

| Operation | Owner (own partition) | Analyst (other partition) | root |
|---|---|---|---|
| SELECT | Yes | Yes, with a matching grant | As owner of its own partition |
| INSERT / UPDATE / DELETE | Yes | No | As owner |
| CREATE / DROP TABLE, CREATE INDEX | Yes | No | As owner |
| CREATE / DROP DATABASE | Yes | No | As owner |
| CREATE / DROP / ALTER USER | No | No | Yes |
| SHOW GRANTS for another user | No | No | Yes |

### 11.2 Enforcement points

- **DML and queries:** `engine_check_access(write_required)` runs before every SELECT, INSERT, UPDATE and DELETE. An owner always passes. An analyst passes only reads, and only while a grant matching `(user_id, partition_id, schema_name)` exists.
- **Schema DDL:** requires the partition's catalog to be attached as owner.
- **User management:** requires `user_id == 1`.

A user may own at most one partition. A user without a partition can log in and works only through grants.

---

## 12. Quota Accounting

### 12.1 What is counted

`__catalog.mydb.used_bytes` is the partition's single authoritative usage counter. It counts the **data and index pages** of every relation: `sum(RelationEntry.num_pages) × 16 KB`. It does not count:

- page 0 of each relation file;
- `__catalog.mydb` and `__schema.mydb`;
- statistics files;
- WAL files.

`RelationEntry.num_pages` is the per-relation source of truth. Schema size is derived from it and never stored.

### 12.2 Enforcement

Quota is enforced in the partition manager, around storage calls, in two steps:

1. **Pre-check.** Before an operation that may allocate pages, the engine requires `quota_bytes − used_bytes ≥ k × 16 KB`, otherwise it returns `MYDB_ERR_FULL`. The headroom *k* depends on the operation:

   | Operation | *k* |
   |---|---|
   | CREATE TABLE | 1 + number of secondary indexes |
   | CREATE INDEX | 1 |
   | INSERT, UPDATE | 4 (allows for B+ tree splits) |

2. **Reconciliation.** After the operation, the engine compares the relation's page count before and after. It applies the difference to both `RelationEntry.num_pages` and `used_bytes`. A failed operation is reconciled the same way.

Within a transaction, these counter changes are made in memory and marked dirty. They are written at COMMIT and discarded at ROLLBACK (§13.2), so a rolled-back transaction leaves usage unchanged on disk.

---

## 13. Durability Model

### 13.1 Write policy per file

| File | When written | `fsync` |
|---|---|---|
| `__database.mydb`, `users.mydb`, `privileges.mydb` | Immediately on every change | Yes, every write |
| `__catalog.mydb` | Immediately for DDL and ID allocation; at COMMIT for quota changes | Yes, every save |
| `__schema.mydb` page 0 | Immediately for DDL; at COMMIT for row and page counters | At COMMIT (active schema) |
| `__schema.mydb` definition page | Immediately on CREATE TABLE; at COMMIT for `auto_incr_counter` and root-page changes | At COMMIT (active schema) |
| Relation data pages | On COMMIT, buffer-pool eviction, schema eviction or shutdown | No |
| Statistics pages | On `ANALYZE TABLE` | No |

### 13.2 Transactions (pre-WAL)

Each partition has one `TransactionManager`. Statements outside an explicit `BEGIN` run in auto-commit mode: each DML statement is its own transaction.

- **BEGIN** assigns a new transaction ID. It is written into `DB_TRX_ID` of every row the transaction writes.
- **COMMIT** writes back every dirty buffer-pool page, the active schema's dirty page 0 and definition pages, and the dirty catalog, then `fsync`s the schema file.
- **ROLLBACK** evicts every buffer-pool page without writing it, re-reads the active schema's page 0 and the catalog from disk, and drops dirty cached definitions. The on-disk state from before the transaction therefore becomes current again.

This model relies on the buffer pool holding every page a transaction dirties until COMMIT; a dirty page written out early to free a frame is not undone by ROLLBACK (MYDB-DD-02 §9.9). The write-ahead log (MYDB-DD-04) replaces it with log-based durability and crash recovery. The partition-scoped layout lets each partition keep its own log and recover independently.

---

## 14. Capacity Limits

| Limit | Value | Constant |
|---|---|---|
| Partitions per engine | 16 | `MAX_PARTITIONS` |
| Users per engine | 32 | `USERS_MAX_SLOTS` |
| Grants per engine | 256 | `PRIVILEGES_MAX_SLOTS` |
| Schemas per partition | 64 | `MAX_SCHEMAS_PER_PARTITION` |
| Relations per schema | 64 | `MAX_RELATIONS_PER_SCHEMA` |
| Schemas cached per partition | 8 | `PARTITION_BUFFER_SLOTS` |
| Relation definitions cached per schema | 32 | `PB_INNER_SLOTS` |
| Concurrent connections | 32 | `MAX_CONNECTIONS` |
| Open statistics handles | 80 | `MAX_STATS_HANDLES` |
| Columns per relation | 32 | `MAX_COLUMNS` |
| Foreign keys per relation | 8 | `MAX_FOREIGN_KEYS` |
| Secondary (UNIQUE) indexes per relation | 8 | `MAX_SECONDARY_IDX` |
| Username / schema name / relation name | 31 characters | 32-byte fields |
| Column name | 63 characters | `MAX_COLUMN_NAME` |
| VARCHAR length | 150 | `MAX_VARCHAR_LEN` |
| ENUM values per column | 16 × 31 characters | `MAX_ENUM_VALUES` |
| Partition path | 255 characters | 256-byte field |
| Quota per partition | 100 MB – 5 GB (default 1 GB) | `ENGINE_*_QUOTA_BYTES` |

---

## 15. Design Rationale

| Decision | Rationale |
|---|---|
| Partition as the unit of isolation | Quota, ownership, runtime context and (with WAL) recovery all fall on one boundary, so one user's workload or failure stays inside that boundary. |
| Users and grants at engine level | Credentials must be readable before any partition is opened. Storing them in a partition would make opening a partition depend on itself. Users without partitions are possible. |
| Metadata outside the buffer pool | Small fixed-size files are read whole with one system call. Keeping them out of the buffer pool means metadata cannot evict data pages, and each I/O path uses exactly one mechanism. |
| One magic number plus a file-type code | Same convention as the page-level `page_type`. One validation routine covers every file. |
| Persistent numeric IDs | Log records and recovery can identify tables and schemas after renames and without path lookups. A relation file can identify itself through `table_id`. |
| Root pages only in `RelationDef` | Keeping a single copy means no second copy on disk can diverge from it. |
| Statistics at engine level | Keeps the planner out of partition-private caches. Statistics are read-mostly system metadata, as in PostgreSQL's `pg_statistic`. |
| Schema cache with lazily loaded definitions | Switching schemas does not close or reopen files. Memory per schema is bounded by 32 cached definitions rather than all 64. |
| Write-back of counters with discard on rollback | Row counts, page counts and quota usage stay consistent with committed data. |

---

## 16. Known Limitations

| # | Limitation | Consequence |
|---|---|---|
| L1 | Changes that span several files are not atomic before WAL. CREATE TABLE writes the catalog, `__schema.mydb` and a new relation file in sequence. | A crash between steps can leave an orphaned file, a skipped ID or counters that disagree. Full recovery requires the WAL. |
| L2 | No startup reconciliation of `used_bytes` against the sum of `num_pages`. | Drift from a crash persists until corrected manually. |
| L3 | `__schema.mydb` is pre-allocated at 1 040 KB. | An empty schema uses about 1 MB of disk. |
| L4 | Definition pages and relation file page 0 have no checksum. | Corruption there is detected only through the magic number or later decoding errors. |
| L5 | Single-iteration salted SHA-256. | Fast to brute-force if `users.mydb` leaks. `hash_algorithm` = 2 is reserved for Argon2id. |
| L6 | Relation names are limited to 31 characters by the directory entry, although `RelationDef` has room for 63. | Longer names are rejected. |
| L7 | An analyst's USE resolves the first grant whose `schema_name` matches. | If one user holds grants to same-named schemas in two partitions, they cannot choose between them. |
| L8 | No SQL statement creates or revokes grants (`SHOW GRANTS` only). | Grants can only be managed through the system-schema API. |
| L9 | A user owns at most one partition, and an engine has at most 16. | Engine capacity is bounded by `MAX_PARTITIONS`. |
| L10 | Statistics pages are keyed by directory slot index. | A dropped relation's statistics remain in its slot until `ANALYZE TABLE` runs on the relation that reuses the slot. |

---

## Appendix A: Changes from Superseded Documents

| Area | Storage Design v2 / V3 proposal | Current design (this document) |
|---|---|---|
| Format version | 1 | 3 |
| Checksums | FNV-1a everywhere | CRC32 for metadata files; CRC32C for data pages |
| Metadata header size | 64-byte fixed headers | 128-byte headers with reserved growth area (`__database`, `__catalog`, `__schema` page 0) |
| `__database.mydb` | 16 entries at offset 64 | 16 entries at offset 128. Adds `next_partition_id`. |
| `__catalog.mydb` | 71 schema slots | 64 schema slots. Adds `next_table_id`, `next_schema_id`; `SchemaEntry` gains `schema_id`. |
| `SchemaHeader` | No schema identity | Adds `schema_id` |
| `RelationEntry` | `avg_row_size`, no identity | `tree_height` and `table_id` instead of `avg_row_size` |
| Relation file header | Root page number stored | `table_id` and `flags` stored. Root pages only in `RelationDef`. |
| Users / privileges | Specified as relations | Fixed-slot files (32 users, 256 grants) with in-memory hash indexes |
| Statistics | Inside `RelationEntry`; later `<schema>/__stats.mydb` | Separate MCV/histogram files under `system_schema/stats/` |
| Quota enforcement | Inside `disk_alloc_page` with a write on every page | Pre-check plus reconciliation in the partition manager. Written at COMMIT and discarded at ROLLBACK. |
| Cache 1 / Cache 2 | Standalone catalog and a single schema slot | Unified PartitionBuffer: PB[0] catalog, PB[1..8] LRU schemas, 32-frame definition cache each |
| Storage engine | `static` global singleton | One per `PartitionCtx` |
| Active schema | Engine-wide | Per connection, applied to the partition per statement |
| Partition lifetime | Single active partition | Up to 16 loaded, reference-counted by attached connections |
| Buffer pool | 64 frames (1 MB) | 4 096 frames (64 MB) per partition |
| Authentication | SHA-256 hash | SHA-256(salt || password); network challenge–response with nonce |
