# MYDB Storage Engine: Design Specification

| | |
|---|---|
| **Document ID** | MYDB-DD-02 |
| **Version** | 1.0 |
| **Author** | Hasnat Akram, NUM-BSCS-2023-01 |
| **Date** | 2026-10-09 |
| **Supersedes** | Storage-engine sections of *MYDB Storage Engine: Proposed Storage Hierarchy Design* (v2); the remediation plan in *Buffer Pool Internals* §17 |
| **Related documents** | MYDB-DD-01 Database Hierarchy Design · MYDB-DD-04 WAL Design · PartitionBuffer Design · Buffer Pool Internals (measurements) |

---

## 1. Introduction

### 1.1 Purpose

This document specifies how MYDB stores table data. It covers the per-table file, the slotted page format, key and row encoding, the clustered B+ tree that holds every table's rows, the secondary B+ tree indexes, and the buffer pool that caches pages in memory.

### 1.2 Scope

In scope: everything below the partition manager's `pm_*` API. That is the `storage_engine` module: `disk_manager`, `page`, `btree`, `buffer_pool` and `storage`.

Out of scope:

| Topic | Document |
|---|---|
| Directory hierarchy, metadata files, quota, access control | MYDB-DD-01 |
| Schema-metadata cache (`RelationDef` pages) | PartitionBuffer Design |
| Write-ahead logging, recovery | MYDB-DD-04 |
| Measurement method and raw results for the buffer pool | Buffer Pool Internals |

### 1.3 Conventions

- Offsets are in bytes, and ranges are inclusive.
- `N` denotes `BUFFER_POOL_SIZE`, the number of buffer-pool frames.
- **Byte order differs by structure, and this is part of the format:**

| Structure | Byte order |
|---|---|
| File header (page 0) | Little-endian (native) |
| Page header, record header, page directory | Big-endian |
| Index keys | Big-endian, order-preserving (§5) |
| Record length prefixes (`klen`, `vlen`), child page numbers | Big-endian |
| Row column values, hidden columns, null bitmap | Little-endian (native) |
| RID inside a secondary-index record | Little-endian (native) |

---

## 2. Architecture

### 2.1 Layers

```
 partition_manager (pm_*)        constraints, quota, transactions, schema stats
        │
 storage.c        Storage API    row (de)serialisation, open-table cache,
        │                        clustered + secondary maintenance
 btree.c          B+ tree        search, insert, split, delete, cursors
        │
 buffer_pool.c    Buffer pool    page cache: lookup, pin, replacement, write-back
        │
 page.c           Page format    slotted page: records, directory, checksum
        │
 disk_manager.c   Disk manager   one file per table: page I/O, allocation, header
        │
 <partition>/<schema>/<relation>.mydb
```

### 2.2 Principles

1. **One file per table.** The clustered index and all secondary indexes of a table live in one file, so dropping a table is one `unlink`.
2. **Index-organised tables.** Rows are stored in the leaves of a B+ tree keyed by the primary key. There is no separate heap. Every table must declare exactly one single-column primary key.
3. **All page access goes through the buffer pool,** except page 0 (the file header), which the disk manager reads and writes directly.
4. **The storage engine is stateless with respect to schema.** Every call carries its `RelationDef`. The engine holds no catalog, quota, constraint or transaction state. There is one `StorageEngine` per partition.

---

## 3. Relation File

### 3.1 Location and addressing

Each table is stored in `<partition>/<schema>/<relation>.mydb`. The file is an array of 16 KB pages, and page *n* begins at byte `n × 16 384`. Page numbers are `uint32`. `INVALID_PAGE = 0xFFFFFFFF` means "no page".

### 3.2 Page 0: file header

Page 0 is never cached in the buffer pool. It is read when the table is opened and rewritten on every page allocation.

| Offset | Size | Field | Description |
|---|---|---|---|
| 0..3 | 4 | `magic` | `0x4D594442` |
| 4..5 | 2 | `version` | Format version |
| 6..7 | 2 | `file_type` | 4 = `FILETYPE_RELATION` |
| 8..11 | 4 | `num_pages` | Total pages including page 0 |
| 12..15 | 4 | `table_id` | Persistent identity (MYDB-DD-01 §5) |
| 16 | 1 | `flags` | bit 0 = table has an AUTO_INCREMENT column |
| 17..16383 | 16 367 | zero | |

The file header does not record the B+ tree root pages or the auto-increment counter. Both live only in the table's `RelationDef` in `__schema.mydb`, so no second copy on disk can disagree with them.

### 3.3 Page allocation

Pages are only ever appended. `disk_alloc_page` writes a zeroed page at offset `num_pages × 16 KB`, increments `num_pages`, rewrites page 0, and returns the new page number. A zeroed page has `page_no = 0` in its header, which is how the buffer pool recognises a page that has been allocated but not yet initialised (§9.7).

Pages are never freed or reused, and the file never shrinks. Space is returned only when the whole table is dropped.

### 3.4 Disk I/O

All I/O is positional (`pread`/`pwrite`), and loops until the full 16 KB transfer completes. The disk manager does not `fsync`. Durability points are owned by the layers above (MYDB-DD-01 §13).

---

## 4. Page Format

### 4.1 Layout

Pages 1..n of a relation file use an InnoDB-style slotted format:

```
 0        38        51        64                                   16376    16384
 ┌────────┬─────────┬─────────┬──────────────────────┬─────────────┬────────┐
 │ header │infimum  │supremum │ records → (heap)     │ ← directory │trailer │
 │  38 B  │  13 B   │  13 B   │ grow upward          │ grows down  │  8 B   │
 └────────┴─────────┴─────────┴──────────────────────┴─────────────┴────────┘
                                ^ free_offset          ^ 16376 − 2·num_dir_slots
```

### 4.2 Page header (38 B, big-endian)

| Offset | Size | Field | Description |
|---|---|---|---|
| 0 | 4 | `checksum` | CRC32C over bytes 4..16375 (§4.8) |
| 4 | 4 | `page_no` | This page's number; 0 means never initialised |
| 8 | 4 | `prev_page` | Leaf: left sibling. Internal: **leftmost child** |
| 12 | 4 | `next_page` | Leaf: right sibling. Internal: unused (`INVALID_PAGE`) |
| 16 | 8 | `lsn` | Log sequence number of the last change (MYDB-DD-04) |
| 24 | 2 | `page_type` | §4.3 |
| 26 | 2 | `num_records` | Live user records |
| 28 | 2 | `free_offset` | Where the next record is written |
| 30 | 2 | `garbage_offset` | Reserved for a deleted-record free list; always 0 |
| 32 | 2 | `num_dir_slots` | Directory entries; always equal to `num_records` |
| 34 | 4 | reserved | |

### 4.3 Page types

| Value | Name | Used for |
|---|---|---|
| 0 | `PAGE_TYPE_DATA` | Clustered-index leaf (full rows) |
| 1 | `PAGE_TYPE_INTERNAL` | Internal node of any B+ tree |
| 2 | `PAGE_TYPE_META` | Reserved |
| 3 | `PAGE_TYPE_INDEX` | Secondary-index leaf |

The page type determines how a record's length is computed (§4.6), so every page in an index carries the type of that index.

### 4.4 Records and the record header

Every record is a 5-byte header followed by its payload. Offsets that refer to a record point to its **payload**, not its header.

| Byte | Field |
|---|---|
| 0 | `info_flags`: bit 0 = deleted, bit 1 = reserved (min-rec) |
| 1..2 | `(heap_no << 3) \| rec_type`, big-endian. `heap_no` is informational; `rec_type`: 0 ordinary, 1 node pointer, 2 infimum, 3 supremum |
| 3..4 | `next_offset`: absolute payload offset of the next record in key order, big-endian; 0 ends the chain |

Two pseudo-records are created when the page is initialised and are never removed:

| Record | Header at | Payload at | Payload |
|---|---|---|---|
| Infimum | 38 | 43 | `"infimum\0"` |
| Supremum | 51 | 56 | `"supremum"` |

User records start at offset 64 (`USER_RECORDS_OFFSET`). They are appended at `free_offset` in arrival order; physical position carries no meaning.

### 4.5 Page directory

The directory is an array of 2-byte big-endian payload offsets growing down from the trailer. Slot *i* is stored at `16374 − 2i`. The directory is kept in key order: slot 0 is the smallest key. It contains only live records.

Records are therefore reachable two ways, and the two must agree:

- through the directory, in key order, which is used for search and iteration;
- through the `next_offset` chain from infimum to supremum. The chain also threads deleted records until the page is rebuilt.

**Invariant:** walking the chain and skipping deleted records visits exactly the directory's offsets, in the same order, and `num_records == num_dir_slots`. Debug builds check this after every page mutation.

### 4.6 Record payload length

A record's payload length is derived from its content and the page type. No length is stored in the record header.

| Page type | Payload | Length |
|---|---|---|
| DATA | `[klen:2][key][vlen:2][value]` | 4 + klen + vlen |
| INTERNAL | `[klen:2][key][child:4]` | 6 + klen |
| INDEX | `[klen:2][key][page_no:4][slot_no:2]` | 8 + klen |

### 4.7 Operations

**Free space** = `(16376 − 2 × num_dir_slots) − free_offset`.

**Insert at slot *s*** requires `5 + payload + 2` bytes of free space, otherwise it returns `MYDB_ERR_FULL`. The steps are:

1. Write the header and payload at `free_offset`.
2. Link the record into the chain after the record in slot *s − 1*, or after infimum when *s* = 0.
3. Shift directory slots *s..n−1* up by one and write slot *s*.
4. Advance `free_offset` and increment both counters.

The caller chooses *s* by key search.

**Delete slot *s*** is lazy:

1. Set the deleted bit in the record header.
2. Remove slot *s* from the directory, shifting later slots down.
3. Decrement both counters.

The record's bytes and its place in the chain remain.

**Rebuild** creates a fresh page and re-inserts the live records in directory order. This produces a compact heap, a clean chain and no dead bytes. A rebuild happens whenever a page is split (§7.4). `page_compact` performs the same rebuild in place.

**Effect on record positions.** Because the directory is kept in key order, inserting or deleting at slot *s* renumbers every later slot on the page. A record's slot number is therefore stable only until the next change to its page (see §8.5).

### 4.8 Trailer and checksum

| Offset | Size | Field |
|---|---|---|
| 16376 | 4 | copy of the header checksum |
| 16380 | 4 | low 32 bits of `lsn` |

The checksum is **CRC32C** (Castagnoli polynomial) over bytes `4..16375`, so the checksum field and the trailer are excluded. It is computed with the SSE4.2 `crc32` instruction, eight bytes per instruction, with a byte-wise tail. The buffer pool sets the checksum each time it writes a page back and verifies it each time it reads a page in (§9.7).

CRC32C is used rather than a byte-at-a-time hash because the checksum runs on every page read from disk and every page written back. A hardware CRC processes 16 KB in a few thousand cycles, and has stronger error detection than FNV-1a.

---

## 5. Key Encoding

Index keys are encoded so that **comparing the bytes gives the same order as comparing the values**. Keys are compared with `memcmp` and never decoded during search.

| Type | Encoding | Length |
|---|---|---|
| INT | int32 with sign bit flipped, big-endian | 4 |
| DECIMAL | int64 (value × 10^scale) with sign bit flipped, big-endian | 8 |
| DATE | int32 `YYYYMMDD` with sign bit flipped, big-endian | 4 |
| DATETIME | int64 `YYYYMMDDHHmmSS` with sign bit flipped, big-endian | 8 |
| VARCHAR | `[len:2 BE][bytes]` | 2 + len |
| BOOL, ENUM | one byte | 1 |

**Comparison.** Fixed-width types are compared with `memcmp` over the shorter length, then by length. VARCHAR is compared by content (`memcmp` of the string bytes), then by length, so the length prefix does not affect order.

The largest key is a VARCHAR(150): 152 bytes.

---

## 6. Row Format

A clustered-leaf record stores the encoded primary key followed by the full row:

```
[klen:2 BE][primary key, §5][vlen:2 BE][ row value ]

row value:
  DB_TRX_ID     6 B   transaction that last wrote the row (LE)
  DB_ROLL_PTR   7 B   rollback pointer, zero until undo logging exists
  null bitmap   4 B   bit i = 1 when column i is NULL (LE)
  column 0..n-1       fixed-width, in declaration order
```

**Column widths:**

| Type | Bytes |
|---|---|
| INT | 4 |
| DECIMAL | 8 |
| VARCHAR(m) | 2 + m (2-byte LE length, then data padded to m) |
| ENUM | 1 (index into the column's value list) |
| BOOL | 1 |
| DATE | 4 |
| DATETIME | 8 |

Every column occupies its full declared width, including NULL columns (written as zeros) and short VARCHAR values. Rows of one table are therefore all the same length:

`vlen = 17 + sum(column widths)`, `record = 4 + klen + vlen`, and the space used on the page is `record + 7`.

The primary key appears twice in each record: once encoded as the index key, and once in native form inside the row value. Search uses the first copy and reads return the second. `DB_ROW_ID` is not used, because every table has a declared primary key.

**Example.** For `(id INT PRIMARY KEY, code INT)`, `vlen` = 17 + 8 = 25 and the record is 33 bytes, so about 400 rows fit in one leaf.

---

## 7. Clustered Index

### 7.1 Structure

Each table's clustered index is a B+ tree keyed by the encoded primary key. All rows live in the leaves, which are `PAGE_TYPE_DATA` pages linked in key order through `prev_page`/`next_page`. Internal nodes are `PAGE_TYPE_INTERNAL` pages. The root page number is held in `RelationDef.root_page_no`. A new table starts with one empty leaf as its root, allocated at CREATE TABLE. The maximum supported height is 16.

### 7.2 Internal nodes

An internal node with *k* separator records has *k + 1* children:

- `prev_page` in the page header holds the **leftmost child**, which covers keys less than the first separator;
- separator record *i* is `[klen][key_i][child_i]`, where `child_i` covers keys `key_i ≤ k < key_{i+1}`.

**Search** starts at the leftmost child and follows the child of the right-most separator whose key is ≤ the search key. Within a page, the search is a linear scan of the key-ordered directory.

### 7.3 Insert

1. Descend from the root to the target leaf, recording the path of page numbers.
2. If the key already exists in the leaf, return `MYDB_ERR_DUPLICATE`.
3. Insert at the first slot whose key is ≥ the new key. If the record fits, finish.
4. Otherwise split the leaf (§7.4), insert into whichever half the key belongs to, then insert the separator into the parent. If the parent is also full, split it the same way, and continue up the recorded path.
5. If the root splits, allocate a new internal root. Its leftmost child is the old root and its first separator points to the new right page. Update `root_page_no`.

The caller compares the root page numbers before and after the insert. When a root has changed, it marks the `RelationDef` dirty so the new root is persisted at commit (MYDB-DD-01 §13).

### 7.4 Splits

| | Leaf split | Internal split |
|---|---|---|
| Split point | median of live records | median of separators |
| Left page | rebuilt in place with the lower half | rebuilt in place with the lower half; keeps its leftmost child |
| Right page | newly allocated with the upper half | newly allocated with the upper half; its leftmost child is the median separator's child |
| Separator sent up | **copy** of the right page's first key | the median key itself (**moved up**) |
| Sibling links | `left <-> right <-> old right neighbour` | none |

A split rebuilds both pages (§4.7), so dead records left by deletes are discarded at that point.

Splitting at the median leaves both pages about half full. With ascending keys, such as AUTO_INCREMENT, the left page never receives another insert, so steady-state leaf fill is about 50%.

### 7.5 Delete

Delete searches for the key and deletes its slot lazily (§4.7). Pages are never merged or redistributed, and an emptied page stays in the tree and in the leaf chain.

### 7.6 Update

Update is a delete of the old record followed by an insert of the new one. Before anything is changed, the engine checks that a changed primary key does not already exist and that a changed UNIQUE value does not conflict.

### 7.7 Cursors

A cursor holds `(page_no, next_slot)` and pins no pages between calls.

| Cursor | Positioned at |
|---|---|
| Full scan | The leftmost leaf, found by following `prev_page` (leftmost child) down from the root |
| Range scan from *lo* | The first slot with key ≥ *lo* |

`next` returns the record in the current slot and advances. At the end of a page it follows `next_page`. Upper bounds are checked by the caller. A cursor reports the `(page_no, slot)` of each record it returns, and the row's RID is built from that position.

### 7.8 Height

Height is the number of pages on the leftmost root-to-leaf path. The planner uses it (`RelationEntry.tree_height`) as the cost of one index descent.

---

## 8. Secondary Indexes

### 8.1 Creation

| Source | Kind | Built |
|---|---|---|
| A UNIQUE column in CREATE TABLE | Unique | Empty root allocated with the table |
| CREATE INDEX on a UNIQUE column | Unique | Root allocated and existing rows backfilled |
| CREATE INDEX on any other column | Non-unique | Root allocated and existing rows backfilled |

A table has at most 8 secondary indexes. The primary-key column cannot be indexed again. `RelationDef` records each index's column (`secondary_col_idx[]`) and root page (`secondary_root_page_no[]`).

### 8.2 Structure

A secondary index is a B+ tree in the same file as the clustered index, sharing its page space and its buffer-pool `table_id`. Internal nodes use the clustered format (§7.2). Leaves are `PAGE_TYPE_INDEX` pages, including the initial root. Each leaf record maps a key to the row's position in the clustered index:

```
[klen:2 BE][column value, §5][page_no:4 LE][slot_no:2 LE]
```

The `(page_no, slot_no)` pair is the row's **RID**: the clustered leaf page and the directory slot that hold the row.

A unique index rejects a second entry with an equal key. A non-unique index accepts equal keys, which sit next to each other in leaf order, so a range cursor positioned at a key returns every entry with that key.

### 8.3 Lookup and scan

**Point lookup** first descends the secondary tree to the matching entry and reads its RID. It then fetches clustered page `page_no`, takes the record in directory slot `slot_no` and decodes the row. This costs one secondary descent plus one page access; there is no second tree descent.

**Index scan** opens a cursor on the secondary tree, at the leftmost leaf or at the first key ≥ *lo*. For each entry it reads the RID and fetches the row the same way.

### 8.4 Maintenance

| Operation | Effect on each secondary index |
|---|---|
| INSERT | Add `(value, RID of the new row)` |
| DELETE | Remove the entry with the row's value |
| UPDATE | When the indexed value changes: remove the old entry and add `(new value, new RID)` |
| CREATE INDEX backfill | One entry per existing row, skipping NULL values |

Before any change is made, UPDATE checks every changed unique value for a conflict.

On INSERT, a NULL value is indexed using the zero bytes of its column, the same key as the value 0. A unique index therefore accepts one NULL, and that NULL also conflicts with the value 0 (limitation L11).

### 8.5 RID stability

An RID names a physical position, and that position changes when:

- a record is inserted into or deleted from the same leaf at a lower slot (the directory is renumbered, §4.7);
- the leaf splits (half its records move to a new page);
- the row is updated (it is deleted and re-inserted at a new position).

The secondary entries that refer to these rows are not rewritten when this happens. A secondary entry is therefore exact only until the clustered leaf it points into next changes. See limitation L1 (§12).

---

## 9. Buffer Pool

### 9.1 Role

The buffer pool caches relation pages 1..n in memory. Every B+ tree page access goes through it. The pool owns the decision of when a modified page reaches disk, and verifies every page it reads. There is one pool per partition, embedded in that partition's `StorageEngine`. All tables of the partition share it, and a page is identified by `(table_id, page_no)`.

### 9.2 Sizing and memory model

| Parameter | Value |
|---|---|
| Frames per pool, `N` | 4 096 |
| Page memory per pool | 4 096 × 16 KB = 64 MB (virtual) |
| Fixed bookkeeping per pool | ≈ 256 KB (keys 64 KB, page table 128 KB, LRU links 32 KB, DM table 32 KB) |

**Page memory is committed on demand.** Initialising a pool clears only its bookkeeping: frame keys, page table, LRU list and DM table. The page array is never written at initialisation, and a frame's bytes are not valid until a page has been read into them. Physical memory behind a frame is therefore allocated by the operating system the first time a page is loaded into it. A partition's resident pool size grows with the pages it actually uses, up to 64 MB, rather than being paid in full when the partition opens.

The partition's schema-metadata cache follows the same rule: its 8 × 32 `RelationDef` frames (≈ 6.5 MB) are not cleared at initialisation, and a frame is read only when its directory entry is marked resident (PartitionBuffer Design). Measured with both rules in place, 16 open but idle partitions occupy 3.8 MB in total (Buffer Pool Sharing Study, run 7).

### 9.3 Frame organisation

A frame is the same index *i* into several parallel arrays, each holding one kind of data:

```c
typedef struct {                /* 16 B, four per 64-byte cache line */
    uint32_t page_no;
    int32_t  table_id;
    uint16_t pin_count;         /* > 0: in use, not evictable */
    uint8_t  is_dirty;
    uint8_t  is_valid;          /* 0: frame is empty */
    uint32_t reserved;
} BPFrameKey;

typedef struct {
    alignas(64) BPFrameKey keys[N];        /* scanned: compact, cache-line aligned */
    uint8_t     pages[N][PAGE_SIZE];       /* page bytes, touched only on access   */
    DiskManager *dm[N];                    /* file handle for write-back           */
    alignas(64) BPHashSlot ht[HT_SIZE];    /* page table, §9.4                     */
    int         lru_prev[N], lru_next[N];  /* replacement list, §9.5               */
    int         lru_head, lru_tail;
} BufferPool;
```

**Why the key is stored apart from the page.** The pool keeps descriptors (`keys[]`) and page bytes (`pages[]`) in separate arrays. Every operation that sweeps frames reads only descriptors. Flushing a table, flushing all dirty pages and evicting a table are such sweeps, and they visit all *N* frames. With 16-byte keys stored contiguously, a sweep reads 64 KB of sequential, prefetchable memory: four frames per cache line and 16 frames per 4 KB OS page.

If each descriptor were stored next to its 16 KB page, consecutive descriptors would be 16 KB apart. Each frame visited would then cost its own cache line and TLB entry, and the hardware prefetcher could not help. That access pattern was measured at about 23 µs per full sweep of 4 096 frames (Buffer Pool Internals §17). The `alignas(64)` ensures no group of four keys straddles two cache lines.

### 9.4 Page table

A hash table maps `(table_id, page_no)` to a frame index, so finding a cached page costs O(1) regardless of `N`.

| Property | Value |
|---|---|
| Size | `HT_SIZE` = smallest power of two ≥ 2N (8 192 slots for N = 4 096); load factor ≤ 0.5 |
| Slot | `{int32 table_id, uint32 page_no, int32 frame, uint32 used}`: 16 B, cache-line aligned |
| Hash | Fibonacci hashing: `k = (table_id << 32 \| page_no) × 0x9E3779B97F4A7C15`; home slot = high 32 bits of `k` masked to `HT_SIZE − 1` |
| Collisions | Open addressing with linear probing |
| Deletion | Backward-shift: later entries of the same probe run are moved into the hole, so there are no tombstones and lookups never step over deleted slots |

Each valid frame has exactly one page-table entry. The entry is added when a page is loaded into the frame, and removed when the frame is reused for another page or its table is evicted.

### 9.5 Replacement policy

Replacement is **LRU**, kept as an intrusive doubly linked list over frame indexes (`lru_prev`, `lru_next`). The head is the most recently used frame and the tail the least recently used.

- **Touch** (on every hit and every load): unlink the frame and push it to the head. O(1).
- **Victim selection:** walk from the tail toward the head and take the first frame that is either empty or has `pin_count == 0`. At initialisation every frame is empty and the whole list is the tail, so a filling pool never evicts. A frame emptied later by `bp_evict_table` keeps its position in the list, and is reused when the walk reaches it.
- **All frames pinned:** the fetch fails immediately and returns NULL. It does not wait; under single-threaded execution nothing could release a pin while the caller waited.

### 9.6 Frame placement

At initialisation the LRU list is ordered so that **frame 0 is at the tail**, followed by frame 1, and so on. Empty frames are therefore used in ascending index order, and a pool that has loaded *r* distinct pages without evicting uses frames `0..r−1`. Combined with on-demand memory (§9.2), the memory committed to the pool is a contiguous prefix of the page array, sized to what the partition has actually cached, rather than pages scattered across the 64 MB region.

Placement is also what keeps any position-ordered search proportional to the pool's occupancy rather than its declared size. If frames filled from index N−1 downward while a search walked upward from 0, every search would first cross the entire empty region. The page table (§9.4) removes such searches from lookups, but frame order still determines where page memory is committed.

### 9.7 Operations

**`bp_fetch_page(table_id, page_no)`** returns a pinned pointer to the page bytes:

1. Refuse `page_no == 0`: the file header is not cached.
2. Look up the page table. **Hit:** increment `pin_count`, touch, return.
3. **Miss:** choose a victim (§9.5); if none, return NULL.
4. If the victim is dirty, compute its checksum and write it with the victim's own `dm[]` handle (not the caller's, which may belong to a different table).
5. Read the requested page into the frame.
6. Verify its CRC32C, unless the header's `page_no` is 0, which marks a freshly allocated page that has not been initialised. On a mismatch, return NULL.
7. Remove the victim's page-table entry. Set the key to `(table_id, page_no, pin = 1, clean, valid)` and record `dm[]`. Insert the new page-table entry and touch.

**`bp_unpin_page(table_id, page_no, dirty)`** decrements `pin_count`. If `dirty` is set, it marks the frame dirty. Every fetch must be matched by exactly one unpin.

**`bp_alloc_page`** appends a zeroed page to the file (§3.3) and fetches it. The caller initialises it with `page_init`.

**Write-back and eviction:**

| Function | Effect |
|---|---|
| `bp_flush_page` | Write one page if it is dirty |
| `bp_flush_table` | Write every dirty page of one table; on a write error, continue with the rest and keep the failed page dirty |
| `bp_flush_dirty_all` | Write every dirty page in the pool (COMMIT) |
| `bp_evict_table` | Invalidate every frame of one table without writing it, removing their page-table entries. Fails if any is pinned. Must run before the table's `DiskManager` is closed. |

Every write-back sets the page checksum immediately before the write.

### 9.8 Complexity

| Operation | Cost |
|---|---|
| Fetch hit, unpin, flush one page | O(1) expected |
| Fetch miss | O(1) expected + victim walk past pinned frames + one read (+ one write if dirty) |
| Flush table, flush all, evict table | O(N) over the 64 KB descriptor array |

### 9.9 Interaction with transactions

The pool does not track transactions. Before the WAL, the partition manager uses the pool as follows (MYDB-DD-01 §13.2):

- **COMMIT** calls `bp_flush_dirty_all`.
- **ROLLBACK** calls `bp_evict_table` for every open table. Dirty pages are discarded and re-read from disk on next access.

**The pool can write a dirty page to disk before COMMIT.** When it needs a frame and the least recently used unpinned page is dirty, it writes that page out to free the frame (§9.7, step 4). Rollback cannot undo such a write. Rollback is therefore exact only for transactions whose dirty pages all stay resident until they end. The WAL removes this condition by logging changes before any page reaches disk (MYDB-DD-04).

### 9.10 Concurrency

The pool has no latches. It relies on single-threaded statement execution within a partition (MYDB-DD-01 §9.3). Pin counts protect pages from eviction while a caller is using them. They do not protect against concurrent access.

---

## 10. Open-Table Cache

Each `StorageEngine` keeps up to 64 open tables, keyed by `(schema name, relation name)`. Opening a table does three things:

1. open its file and read `num_pages`;
2. build the clustered `BTree` handle from `RelationDef.root_page_no`;
3. build one handle per secondary index (unique or non-unique according to the column).

The buffer-pool `table_id` of a table is its persistent `table_id`.

A table is opened lazily on first access and stays open until it is dropped or the partition closes. Closing a table flushes it, evicts it, then closes its file, in that order.

---

## 11. Capacity Limits

| Limit | Value |
|---|---|
| Page size | 16 384 B |
| Pages per file | 2³² − 2 |
| Columns per table | 32 |
| Primary key | exactly one column |
| Secondary indexes per table | 8 |
| Key length | 152 B (VARCHAR(150)) |
| VARCHAR length | 150 |
| B+ tree height | 16 |
| Records per page (structural bound) | ≈ 1 160 (smallest internal record: 1-byte key) |
| Open tables per partition | 64 |
| Buffer-pool frames per partition | 4 096 |

---

## 12. Known Limitations

| # | Limitation | Consequence |
|---|---|---|
| L1 | Secondary indexes store physical RIDs that are not updated when rows move (§8.5). | After an insert, delete, update or split in a clustered leaf, an index lookup can return a different row or none. Storing the primary key instead of the RID, then descending the clustered index, would make entries position-independent. This is the InnoDB design. |
| L2 | Steal without undo (§9.9). | A dirty page written during a transaction survives ROLLBACK. Resolved by the WAL. |
| L3 | No page merge, no page reuse, no file shrink. | Space freed by deletes is reused only when the same leaf receives new inserts or splits. Emptied pages remain. |
| L4 | Median split only. | Ascending-key tables fill leaves to about 50%, which roughly doubles their page count and lowers the buffer-pool hit ratio. Splitting at the end when the insert lands at the right edge of the rightmost leaf would fill these pages. |
| L5 | Fixed-width rows. | A VARCHAR always takes its declared width, regardless of the value stored. |
| L6 | Linear search within a page. | Up to a few hundred key comparisons per page; the directory is key-ordered, so binary search is possible. |
| L7 | The CRC32C path requires SSE4.2. | An x86-64 CPU with SSE4.2 is required. Other architectures need a software or architecture-specific CRC32C. |
| L8 | Flush and evict sweeps are O(N). | Their cost is bounded by the 64 KB descriptor array. |
| L9 | No latching (§9.10). | Concurrent execution within a partition requires page latches. |
| L10 | Secondary deletes are by key only. | In a non-unique index, deleting a row removes the first entry with an equal key, which may belong to another row. Deleting by `(key, primary key)` resolves this, and follows naturally from the fix for L1. |
| L11 | NULL is indexed on INSERT as the zero value of its type (§8.4). | A unique column accepts only one NULL, and a NULL conflicts with 0. SQL semantics require NULLs to be left out of unique indexes, as the CREATE INDEX backfill already does. |
