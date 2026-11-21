#ifndef SCHEMA_FILE_H
#define SCHEMA_FILE_H

#include "common.h"
#include "relation_def.h"

/* ------------------------------------------------------------------ */
/*  __schema.mydb — schema definition file                            */
/*                                                                    */
/*  One file per schema, ~1 MB fixed (SCHEMA_FILE_PAGES x PAGE_SIZE). */
/*                                                                    */
/*    Page 0          : SchemaHeader + RelationEntry slot directory   */
/*                      + CRC-32C checksum trailer                    */
/*    Pages 1..64     : one full serialized RelationDef per page,     */
/*                      allocated on demand                           */
/*                                                                    */
/*  Loaded into Cache 2 on USE schema_name. Direct pread/pwrite —     */
/*  bypasses the buffer pool entirely (consistent with other engine   */
/*  metadata files).                                                  */
/* ------------------------------------------------------------------ */

/* SchemaHeader — Page 0 fixed prefix (72 B per design doc §8.1) */
typedef struct {
    uint32_t partition_id;            /* which partition owns this schema */
    uint32_t schema_id;                /* persistent identity, stamped once at
                                         * CREATE SCHEMA via cat_alloc_schema_id;
                                         * mirrored into the partition catalog's
                                         * SchemaEntry.schema_id */
    char     schema_name[32];
    uint64_t created_at;              /* YYYYMMDDHHmmSS */
    uint64_t last_modified;           /* refreshed on every Page 0 save */
    uint64_t size_bytes;              /* computed at load, NOT persisted */
    uint8_t  num_relations;           /* count of valid slots */
} SchemaHeader;

/* RelationEntry — slot directory entry (56 B per design doc §8.2) */
typedef struct {
    char     relation_name[32];
    uint8_t  is_valid;                /* 1 = occupied, 0 = empty */
    uint8_t  page_no;                 /* 1..SCHEMA_FILE_PAGES-1 */
    uint8_t  num_columns;
    uint32_t num_rows;                /* maintained by INSERT/DELETE callers */
    uint32_t num_pages;               /* persisted source of truth for size */
    uint8_t  tree_height;             /* B+ tree height (1 = root only); CBO cost input */
    uint8_t  reserved;               /* reserved for future CBO fields */
    uint32_t table_id;                /* persistent identity; mirrors the relation
                                        * file's own FileHeader.table_id. Catalog-level
                                        * copy — read here, not by opening the .mydb file. */
} RelationEntry;

/* In-memory schema file representation — Page 0 only (header + relation
 * directory). This is exactly what PB[X][0] is (PARTITION_BUFFER_DESIGN.md
 * §2): always resident whenever the outer slot is loaded, never itself
 * evicted independently.
 *
 * Phase 2 of the PartitionBuffer redesign moved RelationDef storage OFF
 * this struct entirely — `defs[]` and Phase 1's `pin_count[]` used to live
 * here, parallel-indexed with `relations[]`. They don't anymore: RelationDef
 * pages are now a real evictable 32-of-64 cache owned by `PartitionBuffer`
 * (`PBOuterSlot.inner[]`/`dir[]`/`pin_count`, partition_buffer.h) — up to 64
 * relations can exist (`relations[]` below still covers all of them, it's
 * small and always fully resident), but only 32 of their RelationDefs are
 * ever loaded in memory at once. `schema_find_relation()` — which used to
 * hand back `&defs[slot]` — is gone with it; resolving a RelationDef* now
 * goes through PartitionBuffer's lazy-load path (pm_find_relation), not
 * SchemaFile. */
typedef struct {
    SchemaHeader  header;
    RelationEntry relations[MAX_RELATIONS_PER_SCHEMA];
    int           fd;                 /* open fd; -1 when closed */
    char          path[256];
} SchemaFile;

/* ------------------------------------------------------------------ */
/*  Lifecycle                                                          */
/* ------------------------------------------------------------------ */

/* Create a new __schema.mydb. Fails if the file already exists.
 * Pre-allocates SCHEMA_FILE_SIZE zeroed bytes and writes Page 0.
 * schema_id is the already-allocated persistent id (from
 * cat_alloc_schema_id) — stamped into SchemaHeader.schema_id. */
int schema_create(const char *path, uint32_t partition_id, uint32_t schema_id,
                  const char *schema_name, SchemaFile *out);

/* Open an existing __schema.mydb. Verifies magic, version, file type,
 * and Page 0 checksum. Loads the slot directory only — RelationDef pages
 * are no longer eagerly loaded (Phase 2: they're lazy-loaded on demand
 * into PartitionBuffer's inner cache, see pm_find_relation). */
int schema_open(const char *path, SchemaFile *out);

/* Close the file descriptor. Caller is responsible for saving any
 * unsaved changes; every public mutator already saves. */
int schema_close(SchemaFile *sf);

/* Refresh last_modified, repack Page 0, recompute checksum, pwrite,
 * fsync. Use after editing header/relations[] without going through
 * a higher-level mutator. */
int schema_save_page0(SchemaFile *sf);

/* Re-read and unpack Page 0 from disk into the already-open *sf,
 * discarding any in-memory changes made since the last save (header,
 * relations[]). Phase 3 of the PartitionBuffer redesign: the write-back
 * counterpart to schema_save_page0 — used by pb_discard_slot_dirty
 * (partition_buffer.h) to revert a rolled-back transaction's uncommitted
 * stat bumps without persisting them. size_bytes is recomputed after
 * reload, same as schema_open does. */
int schema_reload_page0(SchemaFile *sf);

/* ------------------------------------------------------------------ */
/*  RelationDef CRUD                                                   */
/* ------------------------------------------------------------------ */

/* Add a relation: picks the lowest free slot in relations[] and the
 * lowest free def page (1..SCHEMA_FILE_PAGES-1), serializes def to that
 * page, marks the slot valid, persists the def page and Page 0.
 * Rejects duplicate relation_name. Returns MYDB_ERR_FULL when no slot
 * is free. Does not populate any cache — the caller (PartitionBuffer)
 * lazy-loads on next access same as any other relation. */
int schema_add_relation(SchemaFile *sf, const RelationDef *def);

/* Remove a relation by name: zeroes the def page on disk, clears the
 * slot, persists Page 0. Returns MYDB_ERR_NOT_FOUND if absent. Caller
 * (pm_drop_table) is responsible for invalidating any cached inner frame
 * for this relation's page_no in PartitionBuffer — schema_file.c has no
 * visibility into PartitionBuffer, by design (see relation_def.h's
 * layering note). */
int schema_remove_relation(SchemaFile *sf, const char *relation_name);

/* Directory-only lookup (RelationEntry — name/page_no/table_id/stats),
 * never evicted, no pin needed. Pointer valid for the lifetime of *sf,
 * NULL if no valid slot matches. For the actual RelationDef content, see
 * pm_find_relation (partition_buffer's lazy-load cache). */
RelationEntry *schema_find_relation_stat(SchemaFile *sf, const char *relation_name);

/* Load one relation's def page from disk (page_no already known — e.g.
 * from schema_find_relation_stat) into *out: deserializes and stamps
 * owner_schema, same as schema_open's old eager-load loop used to do
 * per-relation. This is the lazy-load primitive PartitionBuffer's inner
 * cache (pb_pin_relation, partition_buffer.h) calls to populate one
 * frame — schema_file.c holds no RelationDef cache of its own to serve
 * this from (Phase 2). */
int schema_load_relation_page(SchemaFile *sf, uint8_t page_no, RelationDef *out);

/* Re-serialize and persist def to the relation's existing page (found by
 * def->relation_name). Used when a running relation's auto_incr_counter
 * or root_page_no changes. Does NOT touch Page 0. Caller passes the
 * content directly (schema_file.c no longer holds any RelationDef data
 * itself) — typically a PBInnerFrame.def the caller already has pinned. */
int schema_flush_relation(SchemaFile *sf, const RelationDef *def);

/* Update optimizer-stat fields (num_rows, num_pages, tree_height) on
 * the relation's slot, in memory only. Phase 3: no longer persists —
 * caller (pm_api.c) marks the owning PBOuterSlot's page0_dirty via
 * pb_mark_page0_dirty (partition_buffer.h) instead; actual persistence
 * happens later via pb_flush_slot_dirty (on COMMIT, eviction, or
 * shutdown). */
int schema_update_stats(SchemaFile *sf, const char *relation_name,
                        uint32_t num_rows, uint32_t num_pages,
                        uint8_t tree_height);

/* Increment (delta>0) or decrement (delta<0) the num_pages counter for
 * one relation slot, in memory only (Phase 3 — see schema_update_stats's
 * comment; caller marks page0_dirty, does not persist here). Refuses to
 * drive the counter below 0. Returns MYDB_ERR_NOT_FOUND if no valid slot
 * matches `relation_name`.
 *
 * Used by storage.c after every successful partition_alloc_page on a
 * user relation (phase 9 — closes the deferral from phase 6's
 * single-responsibility split). */
int schema_bump_relation_pages(SchemaFile *sf, const char *relation_name,
                               int32_t delta);

/* Increment (delta>0) or decrement (delta<0) the num_rows counter for
 * one relation slot, in memory only (Phase 3 — see schema_update_stats's
 * comment; caller marks page0_dirty, does not persist here). Refuses to
 * drive the counter below 0. Returns MYDB_ERR_NOT_FOUND if no valid slot
 * matches `relation_name`.
 *
 * Called by storage.c after every successful storage_insert (+1) and
 * storage_delete (-1) to keep num_rows accurate for the CBO. */
int schema_bump_relation_rows(SchemaFile *sf, const char *relation_name,
                              int32_t delta);

/* Sum of (entry.num_pages * PAGE_SIZE) across valid slots. Computed
 * fresh — never read from disk. */
uint64_t schema_compute_size_bytes(const SchemaFile *sf);

#endif /* SCHEMA_FILE_H */
