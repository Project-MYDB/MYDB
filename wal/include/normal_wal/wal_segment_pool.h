#ifndef WAL_SEGMENT_POOL_H
#define WAL_SEGMENT_POOL_H

#include <stddef.h>
#include <stdint.h>
#include "common.h"
#include "normal_wal/wal_segment.h"
#include "wal_worker.h"

/*
 * wal_segment_pool.h — the pool of WAL_SEGMENT_POOL_SLOTS physical
 * wal_<N>.mydb segment files: pre-allocation, round-robin claiming, raw
 * page I/O within a claimed slot, and the crash-reload tail-scan
 * (MYDB_WAL_IMPLEMENTATION.md §8.4).
 *
 * WalSlot (the in-memory runtime handle — fd + cached header) lives here,
 * not in wal_segment.h, which now covers only the on-disk WalSegmentHeader
 * *format*. This module is the pool/runtime concern.
 *
 * Interface discipline: WalSegmentPool/WalSlot are transparent structs
 * (visible fields, stack-constructible, easy to test) — the same shape
 * BufferPool (storage_engine/include/buffer_pool.h) already uses in this
 * codebase. But exactly like BufferPool, nothing outside this module's
 * own .c file is meant to touch pool->slots[i] fields directly. Every
 * future consumer goes through the wal_segment_pool_*() functions below
 * only — in particular, two consumers this phase doesn't build yet but
 * whose access pattern shapes what's exposed here:
 *
 *   - The Flusher (design doc §7) will write pages one at a time to
 *     whichever slot it currently holds active, via
 *     wal_segment_pool_write_page(). When IT decides that segment is
 *     full ("if segment full: close segment, claim next SEG_FREE, open
 *     new segment" — its own decision, using its own cursor, not this
 *     module's job to predict), it calls wal_segment_pool_mark_done() to
 *     transition SEG_ACTIVE -> SEG_DONE, then wal_segment_pool_claim_next()
 *     for the next one. This module only supplies those primitives; the
 *     rollover *decision* and cursor bookkeeping belong to the Flusher
 *     phase, not here.
 *   - The Normal WAL Archiver (design doc §7) calls
 *     wal_segment_pool_read_segment() to get a whole segment's raw bytes
 *     in one call ("COPY PHASE: stream segment -> tmp/tmp_seg_<N>.mydb"
 *     copies the entire file, not page-by-page).
 *
 * Still no ring buffer, current_lsn, or TxnManager/PartitionCtx wiring —
 * this pool takes an explicit wal_dir path from its caller, the same
 * pattern DiskManager already uses for table files (storage_engine/
 * include/disk_manager.h), staying fully standalone.
 */

#define WAL_SEGMENT_POOL_SLOTS      10
#define WAL_SEGMENT_FILE_SIZE       (2 * 1024 * 1024)                       /* 2MB, pre-allocated */
#define WAL_SEGMENT_PAGES_PER_FILE  (WAL_SEGMENT_FILE_SIZE / WAL_PAGE_SIZE) /* 512: 1 header page-slot + 511 data pages */

/* ------------------------------------------------------------------
 * WalSlot — in-memory only, never persisted. One per physical segment
 * slot. No last_page_modified/last_lsn fields: nothing here needs to
 * duplicate what header.end_lsn / the ring buffer's own cursors already
 * track (a later phase).
 * ------------------------------------------------------------------ */
typedef struct {
    uint8_t           slot_no;    /* 0..WAL_SEGMENT_POOL_SLOTS-1 */
    int               fd;         /* open for process lifetime */
    WalSegmentHeader  header;     /* in-memory cached copy */
} WalSlot;

/* ------------------------------------------------------------------
 * WalSegmentPool — one per partition's normal_wal. next_segment_no is
 * the pool-level monotonic counter driving claim_next()'s segment_no
 * assignment (see wal_segment_pool.c for the full reasoning: this phase
 * only exercises SEG_FREE -> SEG_ACTIVE, so numbers are assigned at
 * claim time from one shared counter rather than the design doc's
 * Archiver-driven free-time stamping, which needs a thread this phase
 * doesn't build yet).
 * ------------------------------------------------------------------ */
typedef struct {
    char      wal_dir[256];
    uint32_t  partition_id;
    uint64_t  next_segment_no;
    WalSlot   slots[WAL_SEGMENT_POOL_SLOTS];
} WalSegmentPool;

/* ------------------------------------------------------------------
 * Lifecycle.
 *
 * wal_segment_pool_init: mkdir(wal_dir) if missing. For each of the 10
 * slots: if wal_<i>.mydb doesn't exist, create + posix_fallocate() to
 * WAL_SEGMENT_FILE_SIZE + write an initial SEG_FREE header + fdatasync; if it
 * exists, open + validate + load its header. Reseeds next_segment_no
 * from whatever's on disk (see wal_segment_pool.c). Any slot reloaded in
 * SEG_ACTIVE state is tail-scanned automatically (its on-disk data_pages
 * is stale by definition while active).
 * ------------------------------------------------------------------ */
int wal_segment_pool_init(WalSegmentPool *pool, const char *wal_dir, uint32_t partition_id);
int wal_segment_pool_shutdown(WalSegmentPool *pool);

/* SEG_FREE -> SEG_ACTIVE: claims slot (next_segment_no %
 * WAL_SEGMENT_POOL_SLOTS), stamps its segment_no, rewrites the header.
 * *out_slot_index names the claimed slot. Returns MYDB_ERR if that slot
 * isn't currently SEG_FREE (expected once all 10 slots have been
 * claimed once and none has been freed yet — freeing is a later
 * phase's job).
 *
 * Deliberately never fdatasyncs: either write()'s own trailing flush
 * covers this same fd moments later (the rollover path — the only
 * caller that matters for latency), or the first real write into this
 * segment covers it (a standalone claim), or a crash before either
 * safely reverts this slot to SEG_FREE on reload (nothing was lost,
 * because nothing was written under this claim yet). */
int wal_segment_pool_claim_next(WalSegmentPool *pool, uint32_t *out_slot_index);

/* Raw page I/O within an already-claimed slot. page_no is 1-based (0 is
 * the header's own page-slot) and must be < WAL_SEGMENT_PAGES_PER_FILE.
 * No fsync here — that's the Flusher's job in a later phase; this is
 * infrastructure, not the durable commit path. This is the primitive the
 * Flusher will call once per page, tracking its own cursor across calls. */
int wal_segment_pool_write_page(WalSegmentPool *pool, uint32_t slot_index,
                                 uint32_t page_no, const uint8_t *buf);
int wal_segment_pool_read_page(WalSegmentPool *pool, uint32_t slot_index,
                                uint32_t page_no, uint8_t *out_buf);

/* wal_segment_pool_write, worker parameter: forwarded as-is into
 * mark_done() if this call rolls over (letting the old segment's fsync
 * overlap with this call's own new-segment writes) — may be NULL, in
 * which case mark_done() falls back to a synchronous fdatasync exactly
 * like before this parameter existed.
 *
 * wal_segment_pool_write — the abstraction the Flusher actually calls.
 * This function does NOT parse or construct WalPageHeader content — the
 * Flusher's buf is a run of bytes copied straight out of ring-buffer
 * frames, which are already page-shaped (header + data, LSNs already
 * stamped inside — impl doc §8.6), except possibly its very first bytes,
 * which may be a bare continuation (no header) into a page whose header
 * was already written by an earlier call. Either way, this function is a
 * blind byte mover: it only knows page/segment *geometry*
 * (WAL_PAGE_SIZE, WAL_SEGMENT_PAGES_PER_FILE), never record content.
 *
 * slot_index, page_no, and offset are the Flusher's own cursor position
 * (its own {segment_no, page_no, offset, buf} — design doc §7's WalCursor
 * shape, minus lsn/buf which don't belong to this function's parameters),
 * passed in and updated in place: offset is the exact byte position
 * within page_no's 4096-byte slot to start writing at (this module never
 * reads a page back to "discover" that — the Flusher already knows it
 * from its own bookkeeping). On return, slot_index/page_no/offset name
 * where the *next* call should resume. The Flusher never computes page
 * or segment boundaries itself.
 *
 * Splits buf across as many pages as needed purely by byte count
 * (WAL_PAGE_SIZE per page-slot). When offset reaches WAL_PAGE_SIZE, that
 * page-slot is full: moves to the next page_no, or — if that was the
 * segment's last page-slot — auto-finalizes the segment and claims the
 * next one. The one exception to "never reads header content": right
 * before finalizing, reads the just-filled last page back
 * (wal_segment_pool_read_page + wal_page_header_deserialize) purely to
 * copy its end_lsn field into mark_done()'s end_lsn argument — the only
 * reliable source for a segment's true highest LSN, since start_lsn alone
 * is only the *first* record's LSN, not the last (see wal_page.h's
 * end_lsn field comment). This is a field copy, not content
 * interpretation. Returns MYDB_ERR if claim_next fails mid-write (pool
 * exhausted).
 *
 * TEMPORARY: fdatasyncs before returning on every call — the Flusher
 * exists now (wal_flusher.h) but calls this once per drained frame with
 * no batching of its own yet (design doc §11's group-commit isn't built),
 * so there's still no batching to fold this flush into. fdatasync, not
 * fsync: these files are posix_fallocate'd to a fixed size once and
 * never resized, so there's no essential size metadata for fdatasync to
 * need to flush beyond the data itself — avoids dragging the filesystem
 * journal into every flush on filesystems like ext4. This trailing fsync
 * always stays synchronous regardless of worker — it's the last thing
 * this call does, nothing left to overlap it with; only mark_done()'s
 * fsync (a different file) benefits from being offloaded. If worker was
 * given, waits on it (a cheap no-op if this call never rolled over)
 * before returning, so a caller with a worker still gets the same
 * "fully durable by the time this returns" guarantee as the no-worker
 * path. One line to remove once the Flusher batches multiple frames
 * behind one flush per cycle. */
int wal_segment_pool_write(WalSegmentPool *pool, WalWorker *worker,
                            uint32_t *slot_index, uint32_t *page_no, uint32_t *offset,
                            const uint8_t *buf, size_t buf_len);

/* SEG_ACTIVE -> SEG_DONE: stamps the caller-supplied final end_lsn/
 * data_pages (this module has no LSN concept of its own — the Flusher,
 * once it exists, is the one that knows both values from its own
 * bookkeeping), rewrites the header. The Flusher calls this
 * when *it* decides a segment is full, right before claim_next() for
 * the replacement — the decision of "is this segment full" is the
 * Flusher's, not this module's; this only performs the transition once
 * told to. Returns MYDB_ERR if the slot isn't currently SEG_ACTIVE.
 *
 * worker == NULL: fdatasyncs synchronously before returning (today's
 * behavior). worker != NULL: hands the fdatasync to the worker and
 * returns immediately without waiting — write()'s rollover branch
 * overlaps this with writing the new segment, waiting on the worker
 * only once that's also done. Callers that pass a worker must
 * eventually call wal_worker_wait() before relying on this segment's
 * DONE state being durable — with the ticket handed back through
 * out_ticket, which names the fsync this call posted. out_ticket may be
 * NULL (it is meaningless when worker is NULL), and is left at 0 unless
 * a request was actually posted, so a caller that zero-initialises it
 * and passes the same variable down every path always waits on exactly
 * what it submitted, and on nothing when it submitted nothing. */
int wal_segment_pool_mark_done(WalSegmentPool *pool, WalWorker *worker,
                               uint32_t slot_index, uint64_t end_lsn, uint32_t data_pages,
                               uint64_t *out_ticket);

/* Reads a whole segment file's raw WAL_SEGMENT_FILE_SIZE bytes (header
 * page-slot included) into out_buf in one call — the Normal WAL
 * Archiver's COPY PHASE copies the entire file, not page-by-page (design
 * doc §7). out_buf must have room for WAL_SEGMENT_FILE_SIZE bytes. */
int wal_segment_pool_read_segment(WalSegmentPool *pool, uint32_t slot_index,
                                   uint8_t *out_buf);

/* Scans page_no = 1, 2, ... in the given slot's file, validating each via
 * wal_page_header_deserialize, stopping at the first invalid/unwritten
 * page. *out_data_pages gets the count of valid pages found. Called
 * internally by wal_segment_pool_init() for any slot reloaded in
 * SEG_ACTIVE state; exposed publicly so tests can exercise it directly.
 *
 * Sound here only because this pool has no free/reuse path yet: a slot
 * is claimed once and never released, so a page can never hold a
 * previous generation's content. Whoever adds one (the Normal WAL
 * Archiver) MUST zero a slot's content pages before releasing it —
 * stale pages deserialize perfectly well, so this walk would run past
 * the live tail and report the older segment's count. large_wal hit
 * exactly that; see large_wal_segment_pool_free_slot for the fix and
 * the write-ordering it needs. */
int wal_segment_pool_tail_scan(WalSegmentPool *pool, uint32_t slot_index,
                                uint32_t *out_data_pages);

#endif /* WAL_SEGMENT_POOL_H */
