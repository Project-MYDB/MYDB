#ifndef LARGE_WAL_SEGMENT_POOL_H
#define LARGE_WAL_SEGMENT_POOL_H

#include <stddef.h>
#include <stdint.h>
#include <pthread.h>
#include "common.h"
#include "large_wal/large_wal_segment.h"
#include "large_wal/large_wal_registry.h"
#include "wal_worker.h"

/*
 * large_wal_segment_pool.h — the rotation pool of
 * LARGE_WAL_SEGMENT_POOL_SLOTS physical large_wal_<N>.mydb segment
 * files: pre-allocation, round-robin claiming, raw page I/O within a
 * claimed slot, and the crash-reload tail-scan (MYDB_WAL_IMPLEMENTATION.md
 * §10.1). Mirrors normal_wal/wal_segment_pool.h's own original scope
 * exactly — init/reload, claim_next, raw page I/O, finalize, tail_scan.
 *
 * LargeWalSlot (the in-memory runtime handle — fd + cached header) lives
 * here, not in large_wal_segment.h, which covers only the on-disk
 * LargeWalSegmentHeader *format*. This module is the pool/runtime
 * concern — same split normal_wal already established between
 * wal_segment.h and wal_segment_pool.h.
 *
 * Files live in the SAME wal/ directory normal_wal's own pool already
 * uses, named large_wal_<slot_index>.mydb
 *
 * Interface discipline: same as WalSegmentPool — transparent struct, but
 * external callers, LARGE_WAL Writer thread and the
 * LARGE_WAL Archiver, go through the
 * large_wal_segment_pool_*() functions below only.
 *
 */

#define LARGE_WAL_SEGMENT_POOL_SLOTS      4
#define LARGE_WAL_SEGMENT_FILE_SIZE       (2 * 1024 * 1024)                          /* 2MB */
#define LARGE_WAL_SEGMENT_PAGES_PER_FILE  (LARGE_WAL_SEGMENT_FILE_SIZE / PAGE_SIZE)   /* 128:
     matches design doc's own "2MB / 16KB = 128 pages per segment" exactly. PAGE_SIZE
     (16384, common.h) reused directly — design doc: "Internal pages: 16KB (matches
     data page size)" — the same B+Tree page size, not a new constant. */

/* How long write()'s rollover waits for the archiver to free a slot
 * before giving up (see large_wal_segment_pool_claim_next_wait).
 *
 * Sized to ride out several copy-outs, not one: a copy-out is a few
 * milliseconds and the archiver's own poll interval is tens, so
 * anything under a second would start failing writes during an ordinary
 * burst -- the exact thing waiting was added to avoid. 5s is also short
 * enough that a stopped or broken archiver still surfaces as an error a
 * person will notice, rather than looking like a hang.
 *
 * If writers start hitting this under real load, that is the answer to
 * the design doc's own open question about whether 4 slots is enough
 * (impl doc 10.1, "still a placeholder pending real load testing"). */
#define LARGE_WAL_CLAIM_WAIT_MS           5000

/* ------------------------------------------------------------------
 * LargeWalSlot — in-memory only, never persisted. One per physical
 * rotation-pool slot.
 * ------------------------------------------------------------------ */
typedef struct {
    uint8_t                slot_no;   /* 0..LARGE_WAL_SEGMENT_POOL_SLOTS-1 */
    int                    fd;        /* open for process lifetime */
    LargeWalSegmentHeader  header;    /* in-memory cached copy */
} LargeWalSlot;

/* ------------------------------------------------------------------
 * LargeWalSegmentPool — one per partition's large_wal. next_segment_no
 * is the pool-level monotonic counter driving claim_next()'s segment_no
 * assignment — same scheme as WalSegmentPool (one shared counter,
 * assigned at claim time): this phase only exercises
 * LSEG_FREE -> LSEG_ACTIVE, so there's no archiver-driven free-time
 * stamping to rely on instead yet.
 * ------------------------------------------------------------------ */
typedef struct {
    char      wal_dir[256];      /* the SAME wal/ directory normal_wal uses —
                                     large_wal_<N>.mydb files sit alongside wal_<N>.mydb,
                                     not a separate folder */
    uint32_t  partition_id;
    uint64_t  next_segment_no;
    LargeWalSlot slots[LARGE_WAL_SEGMENT_POOL_SLOTS];

    /* Not owned; may be NULL (a pool driven directly, e.g. by
     * test_large_wal_segment_pool, needs no registry). When set,
     * claim_next registers every segment_no it mints — see its own doc
     * comment for why that belongs there and not in the caller. It is
     * also where this pool reaches for per-segment locking: every write
     * into a segment's content pages happens under that segment's
     * registry node lock, which is what stops a concurrent
     * large_wal_get seeing a half-written page. A NULL registry means
     * no reader can resolve these segments at all, so there is nothing
     * to lock against. */
    LargeWalRegistry *registry;

    pthread_mutex_t lock;   /* protects slots[].header and next_segment_no.
                                NOT the segment file contents — those are
                                covered by the registry node lock above.
                                Third in large_wal's global lock order:
                                reg -> node -> pool -> idx -> state. */

    /* The writer/archiver handoff, in both directions. Both use the
     * lock above — no new lock, no new ordering rule.
     *
     * They live on the pool rather than on either thread on purpose:
     * that way neither thread has to know the other exists. The writer
     * waits for "a slot became free" without knowing an archiver is
     * what frees them, and the archiver waits for "a slot became done"
     * without knowing a writer is what fills them. Keeps
     * large_wal_writer.h's "never touches large_wal_archiver at all"
     * literally true. The pool is already the one thing both of them
     * share, so it is where the handoff belongs. */
    pthread_cond_t  slot_done_cv;   /* broadcast by mark_done()  -- wakes the archiver */
    pthread_cond_t  slot_free_cv;   /* broadcast by free_slot()  -- wakes a waiting writer */

    /* Set by shutdown() so anything blocked in claim_next_wait() gives
     * up instead of waiting out its full timeout on a pool that is
     * being torn down. */
    uint8_t         shutting_down;
} LargeWalSegmentPool;

/* ------------------------------------------------------------------
 * Lifecycle.
 *
 * large_wal_segment_pool_init: mkdir(wal_dir) if missing. For each of
 * the 4 slots: if large_wal_<i>.mydb doesn't exist, create +
 * posix_fallocate() to LARGE_WAL_SEGMENT_FILE_SIZE + write an initial
 * LSEG_FREE header + fdatasync; if it exists, open + validate + load its
 * header. Reseeds next_segment_no from whatever's on disk. Any slot
 * reloaded in LSEG_ACTIVE state is tail-scanned automatically (its
 * on-disk data_pages is stale by definition while active).
 * ------------------------------------------------------------------ */
int large_wal_segment_pool_init(LargeWalSegmentPool *pool, const char *wal_dir,
                                 uint32_t partition_id, LargeWalRegistry *registry);
int large_wal_segment_pool_shutdown(LargeWalSegmentPool *pool);

/* LSEG_FREE -> LSEG_ACTIVE: claims slot (next_segment_no %
 * LARGE_WAL_SEGMENT_POOL_SLOTS), stamps its segment_no, rewrites the
 * header. *out_slot_index names the claimed slot. Returns MYDB_ERR if
 * that slot isn't currently LSEG_FREE (expected once all 4 slots have
 * been claimed once and none has been freed yet — freeing is the
 * archive-section phase's job).
 *
 * Deliberately never fdatasyncs: either write()'s own trailing flush
 * covers this same fd moments later (the rollover path — the only
 * caller that matters for latency), or the first real write into this
 * segment covers it (a standalone claim), or a crash before either
 * safely reverts this slot to LSEG_FREE on reload (nothing was lost,
 * because nothing was written under this claim yet).
 *
 * Registers the new segment_no -> fd mapping in pool->registry (when
 * one is set). This belongs here rather than in callers because
 * claiming is exactly the moment a segment_no comes into existence, and
 * because write() calls claim_next itself during its own rollover — a
 * caller registering afterwards could only see the LAST segment a
 * multi-boundary write passed through, silently leaving the ones in
 * between unresolvable. The fd doesn't change (the 4 slot files are
 * opened once at init and held for the process's life); it's the *key*
 * that changes, since this slot now holds a different segment_no.
 *
 * Locking: takes pool->lock for the slot selection and header rewrite,
 * RELEASES it, and only then takes the registry's write lock to
 * register. The two must not nest, or they close a cycle against
 * copy_out, which runs registry -> node -> free_slot -> pool. Splitting
 * them is safe because between the two the segment is ACTIVE but
 * unregistered, and no index entry names it yet — so no reader can be
 * asking for it. */
int large_wal_segment_pool_claim_next(LargeWalSegmentPool *pool, uint32_t *out_slot_index);

/* Same as claim_next, but waits instead of failing when the target slot
 * isn't LSEG_FREE yet.
 *
 * Blocks on slot_free_cv until the archiver frees a slot, retrying each
 * time it is woken, and gives up with MYDB_ERR once timeout_ms has
 * passed (or immediately if the pool is shutting down). The wait is
 * bounded deliberately: with no archiver running -- or a broken one --
 * an unbounded wait would hang the writer forever with nothing to
 * indicate why.
 *
 * Why wait at all: a full pool clears itself within a few milliseconds,
 * because a copy-out is already in flight. Failing the write outright
 * turns that momentary condition into a failed user transaction. On
 * timeout this returns exactly what plain claim_next returns today, so
 * it can only ever do better, never worse.
 *
 * Only large_wal_segment_pool_write's rollover path uses this. Plain
 * claim_next stays for writer_init and for tests that want the
 * immediate answer.
 *
 * Holds no registry lock while waiting -- it never takes one at all,
 * and the caller (the rollover point inside write()) holds none either.
 * That is what leaves copy_out free to take reg -> node -> pool and
 * signal the condvar this is sleeping on. */
int large_wal_segment_pool_claim_next_wait(LargeWalSegmentPool *pool,
                                            uint32_t timeout_ms,
                                            uint32_t *out_slot_index);

/* Snapshots a slot's segment_no and state under pool->lock. Exists so
 * the archiver can decide what to do with a slot without reading
 * slots[].header behind the lock's back — it needs the segment_no to
 * find the registry node it must hold, and the lock order forbids
 * taking pool->lock once it does. Either out pointer may be NULL. */
int large_wal_segment_pool_slot_info(LargeWalSegmentPool *pool, uint32_t slot_index,
                                      uint64_t *out_segment_no, uint8_t *out_state);

/* Raw page I/O within an already-claimed slot. page_no is 1-based (0 is
 * the header's own page-slot) and must be < LARGE_WAL_SEGMENT_PAGES_PER_FILE.
 * No fsync here — infrastructure, not the durable path (that's the
 * writer-thread's concern). */
int large_wal_segment_pool_write_page(LargeWalSegmentPool *pool, uint32_t slot_index,
                                       uint32_t page_no, const uint8_t *buf);
int large_wal_segment_pool_read_page(LargeWalSegmentPool *pool, uint32_t slot_index,
                                      uint32_t page_no, uint8_t *out_buf);

/* LSEG_ACTIVE -> LSEG_DONE: stamps the caller-supplied final end_lsn/
 * data_pages, rewrites the header. Does NOT free the slot back to
 * LSEG_FREE or copy anything anywhere — that's the archive-section
 * phase's job (impl doc §10.1's copy-out step). A LSEG_DONE slot just
 * sits there, unusable for a new claim, until that phase exists.
 * Returns MYDB_ERR if the slot isn't currently LSEG_ACTIVE.
 *
 * worker == NULL: fdatasyncs synchronously before returning (today's
 * behavior). worker != NULL: hands the fdatasync to the worker and
 * returns immediately without waiting — this is the durability of a
 * segment that's about to be rolled away from, which write()'s
 * rollover branch can safely overlap with writing the new segment,
 * waiting on the worker only once that's also done. Callers that pass
 * a worker must eventually call wal_worker_wait() before relying on
 * this segment's DONE state being durable. */
int large_wal_segment_pool_mark_done(LargeWalSegmentPool *pool, WalWorker *worker,
                                     uint32_t slot_index, uint64_t end_lsn, uint32_t data_pages);

/* LSEG_DONE -> LSEG_FREE: called by the archiver only after the
 * holding-area copy's fsync has confirmed (impl doc §10.1's ordering
 * rule — a slot must never be marked FREE/reusable before that, or a
 * segment_no could transiently exist validly in two places).
 *
 * Also overwrites the slot's content pages with zeros, so the segment
 * about to be handed out doesn't inherit the previous one's pages —
 * they would otherwise still be there, valid magic and CRC intact, for
 * tail_scan to miscount. That happens strictly before the header flips
 * to LSEG_FREE, with its own fdatasync in between: see the source for
 * why the two writes must not share a flush.
 *
 * Zeroes segment_no/start_lsn/end_lsn/data_pages, rewrites + fdatasyncs
 * the header. Returns MYDB_ERR if the slot isn't currently LSEG_DONE.
 *
 * LOCKING CONTRACT: the caller must already hold this segment's registry
 * node lock (large_wal_registry_acquire) across the call. This function
 * takes pool->lock itself, but it cannot take the node lock — copy_out,
 * its only real caller, is already holding it so that freeing the slot
 * and repointing the registry at the holding-area copy look like one
 * atomic step to a reader. Taking it again here would self-deadlock.
 * Without that contract the zeroing below would race a large_wal_get
 * still preading this slot. */
int large_wal_segment_pool_free_slot(LargeWalSegmentPool *pool, uint32_t slot_index);

/* Scans page_no = 1, 2, ... in the given slot's file, validating each via
 * large_wal_page_header_deserialize, stopping at the first invalid/
 * unwritten page. *out_data_pages gets the count of valid pages found.
 * Called internally by large_wal_segment_pool_init() for any slot
 * reloaded in LSEG_ACTIVE state; exposed publicly so tests can exercise
 * it directly.
 *
 * "Stop at the first page that doesn't deserialize" is only a sound
 * definition of the tail BECAUSE free_slot zeroes a slot's content
 * before releasing it. A page left behind by a previous generation
 * deserializes perfectly well — same magic, same version, same
 * file_type, valid CRC — so without that zeroing this walks straight
 * past the live tail and reports the *older* segment's page count.
 * Anything that later gains its own free/reuse path (normal_wal's
 * segment pool, once the Normal WAL Archiver exists) has to zero on
 * release too, or inherits the same bug. */
int large_wal_segment_pool_tail_scan(LargeWalSegmentPool *pool, uint32_t slot_index,
                                      uint32_t *out_data_pages);

/* Reads a whole segment file's raw LARGE_WAL_SEGMENT_FILE_SIZE bytes
 * (header page-slot included) into out_buf in one call — mirrors
 * wal_segment_pool_read_segment. The eventual copy-out-to-holding-area
 * step (archive section phase, impl doc §10.1) needs exactly this kind
 * of whole-file read. out_buf must have room for
 * LARGE_WAL_SEGMENT_FILE_SIZE bytes. */
int large_wal_segment_pool_read_segment(LargeWalSegmentPool *pool, uint32_t slot_index,
                                         uint8_t *out_buf);

/* large_wal_segment_pool_write — mirrors wal_segment_pool_write exactly:
 * a blind byte mover, never parses or constructs page-header content —
 * the caller's buf already carries whatever headers/LSNs it needs (the
 * LARGE_WAL Writer thread builds these, the same way the Flusher builds
 * ring-buffer frames). Only knows page/segment *geometry* (PAGE_SIZE,
 * LARGE_WAL_SEGMENT_PAGES_PER_FILE).
 *
 * slot_index/page_no/offset are the caller's own cursor position,
 * purely positional — never derived from anything inside buf — passed
 * in and updated in place: offset is the exact byte position within
 * page_no's PAGE_SIZE slot to start writing at. On return,
 * slot_index/page_no/offset name where the *next* call should resume.
 *
 * Splits buf across as many pages as needed purely by byte count. When
 * offset reaches PAGE_SIZE, that page-slot is full: moves to the next
 * page_no, or — if that was the segment's last page-slot —
 * auto-finalizes the segment and claims the next one. The one
 * exception to "never reads header content": right before finalizing,
 * reads the just-filled last page back and copies its own end_lsn field
 * into mark_done()'s end_lsn argument — the only reliable source for a
 * segment's true highest LSN, since a page's start_lsn names only the
 * first record on it, not the last. Identical to normal_wal's own
 * rollover path, now that both subsystems share WalPageHeader (which is
 * exactly why that struct carries the start_lsn/end_lsn pair rather
 * than a single per-page LSN). A field copy, not content
 * interpretation. Returns MYDB_ERR if claim_next fails mid-write (pool
 * exhausted).
 *
 * worker: forwarded as-is into mark_done() if this call rolls over
 * (letting the old segment's fsync overlap with this call's own new-
 * segment writes) — may be NULL, in which case mark_done() falls back
 * to a synchronous fdatasync exactly like before this parameter existed.
 *
 * TEMPORARY: fdatasyncs before returning on every call — same reasoning
 * and removal condition as wal_segment_pool_write's own temporary
 * fdatasync (design doc §11: large_wal_writer exists now, but each
 * submit() is still a fully synchronous, single-record handoff — no
 * group-commit batching yet to fold this flush into). fdatasync, not
 * fsync: these files are posix_fallocate'd to a fixed size once and
 * never resized, so there's no essential size metadata for fdatasync to
 * need to flush beyond the data itself. This trailing fsync always
 * stays synchronous on the calling thread regardless of worker — it's
 * the last thing this call does, so there's nothing left to overlap it
 * with; only mark_done()'s fsync (a different file) benefits from being
 * offloaded. If worker was given, waits on it (wal_worker_wait — a
 * cheap no-op if this call never rolled over) before returning, so a
 * caller with a worker still gets the same "fully durable by the time
 * this returns" guarantee as the no-worker path. One line to remove
 * once submit() batches multiple pending records behind one flush. */
int large_wal_segment_pool_write(LargeWalSegmentPool *pool, WalWorker *worker,
                                  uint32_t *slot_index, uint32_t *page_no, uint32_t *offset,
                                  const uint8_t *buf, size_t buf_len);

#endif /* LARGE_WAL_SEGMENT_POOL_H */
