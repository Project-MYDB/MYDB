#ifndef LARGE_WAL_WRITER_H
#define LARGE_WAL_WRITER_H

#include <pthread.h>
#include <stdint.h>
#include "common.h"
#include "large_wal/large_wal_segment_pool.h"
#include "large_wal/large_wal_registry.h"
#include "large_wal/large_wal_index.h"
#include "large_wal/large_wal_state.h"
#include "large_wal/large_wal_buffer.h"
#include "wal_worker.h"

/*
 * large_wal_writer.h — the LARGE_WAL Writer thread (MYDB_WAL_
 * IMPLEMENTATION.md Appendix A: "single dedicated thread, packs content
 * into rotation segments, fsyncs, advances large_wal_flush_lsn")
 *
 * Owns its LargeWalBuffer (per §10.10 — "the staging area the LARGE_WAL
 * Writer packs content into") and its own live cursor
 * (cur_slot_index/cur_page_no), the same "caller owns and persists the
 * cursor across calls" pattern wal_segment_pool_write's own params and
 * WalFlusher's buf_cursor/seg_cursor already use.
 *
 * submit() is a blocking request/response handoff over one mutex+
 * condvar, not a general async queue — the engine's whole execution
 * model is single-statement-in-flight (CLAUDE.md's Concurrency model),
 * and §10.7's Commit Wait Rule is phrased as a wait, not a callback, so
 * at most one large-record write is ever in flight system-wide. This
 * also means no separate large_wal_flush_lsn query API is needed: if
 * submit() returns MYDB_OK, large_wal_flush_lsn has already advanced to
 * at least this record's content_lsn (persisted+fsynced before submit()
 * returns).
 *
 * Per-submit sequence (do_write, large_wal_writer.c): segment-fit check
 * first (roll over before packing, never mid-buffer, closing Phase 4's
 * "segment-fit" open item) -> pack fully into the owned buffer,
 * stamping real page headers (closing Phase 4's "header
 * stamping" open item, since the writer -- unlike the buffer alone --
 * knows exactly where these pages are landing) -> one byte-for-byte
 * large_wal_segment_pool_write() call, reused as-is -> index insert ->
 * large_wal_state advance.
 *
 * Also registers every segment it claims with the registry
 * (large_wal_registry_register, large_wal_registry.h) immediately,
 * widening that registry to cover a segment's whole life instead of
 * just its post-copy_out residency (Phase 3's deferred "Structural
 * consequence"). Registers with owns_fd=0 — a rotation slot's fd is
 * the pool's own, closed by the pool's shutdown, never this registry's.
 * Never touches large_wal_archiver at all — the writer only ever
 * needed register(), not anything archiver-specific, so it depends on
 * the registry directly rather than the whole archiver module.
 *
 * Still standalone: dependencies (pool/registry/idx/state) are caller-
 * supplied, not constructed here, matching every phase so far. No
 * wal_manager/large_wal_manager/TxnManager wiring, no per-segment_no
 * locking (see this phase's plan, "Concurrency" section) — this phase's
 * own tests never have a concurrent large_wal_get() caller racing an
 * in-flight submit().
 */

typedef struct {
    LargeWalSegmentPool      *pool;      /* not owned */
    LargeWalRegistry         *registry; /* not owned */
    LargeWalIndex            *idx;    /* not owned */
    LargeWalState            *state; /* not owned */
    WalWorker                *worker; /* not owned; may be NULL — see large_wal_writer_init */

    LargeWalBuffer  buf;   /* OWNED — embedded, per §10.10 */

    /* Live append cursor, seeded at init() from whatever's already
     * LSEG_ACTIVE (or a fresh claim if none) — Phase 2's own
     * crash-reload tail-scan already makes data_pages accurate
     * post-crash, so no new recovery logic is needed here. */
    uint32_t  cur_slot_index;
    uint32_t  cur_page_no;
    uint32_t  cur_offset;             /* bytes already used in cur_page_no's
                                          content area (0 .. LARGE_WAL_PAGE_
                                          USABLE-1). Non-zero means the next
                                          record packs in behind the last
                                          one on that same page rather than
                                          starting a fresh page — the whole
                                          point of tight packing, and what
                                          gives LargeWalIndexEntry.offset a
                                          real value. */
    uint64_t  cur_segment_last_lsn;   /* content_lsn of the last record
                                          written into cur_slot_index —
                                          used as end_lsn if a future
                                          submit closes this segment
                                          early via manual rollover */

    pthread_t        thread;
    pthread_mutex_t  lock;
    pthread_cond_t   cond;
    uint8_t          started;
    uint8_t          stop_requested;

    /* single-slot request/response mailbox — content and out_entries are
     * borrowed pointers, valid for the call's duration since submit()
     * blocks */
    uint8_t                       request_pending;
    uint8_t                       request_done;
    int                           last_result;
    const uint8_t                *req_content;
    uint32_t                      req_total_size;
    LargeWalIndexEntry           *req_out_entries;
    uint32_t                       req_out_cap;
    uint32_t                       req_out_count;
} LargeWalWriter;

/* Seeds cur_slot_index/cur_page_no from whatever segment is already
 * LSEG_ACTIVE in pool (claiming a fresh one via claim_next() if none
 * is), and registers that segment with registry either way — necessary
 * because LargeWalRegistry is in-memory only and starts empty every
 * process start. Does not start the thread. worker (may be NULL) is
 * stored and forwarded into every large_wal_segment_pool_write()/
 * mark_done() call this writer makes — see large_wal_segment_pool.h's
 * own worker parameter doc comments. */
int large_wal_writer_init(LargeWalWriter *writer, LargeWalSegmentPool *pool,
                           LargeWalRegistry *registry, LargeWalIndex *idx,
                           LargeWalState *state, WalWorker *worker);

/* Starts the background thread. init() must have succeeded first. */
int large_wal_writer_start(LargeWalWriter *writer);

/* Signals stop, joins the thread. Safe to call even if start() was
 * never called, or after an earlier stop() — a no-op in both cases. */
int large_wal_writer_stop(LargeWalWriter *writer);

/* Blocking. Wakes the thread, waits for this batch to be packed,
 * written, indexed, and durably flush_lsn-advanced before returning.
 * Fails immediately (no hang) if the writer isn't started or is
 * stopping.
 *
 * content is a blob of one or more back-to-back serialized records; the
 * writer discovers their boundaries itself by walking each
 * WalRecordHeader's own total_len, and reads each record's LSN and type
 * straight out of that header. No descriptor array is needed — the
 * writer has to parse those headers anyway to set the continuation
 * flag, and a caller-supplied description could only ever duplicate (or
 * contradict) what the blob already carries.
 *
 * out_entries receives one LargeWalIndexEntry per record, in blob
 * order; out_cap is its length. *out_count reports how many records
 * were written — and is meaningful on failure too: a batch spanning
 * several segments is written run by run, so a mid-batch failure leaves
 * the earlier records genuinely durable and indexed, and *out_count
 * says exactly how many. */
int large_wal_writer_submit(LargeWalWriter *writer, const uint8_t *content, uint32_t total_size,
                             LargeWalIndexEntry *out_entries, uint32_t out_cap,
                             uint32_t *out_count);

#endif /* LARGE_WAL_WRITER_H */
