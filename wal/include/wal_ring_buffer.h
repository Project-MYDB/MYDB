#ifndef WAL_RING_BUFFER_H
#define WAL_RING_BUFFER_H

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include "common.h"
#include "wal_types.h"
#include "wal_page.h"
#include "wal_segment_pool.h"

/*
 * wal_ring_buffer.h — the 16MB WAL ring buffer and txn_append_record()
 * (MYDB_WAL_DESIGN.md §2/§5/§7, MYDB_WAL_IMPLEMENTATION.md §8.6).
 *
 * Ring frames are byte-identical to segment pages (WalPageHeader +
 * packed records) — the same shape wal_segment_pool already uses, just
 * living in memory instead of a file. Records span frames, segment pages
 * and segments: a record is copied byte by byte, and when it reaches the
 * end of a frame the rest continues in the next one. There is no padding.
 *
 * The LSN is a position in the log (wal_page.h): segment_no * 2MB +
 * offset in the segment file, page headers and segment page 0 included.
 * So current_lsn alone says where the next byte goes — which data page
 * (and therefore which frame: data page k lives in frame k %
 * WAL_RING_BUFFER_FRAMES) and which offset in it. A frame's page header
 * lives only in the frame and is written once, when the frame is opened.
 *
 * The LSN is assigned here, inside append, under rb->lock, because
 * handing out the position and copying the bytes there must be one step.
 * No ATT/DPT here — that's TransactionManager's domain
 * (partition_manager/include/transaction.h), wired in later once this
 * module gets integrated. prev_lsn chaining is therefore the caller's
 * own responsibility: the caller fills in hdr->prev_lsn/txn_id/table_id/
 * page_no/rec_type before calling append(); this module only assigns
 * lsn/total_len/flags/checksum, and hands back where the record starts
 * and ends.
 *
 * Draining: the ring is a circular queue with two ends. current_lsn is
 * the writer's end; drained_lsn is the reader's end — how far the
 * Flusher has copied pages out (wal_ring_buffer_snapshot). The full
 * check reads drained_lsn under the ring's own lock, and a frame is free
 * once copied out, before its fsync. Durability (flush_lsn) belongs to
 * the Flusher (wal_flusher.h); the ring knows nothing about it.
 *
 * Interface discipline: same as WalSegmentPool (see wal_segment_pool.h)
 * — transparent struct, but external callers go through the functions
 * below only.
 */

#define WAL_RING_BUFFER_SIZE   (16 * 1024 * 1024)                     /* 16MB */
#define WAL_RING_BUFFER_FRAMES (WAL_RING_BUFFER_SIZE / WAL_PAGE_SIZE)  /* 4096 */

/* WalCursor — design doc §7. One shape, reused for both the ring-buffer-
 * side and segment-side position (the design doc's own choice: "WalCursor
 * buf_cursor; WalCursor seg_cursor;"). Defined here rather than in
 * wal_flusher.h specifically so wal_ring_buffer_snapshot can report one
 * without a circular include (wal_flusher.h already includes this header
 * to get WalRingBuffer; the reverse can't also be true). */
typedef struct {
    uint64_t lsn;
    uint32_t page_no;
    uint16_t offset;
} WalCursor;

typedef struct {
    uint8_t       *buf;              /* heap-allocated WAL_RING_BUFFER_SIZE bytes */
    uint64_t       current_lsn;      /* writer's end: where the next byte goes — a
                                        position in the log, never persisted. A data
                                        position (offset >= 24, its frame open) or
                                        exactly a page end (offset 0: that page filled
                                        exactly, the next frame is opened only when
                                        the next record arrives) */
    uint64_t       drained_lsn;      /* reader's end: base LSN of the first page not
                                        yet copied out by snapshot(). Moves in whole
                                        pages, and only past pages that were closed
                                        when copied */

    pthread_mutex_t lock;         /* protects current_lsn/drained_lsn and buf */
    pthread_cond_t  append_wait_cond;   /* paired with lock; broadcast when drained_lsn
                                        moves, for wal_ring_buffer_append_wait() */
} WalRingBuffer;

/* buf is malloc'd (WAL_RING_BUFFER_SIZE bytes) and zeroed; lock/append_wait_cond
 * initialized. start_lsn is where the log continues — a data position,
 * e.g. wal_segment_first_lsn(seg) for a freshly claimed segment. Its frame
 * is opened (header written in place: start_lsn = the page's base,
 * continuation_len 0, flags 0) and drained_lsn starts at that page's
 * base. */
int  wal_ring_buffer_init(WalRingBuffer *rb, uint64_t start_lsn);
void wal_ring_buffer_shutdown(WalRingBuffer *rb);

/* Assigns lsn = current_lsn (stepped to the next page's first data byte
 * first if current_lsn sits on a page end). Stamps hdr->lsn/
 * total_len/checksum, and sets WAL_RECORD_FLAG_CONTINUES_ON_NEW_PAGE in
 * hdr->flags when the record's last byte lands on a later page than its
 * first (clearing it otherwise). Packs the 44-byte header
 * (wal_record_header_pack) and copies header then body across as many
 * frames as they need. When a frame fills and record bytes are left, it
 * opens the next frame (start_lsn = its base, continuation_len = record
 * bytes still to come up to a full page, WAL_PAGE_INCOMING_CONTINUATION),
 * stepping over segment page 0 when it crosses into a new segment. A
 * filled frame needs no update: its header was complete when opened. If
 * the record ends exactly at a frame end, the next frame is opened by
 * the next append. The whole body runs under rb->lock.
 *
 * hdr->prev_lsn/txn_id/table_id/page_no/rec_type are the caller's
 * responsibility — not derived here (no ATT this phase).
 *
 * Full check (exact, in frames): the record may use frames up to the one
 * holding its last byte, and that must not reach the frame still holding
 * the first un-drained page: data_page(last byte) - data_page(drained_lsn)
 * < WAL_RING_BUFFER_FRAMES. Otherwise MYDB_ERR_FULL and nothing is
 * written — the caller wakes the Flusher and waits
 * (wal_ring_buffer_append_wait; wal_manager_append does both).
 *
 * Known gap: a record bigger than the ring can
 * never pass the check, so a caller that waits for space would wait
 * forever. No real record comes close (a row record is under 4KB).
 *
 * On success *out_lsn = where the record starts, *out_end_lsn = just past
 * its last byte (wal_lsn_advance); a record is durable once the Flusher's
 * flush_lsn >= *out_end_lsn. Returns MYDB_OK, MYDB_ERR_FULL, or MYDB_ERR
 * on bad arguments. */
int wal_ring_buffer_append(WalRingBuffer *rb, WalRecordHeader *hdr,
                            const void *body, size_t body_len,
                            uint64_t *out_lsn, uint64_t *out_end_lsn);

/* Blocks on append_wait_cond until a record of total_len bytes (header
 * included) would pass append's full check from the current position.
 * Does not reserve anything: another appender may take the space first,
 * so callers loop (append -> FULL -> wake Flusher -> append_wait -> append).
 * Kept separate from append so the ring never needs to know the Flusher. */
int wal_ring_buffer_append_wait(WalRingBuffer *rb, size_t total_len);

/* The Flusher's snapshot + wal_segment_pool_write is the one way pages
 * leave the ring.
 *
 * Locks rb->lock, memcpy's every closed frame — from the page at
 * drained_lsn up to, but not including, the open frame (or up to
 * current_lsn when it sits on a page end, since then the last frame is
 * closed too) — into scratch (caller-owned, must hold >=
 * scratch_frame_cap * WAL_PAGE_SIZE bytes), wrapping around the ring's
 * physical end in two memcpy's. Advances drained_lsn past what was
 * copied, broadcasts append_wait_cond, unlocks. Does NOT touch durability —
 * this finishes before any I/O happens; the Flusher marks pages durable
 * after its own write + fdatasync (wal_flusher.h).
 *
 * Sets *out_num_frames (0 if nothing closed since drained_lsn) and
 * *out_cursor — lsn = end of the last copied page (its start_lsn +
 * WAL_PAGE_SIZE), page_no = the ring frame drained_lsn now points at,
 * offset = 0 (only whole closed frames are copied; copying the open frame
 * early is the Flusher piece's job). A real Flusher stores whatever this
 * reports as its own buf_cursor (design doc §7). */
int wal_ring_buffer_snapshot(WalRingBuffer *rb,
                              uint8_t *scratch, uint32_t scratch_frame_cap,
                              uint32_t *out_num_frames, WalCursor *out_cursor);

#endif /* WAL_RING_BUFFER_H */
