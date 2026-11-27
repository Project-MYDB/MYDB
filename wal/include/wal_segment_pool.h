#ifndef WAL_SEGMENT_POOL_H
#define WAL_SEGMENT_POOL_H

#include <stddef.h>
#include <stdint.h>
#include "common.h"
#include "wal_segment.h"
#include "wal_page.h"

/*
 * wal_segment_pool.h — the WAL_SEGMENT_POOL_SLOTS segment files of one
 * partition's log (wal_<i>.mydb in wal_dir): creating them, claiming the
 * next one, I/O within one, and finding and cutting the end of the log at
 * start-up. Segment N always lives in slot N % WAL_SEGMENT_POOL_SLOTS.
 * Callers use only these functions, never the slot fields.
 */

#define WAL_SEGMENT_POOL_SLOTS      20
/* A file is one 4KB header page, then 16MB of log. */
#define WAL_SEGMENT_FILE_SIZE       (WAL_PAGE_SIZE + WAL_SEG_SIZE)
#define WAL_SEGMENT_PAGES_PER_FILE  (WAL_SEGMENT_FILE_SIZE / WAL_PAGE_SIZE) /* 4097 */

/* One segment file: its fd, open for the process's lifetime, and a copy
 * of its header. */
typedef struct {
    uint8_t           slot_no;    /* 0..WAL_SEGMENT_POOL_SLOTS-1 */
    int               fd;
    WalSegmentHeader  header;
} WalSlot;

/* The pool. next_segment_no is the number the next claim takes; segment
 * numbers are part of the LSN, so it never goes back. */
typedef struct {
    char      wal_dir[256];
    uint32_t  partition_id;
    uint64_t  next_segment_no;
    WalSlot   slots[WAL_SEGMENT_POOL_SLOTS];
} WalSegmentPool;

/* Creates wal_dir if missing, then opens every slot's file, or creates it
 * (fallocated, FREE header, synced). Sets next_segment_no one past the
 * highest segment ever claimed. */
int wal_segment_pool_init(WalSegmentPool *pool, const char *wal_dir, uint32_t partition_id);

/* Closes every file. */
int wal_segment_pool_shutdown(WalSegmentPool *pool);

/* Claims segment next_segment_no into its slot: FREE -> ACTIVE, header
 * rewritten. *out_slot_index names the slot. Returns MYDB_ERR if that
 * slot is not FREE. No fdatasync: the next sync of this file covers the
 * header, and after a crash before it the slot just reads as FREE, with
 * nothing written in it. */
int wal_segment_pool_claim_next(WalSegmentPool *pool, uint32_t *out_slot_index);

/* Writes or reads one whole log page of a slot. page_no is the file page,
 * 1..4096 (0 is the header). No fsync. Used at start-up and by tests. */
int wal_segment_pool_write_page(WalSegmentPool *pool, uint32_t slot_index,
                                 uint32_t page_no, const uint8_t *buf);
int wal_segment_pool_read_page(WalSegmentPool *pool, uint32_t slot_index,
                                uint32_t page_no, uint8_t *out_buf);

/* Writes len bytes of log from lsn at file offset 4096 + (lsn &
 * WAL_SEG_MASK), with one pwrite and no fsync. The slot must be ACTIVE
 * and hold lsn's segment, and the bytes must end within that segment.
 * len == 0 writes nothing. Returns MYDB_ERR on bad arguments or a failed
 * or short write. */
int wal_segment_pool_pwrite(WalSegmentPool *pool, uint32_t slot_index,
                            uint64_t lsn, const uint8_t *buf, size_t len);

/* The segment number slot_index holds. */
uint64_t wal_segment_pool_segment_no(const WalSegmentPool *pool, uint32_t slot_index);

/* fdatasync of the file holding segment seg. Reads only the slot's fd,
 * no header, so the commit path can call it while a write step rewrites
 * headers. The caller makes sure seg is claimed. */
int wal_segment_pool_fdatasync_segment(WalSegmentPool *pool, uint64_t seg);

/* Finishes slot_index's segment: ACTIVE -> DONE with end_lsn and
 * data_pages, header rewritten. No fsync: the caller syncs the file (the
 * write step hands it to the WalWorker). Returns MYDB_ERR if the slot is
 * not ACTIVE or the write fails. */
int wal_segment_pool_mark_done(WalSegmentPool *pool, uint32_t slot_index,
                               uint64_t end_lsn, uint32_t data_pages);

/* Finds the end of the log. The active segment is the ACTIVE slot with
 * the highest segment_no; the walk starts at the first record of the
 * segment before it, when a slot still holds that one, because the end
 * can lie there (a torn record across the boundary). It walks records
 * until the first that does not check out: every page it touches valid
 * with its own start_lsn, continued pages flagged INCOMING, its lsn field
 * its own position, total_len sane, CRC valid.
 *
 * *out_end_lsn is just past the last valid record (a page end stays a
 * page end); with no record at all, where the first would start.
 * *out_slot is the slot of the segment holding that end; an end exactly
 * at the active segment's end stays with the active slot.
 *
 * Returns MYDB_OK, MYDB_ERR_NOT_FOUND when no slot is ACTIVE (a new log),
 * or MYDB_ERR when a read or an allocation fails. */
int wal_segment_pool_tail_scan(WalSegmentPool *pool, uint32_t *out_slot,
                                uint64_t *out_end_lsn);

/* Cuts the log at end_lsn, the end tail_scan found in slot_index's
 * segment, so nothing after it can pass for log: every later segment is
 * zeroed and made FREE with its number cleared; slot_index's segment is
 * ACTIVE again and zeroed after end_lsn; every claimed file is synced;
 * next_segment_no becomes the segment after slot_index's. The zeroing
 * stops intact records written after a torn one from being taken for log
 * by a later scan. Returns MYDB_OK, or MYDB_ERR (bad arguments, a FREE
 * slot, I/O). */
int wal_segment_pool_truncate(WalSegmentPool *pool, uint32_t slot_index, uint64_t end_lsn);

#endif /* WAL_SEGMENT_POOL_H */
