#ifndef WAL_INSERT_H
#define WAL_INSERT_H

#include <stdint.h>
#include "common.h"
#include "wal_manager.h"
#include "wal_page.h"

/*
 * wal_insert.h — the insert path (MYDB_WAL_DESIGN_V2.md §9, §10): how a
 * thread puts its records into the ring with no lock. The thread hands
 * over its local buffer of whole records and:
 *
 *   1. reserves space with one fetch_add on current_pos (wal_insert_reserve);
 *   2. converts the position to an LSN;
 *   3. fills the records' headers (lsn, prev_lsn, flags, checksum) in
 *      its buffer (wal_insert_fill_headers);
 *   4. plans one piece, the part of the buffer that goes in one frame
 *      (wal_insert_plan);
 *   5. waits until the piece's blocks in the ring are free
 *      (wal_insert_space_check), copies that piece into the ring and
 *      publishes it in the block
 *      counters (wal_insert_copy), which the write step (wal_flush) reads
 *      to find how far the ring is complete; then plans the next piece,
 *      until the buffer is done:

 */

/* Reserves len bytes of the log for the caller's buffer and returns the
 * byte position of its first one: current_pos's record bytes, page
 * headers not counted
 *
 * One fetch_add, no lock, always succeeds, even with the ring full
 * (the wait is the space check's, before each copy), and is never undone
 */
uint64_t wal_insert_reserve(WalManager *wm, uint32_t len);


/* Fills the header of every record in buf, and returns the
 * last record's LSN, the transaction's new last LSN.
 *
 * buf holds whole records back to back, all of one transaction (one
 * thread per session), len their total. The caller has set each header's
 * txn_id, total_len, table_id, page_no, rec_type and reserved, and put
 * the body_checksum (wal_record_header_pack_crc) in the checksum field;
 * any WAL_RECORD_FLAG_CONTINUES_ON_NEW_PAGE it left is cleared here. This
 * fills:
 *
 *   lsn       where the record lands;
 *   prev_lsn  the record before it in buf; for the first, the prev_lsn
 *             argument (the transaction's last LSN, 0 if it has none);
 *   flags     CONTINUES_ON_NEW_PAGE if it runs past its page's end (not
 *             set for one that ends exactly on it);
 *   checksum  body_checksum finished with the header.
 *
 * Call it once per buffer: it replaces each body_checksum with the
 * finished checksum. */
uint64_t wal_insert_fill_headers(uint8_t *buf, uint32_t len,
                                 uint64_t byte_pos, uint64_t prev_lsn);

/* One piece: the part of the caller's buffer that lands in one frame,
 * with everything the copy needs worked out, plus the record boundaries
 * the next call to wal_insert_plan continues from.
 *
 * The page header is planned only for a piece that starts at its page's
 * first record byte (offset 24): that copier writes the header (V2
 * §9.4). continuation_len is the bytes, from the page's first record
 * byte, that continue a record which began on an earlier page, at most
 * 4072; 0 with no flag when a record starts right there.
 *
 * The counters are one add per 512-byte block the piece touches, at most
 * WAL_FRAME_BLOCKS, of the bytes it writes there; the 24 header bytes
 * count in block 0 (V2 §10.3), so every block completes at exactly 512. */
typedef struct {
    /* what this piece writes */
    uint64_t       lsn;          /* its first record byte (page offset >= 24) */
    const uint8_t *src;          /* its bytes, inside the caller's buffer     */
    uint32_t       len;          /* bytes, never past the page's end          */
    uint32_t       frame;        /* ring frame of lsn                         */
    uint8_t        has_page_hdr; /* piece starts at offset 24: write page_hdr */
    uint8_t        n_ctr;        /* entries used in ctr_idx / ctr_add         */
    uint8_t        page_hdr[WAL_PAGE_HEADER_SIZE];   /* serialized, CRC done  */
    uint32_t       ctr_idx[WAL_FRAME_BLOCKS];        /* counter index         */
    uint16_t       ctr_add[WAL_FRAME_BLOCKS];        /* bytes to add to it    */

    /* carried from one wal_insert_plan call to the next */
    uint32_t       rec_start;    /* buffer offset of the record holding the
                                    piece's first byte                       */
    uint32_t       rec_end;      /* and of that record's end                 */
} WalByteInsert;

/* Plans into *p the piece of buf that starts at buffer offset off, LSN
 * lsn: from there to the page's end, or to the buffer's end if that
 * comes first.
 *
 * buf holds whole records back to back, headers filled (total_len read
 * from them, so the plan knows where each record ends), first byte a
 * record's first byte; len >= WAL_RECORD_HEADER_SIZE, off < len. lsn is
 * a record position (page offset >= 24), as wal_byte_to_lsn returns.
 *
 * Call it with off = 0 first, then for each next piece with the same *p
 * (off = 0 starts over; later calls continue from the record boundaries
 * the previous call left in *p). buf must stay unchanged until the last
 * piece is copied: pieces point into it. */
void wal_insert_plan(const WalManager *wm, WalByteInsert *p, uint64_t lsn,
                     const uint8_t *buf, uint32_t len, uint32_t off);

/* Copies a planned piece into its ring frame (the page header first,
 * if it has one) and publishes it: each counter add is a release, so
 * the bytes are visible to whoever reads the counter with acquire (the
 * write step). The caller makes sure the frame is free first
 * (wal_insert_space_check). */
void wal_insert_copy(WalManager *wm, const WalByteInsert *p);

/* Waits until a piece ending at end_lsn can be copied: the ring's frames
 * are reused every ring length, so what one lap earlier was in the blocks
 * the piece touches must be written out first (V2 §9.3):
 *
 *   space_required = end_lsn rounded up to its 512-byte block end
 *   free when  space_required - ring_size_bytes <= write_lsn
 *
 * The block, not the 2 KB line: the write step zeroes each block it has
 * fully written before publishing write_lsn past it, and never the tail
 * block it stopped inside.
 *
 * write_lsn_cached is the session's own copy of write_lsn, as PostgreSQL's
 * per-backend LogwrtResult: compared first, so the normal case reads no
 * shared memory, and reloaded (acquire) only when it is too low. It must
 * never be above this WalManager's write_lsn: the session sets it to 0
 * when it opens on the partition and never carries it to another
 * WalManager. Too low only costs a reload.
 *
 * Until the write step exists the wait only watches write_lsn (spin,
 * yield, sleep), so something else must move it; the flush piece puts
 * the write step itself here. */
void wal_insert_space_check(WalManager *wm, uint64_t end_lsn, uint64_t *write_lsn_cached);

/* The insert: puts buf's records in the ring and publishes them, by the
 * five steps at the top of this file. buf is as wal_insert_fill_headers
 * takes it and comes back with its headers filled, so the caller can read
 * each record's LSN from it; it can be reused as soon as this returns.
 *
 *   prev_lsn      the transaction's last LSN, 0 if it has none.
 *   write_lsn_cached  the session's copy of write_lsn, 0 when the session
 *                 opened (wal_insert_space_check); updated in place.
 *   out_last_lsn  the last record's LSN: the transaction's new last LSN,
 *                 the prev_lsn of its next call.
 *   out_end_lsn   just past the last byte: what a commit needs flushed. A
 *                 page end stays that page end, as in wal_lsn_advance.
 *
 * In the ring is not durable: that is the flush path's job.
 *
 * Returns MYDB_ERR, with nothing reserved, for a NULL argument or a len
 * shorter than one record header, otherwise MYDB_OK: after the
 * reservation there is no way back (V2 §9.1).
 *
 * Before each piece it waits for room (wal_insert_space_check). Until the
 * write step exists nothing in the engine moves write_lsn, so a caller
 * that fills the ring waits until something does. */
int wal_insert(WalManager *wm, uint8_t *buf, uint32_t len, uint64_t prev_lsn,
               uint64_t *write_lsn_cached, uint64_t *out_last_lsn, uint64_t *out_end_lsn);

#endif /* WAL_INSERT_H */
