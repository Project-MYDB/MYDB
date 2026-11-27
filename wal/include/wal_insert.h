#ifndef WAL_INSERT_H
#define WAL_INSERT_H

#include <stdint.h>
#include "common.h"
#include "wal_manager.h"
#include "wal_page.h"

/*
 * wal_insert.h — the insert path (V2 §9, §10): a session thread puts its
 * buffer of whole records into the ring with no lock. wal_insert
 * reserves space with one fetch_add on current_pos, fills the records'
 * headers, then for each piece (the part of the buffer that goes in one
 * ring frame) waits for ring space, copies it, and publishes it in the
 * block counters the write step reads.
 */

/* Reserves len record bytes and returns the position of the first one
 * (record bytes before it in the whole log, page headers not counted).
 * One fetch_add; it never fails and is never undone. */
uint64_t wal_insert_reserve(WalManager *wm, uint32_t len);

/* Fills the header of every record in buf, which holds whole records of
 * one transaction back to back from position byte_pos, and returns the
 * last record's LSN. The caller has set txn_id, total_len, table_id,
 * page_no, rec_type and reserved, and put the body's running CRC
 * (wal_record_header_pack_crc) in checksum. This sets:
 *
 *   lsn       where the record lands;
 *   prev_lsn  the record before it in buf; for the first, prev_lsn (the
 *             transaction's last LSN, 0 if none);
 *   flags     CONTINUES_ON_NEW_PAGE if it runs past its page's end;
 *   checksum  finished with the header.
 *
 * Call it once per buffer: it replaces the body CRCs. */
uint64_t wal_insert_fill_headers(uint8_t *buf, uint32_t len,
                                 uint64_t byte_pos, uint64_t prev_lsn);

/* One piece: the part of the caller's buffer that lands in one ring
 * frame, with what the copy needs worked out. A piece starting at its
 * page's offset 24 also writes the page header. Each counter add is the
 * piece's bytes in one 512-byte block; the 24 header bytes count in the
 * page's first block. */
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

/* Plans into *p the piece of buf starting at buffer offset off, LSN lsn,
 * up to its page's end or buf's end. buf holds whole records with headers
 * filled; lsn is a record position. Call with off = 0 first, then with
 * the same *p for each next piece. buf must not change until the last
 * piece is copied. */
void wal_insert_plan(const WalManager *wm, WalByteInsert *p, uint64_t lsn,
                     const uint8_t *buf, uint32_t len, uint32_t off);

/* Copies a planned piece into its frame (page header first, if any) and
 * adds its bytes to the counters with release, so whoever reads a counter
 * with acquire sees the bytes. The caller has checked for space. */
void wal_insert_copy(WalManager *wm, const WalByteInsert *p);

/* Returns once a piece ending at end_lsn can be copied: its blocks were
 * written out a ring earlier, i.e. end_lsn rounded up to its block end
 * minus the ring's size is <= write_lsn (V2 §9.3).
 *
 * *write_lsn_cached is the session's copy of write_lsn, checked first;
 * write_lsn is read only when the copy is too low. It must never exceed
 * this WalManager's write_lsn: 0 when the session starts.
 *
 * Without space, it writes the ring out itself (wal_flush_space_wait).
 * A failed write there aborts: the reservation can't be undone, so the
 * log could not go on past it. */
void wal_insert_space_check(WalManager *wm, uint64_t end_lsn, uint64_t *write_lsn_cached);

/* Inserts buf's records (as wal_insert_fill_headers takes them) and
 * returns with their headers filled in buf, which may be reused at once.
 *
 *   prev_lsn          the transaction's last LSN, 0 if none
 *   write_lsn_cached  the session's copy of write_lsn, updated
 *   out_last_lsn      the last record's LSN, the next call's prev_lsn
 *   out_end_lsn       just past the last byte: what a commit must flush
 *                     (a page end stays a page end)
 *
 * The records are in the ring, not yet durable. Returns MYDB_ERR, with
 * nothing reserved, for a NULL argument or len below one record header;
 * otherwise MYDB_OK. */
int wal_insert(WalManager *wm, uint8_t *buf, uint32_t len, uint64_t prev_lsn,
               uint64_t *write_lsn_cached, uint64_t *out_last_lsn, uint64_t *out_end_lsn);

#endif /* WAL_INSERT_H */
