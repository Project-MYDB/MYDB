#ifndef WAL_FRAME_STATE_H
#define WAL_FRAME_STATE_H

#include <stdatomic.h>
#include <stdint.h>
#include "common.h"

/*
 * wal_frame_state.h — the ring's block counters,
 * the insert path (wal_insert) adds to the counters, the write step
 * (wal_flush) reads and zeroes them, and WalManager holds the array.
 */

/* ------------------------------------------------------------------
 * Block counters (V2 §10). The ring is split into 512-byte blocks, and
 * each block has one counter: copiers add the bytes they copied into it
 * (page header bytes included, in block 0 of each frame), so a block
 * that reads WAL_BLOCK_SIZE is fully copied. The write step reads them to
 * find how far the ring is complete without any lock on the insert path.
 *
 * One flat array, counter i for the i-th block of the ring, indexed
 * straight from the LSN: (lsn >> WAL_BLOCK_SHIFT) & ctr_mask, where
 * ctr_mask = ring frames * 8 - 1. Each counter is padded to 16 bytes, so
 * a 64-byte cache line holds 4 (2 KB of the ring) and a frame's 8 take
 * two lines; with the array 64-byte aligned, copiers working in
 * different 2 KB halves of the ring never share a line. V2 §10.1 draws
 * this as three nested structs (BlockSlot in ChunkLine in FrameState);
 * they are the same bytes in the same order, so a flat array of
 * WalBlockCounter is used instead.
 * ------------------------------------------------------------------ */
#define WAL_BLOCK_SHIFT  9
#define WAL_BLOCK_SIZE   (1u << WAL_BLOCK_SHIFT)              /* 512 */
#define WAL_FRAME_BLOCKS (WAL_PAGE_SIZE / WAL_BLOCK_SIZE)     /* 8 per 4KB frame */

typedef struct {
    _Atomic uint16_t done;   /* bytes copied into this block, 0..512 */
    uint8_t          pad[14];
} WalBlockCounter;

_Static_assert(sizeof(WalBlockCounter) == 16, "4 counters per 64-byte cache line");

/* The counter of the block holding lsn. */
static inline _Atomic uint16_t *wal_block_counter(WalBlockCounter *counters,
                                                  uint32_t ctr_mask, uint64_t lsn)
{
    return &counters[(lsn >> WAL_BLOCK_SHIFT) & ctr_mask].done;
}

#endif /* WAL_FRAME_STATE_H */
