#ifndef WAL_FRAME_STATE_H
#define WAL_FRAME_STATE_H

#include <stdatomic.h>
#include <stdint.h>
#include "common.h"

/*
 * wal_frame_state.h — the ring's block counters (V2 §10). The ring is
 * split into 512-byte blocks with one counter each: copiers add the bytes
 * they copied into a block (page header bytes count in a page's first
 * block), so a block reading 512 is fully copied. The write step reads
 * them to find how far the ring is complete, and zeroes the ones it has
 * written. WalManager holds the array.
 *
 * Counter i belongs to the ring's i-th block: index (lsn >> 9) &
 * ctr_mask. Each counter is padded to 16 bytes, 4 per cache line.
 */

#define WAL_BLOCK_SHIFT  9
#define WAL_BLOCK_SIZE   (1u << WAL_BLOCK_SHIFT)              /* 512 */
#define WAL_FRAME_BLOCKS (WAL_PAGE_SIZE / WAL_BLOCK_SIZE)     /* 8 per 4KB frame */

/* One block's counter, padded to a quarter of a cache line. */
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
