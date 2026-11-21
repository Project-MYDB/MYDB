#ifndef CHECKSUM_H
#define CHECKSUM_H

#include <stddef.h>
#include <stdint.h>

/* ------------------------------------------------------------------ */
/*  CRC-32C (Castagnoli): reflected, polynomial 0x82F63B78, initial   */
/*  and final XOR 0xFFFFFFFF. The one checksum and hash every module  */
/*  uses: data pages (page.c), the metadata-file trailers             */
/*  (__database.mydb, __catalog.mydb, __schema.mydb, stats files,     */
/*  users.mydb, privileges.mydb), system_schema's hash-table buckets  */
/*  and, later, the WAL. Check value:                                 */
/*  crc32c("123456789") == 0xE3069283.                                */
/*                                                                    */
/*  On x86-64 with SSE4.2 (every x86 CPU since 2008) it uses the      */
/*  crc32 instruction, which computes exactly this polynomial, 8      */
/*  bytes at a time, as PostgreSQL and InnoDB do. Otherwise (an older */
/*  CPU, or a non-x86 build) it falls back to a table-driven loop,    */
/*  one byte per step, whose 256-entry table is a compile-time        */
/*  constant. The choice is made per call from the CPU's feature      */
/*  bits, so there is no shared state to set up and concurrent        */
/*  callers need nothing.                                             */
/*                                                                    */
/*  FNV-1a, which this header used to provide (first as the metadata  */
/*  trailer checksum, then only as the hash-table hash), is gone:     */
/*  crc32c is faster on this hardware for both jobs.                  */
/* ------------------------------------------------------------------ */
uint32_t crc32c(const void *data, size_t len);

/* ------------------------------------------------------------------ */
/*  Incremental CRC-32C: the same checksum as crc32c(), split so it   */
/*  can cover several buffers that are not next to each other,        */
/*  without copying them into one scratch first:                      */
/*                                                                    */
/*    uint32_t c = CRC32C_INIT;                                       */
/*    c = crc32c_update(c, part_a, a_len);                            */
/*    c = crc32c_update(c, part_b, b_len);                            */
/*    uint32_t result = crc32c_final(c);   // == crc32c(a ++ b)       */
/* ------------------------------------------------------------------ */
#define CRC32C_INIT 0xFFFFFFFFu

/* Folds len bytes into the running state. Not a finished checksum on
 * its own: pass the result to crc32c_final(). */
uint32_t crc32c_update(uint32_t crc, const void *data, size_t len);

/* Applies the final XOR, turning running state into the checksum. */
static inline uint32_t crc32c_final(uint32_t crc) { return ~crc; }

/* The software (table) path on its own, the same running-state contract
 * as crc32c_update. Callers use crc32c_update, which picks the hardware
 * path when it can; this is exposed so test_checksum can test the
 * fallback too on a machine that would never take it. */
uint32_t crc32c_update_sw(uint32_t crc, const void *data, size_t len);

/* 1 if crc32c_update uses the hardware instruction on this CPU, else 0. */
int crc32c_uses_hardware(void);

#endif /* CHECKSUM_H */
