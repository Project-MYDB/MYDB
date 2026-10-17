#ifndef CHECKSUM_H
#define CHECKSUM_H

#include <stddef.h>
#include <stdint.h>

/* ------------------------------------------------------------------ */
/*  FNV-1a 32-bit hash. No longer the metadata-file trailer checksum  */
/*  (see crc32() below) — kept for the one remaining consumer that    */
/*  genuinely wants a hash, not a corruption-detection checksum:      */
/*  system_schema.c's name_hash() (username -> hash-table bucket).    */
/*                                                                    */
/*    seed  = 0x811c9dc5                                              */
/*    prime = 0x01000193                                              */
/*                                                                    */
/*  Originally lived in database_file.c and partition.c as static     */
/*  helpers; lifted here once schema_file.c became the third          */
/*  consumer.                                                         */
/* ------------------------------------------------------------------ */
uint32_t fnv1a(const void *data, size_t len);

/* ------------------------------------------------------------------ */
/*  CRC32 (reflected, polynomial 0xEDB88320 / IEEE 802.3 — the same   */
/*  algorithm InnoDB and zlib use). This is the trailer checksum for  */
/*  every fixed-size MyDB metadata file (__database.mydb,             */
/*  __catalog.mydb, __schema.mydb, __stats.mydb, users.mydb,          */
/*  privileges.mydb) and for WAL page/record/segment headers          */
/*  (MYDB_WAL_DESIGN.md / MYDB_WAL_IMPLEMENTATION.md both specify     */
/*  CRC32 explicitly, distinct from the FNV-1a used elsewhere).       */
/*  Table-driven, 256-entry lookup table computed lazily on first     */
/*  call.                                                             */
/* ------------------------------------------------------------------ */
uint32_t crc32(const void *data, size_t len);

/* ------------------------------------------------------------------ */
/*  Incremental CRC32 — same algorithm/parameters as crc32() above,    */
/*  just split so a checksum can span several non-contiguous buffers   */
/*  without copying them into one scratch first:                       */
/*                                                                     */
/*    uint32_t c = CRC32_INIT;                                         */
/*    c = crc32_update(c, part_a, a_len);                              */
/*    c = crc32_update(c, part_b, b_len);                              */
/*    uint32_t result = crc32_final(c);   // == crc32(a ++ b)          */
/*                                                                     */
/*  This is what WalRecordHeader's checksum needs: it covers the       */
/*  header's leading bytes plus the record body, two spans that are    */
/*  not adjacent on disk (the checksum field itself sits between       */
/*  them). Copying both into a stack scratch — the old approach —      */
/*  bounded record bodies to the scratch's size, which LARGE_WAL       */
/*  records exceeded by definition, as records spanning pages will.    */
/* ------------------------------------------------------------------ */
#define CRC32_INIT 0xFFFFFFFFu

/* Folds len bytes into the running state. Not a finished checksum on
 * its own — pass the result to crc32_final(). */
uint32_t crc32_update(uint32_t crc, const void *data, size_t len);

/* Applies the final XOR, turning running state into the checksum. */
static inline uint32_t crc32_final(uint32_t crc) { return ~crc; }

#endif /* CHECKSUM_H */
