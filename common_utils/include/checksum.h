#ifndef CHECKSUM_H
#define CHECKSUM_H

#include <stddef.h>
#include <stdint.h>

/* ------------------------------------------------------------------ */
/*  FNV-1a 32-bit hash. Used as the trailer checksum for every fixed- */
/*  size MyDB metadata file (__database.mydb, __catalog.mydb,         */
/*  __schema.mydb).                                                   */
/*                                                                    */
/*    seed  = 0x811c9dc5                                              */
/*    prime = 0x01000193                                              */
/*                                                                    */
/*  Same algorithm lived in database_file.c and partition.c as        */
/*  static helpers; lifted here once schema_file.c became the third   */
/*  consumer.                                                         */
/* ------------------------------------------------------------------ */
uint32_t fnv1a(const void *data, size_t len);

#endif /* CHECKSUM_H */
