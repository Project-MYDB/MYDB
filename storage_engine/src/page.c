#include "page.h"
#include "checksum.h"
#include <string.h>
#include <stdint.h>
#include <stdio.h>

/* ------------------------------------------------------------------ */
/*  Record header encode/decode (5 bytes on disk)                     */
/*                                                                    */
/*  Layout:                                                           */
/*    Byte 0   : info_flags                                           */
/*    Bytes 1-2: (heap_no << 3) | rec_type  as big-endian uint16      */
/*    Bytes 3-4: next_offset  as big-endian uint16                    */
/* ------------------------------------------------------------------ */

void rec_hdr_encode(const RecordHeader *rh, uint8_t *dst)
{
    dst[0] = rh->info_flags;
    dst[1] = (uint8_t)(rh->heap_type >> 8);
    dst[2] = (uint8_t)(rh->heap_type & 0xFF);
    dst[3] = (uint8_t)(rh->next_offset >> 8);
    dst[4] = (uint8_t)(rh->next_offset & 0xFF);
}

void rec_hdr_decode(const uint8_t *src, RecordHeader *rh)
{
    rh->info_flags   = src[0];
    rh->heap_type    = ((uint16_t)src[1] << 8) | src[2];
    rh->next_offset  = ((uint16_t)src[3] << 8) | src[4];
}

/* ------------------------------------------------------------------ */
/*  Page header encode/decode                                         */
/*                                                                    */
/*  We store the PageHeader struct at a fixed layout using explicit   */
/*  byte-by-byte writes so the layout is endian-independent.          */
/*                                                                    */
/*  Offsets within the 38-byte header:                                */
/*    0  : checksum      (4B)                                         */
/*    4  : page_no       (4B)                                         */
/*    8  : prev_page     (4B)                                         */
/*    12 : next_page     (4B)                                         */
/*    16 : lsn           (8B)                                         */
/*    24 : page_type     (2B)                                         */
/*    26 : num_records   (2B)                                         */
/*    28 : free_offset   (2B)                                         */
/*    30 : garbage_offset(2B)                                         */
/*    32 : num_dir_slots (2B)                                         */
/*    34 : (4 bytes reserved/padding to reach 38B)                    */
/* ------------------------------------------------------------------ */

/* Store a 4-byte big-endian uint32 at dst */
static void put32(uint8_t *dst, uint32_t v)
{
    dst[0] = (uint8_t)(v >> 24);
    dst[1] = (uint8_t)(v >> 16);
    dst[2] = (uint8_t)(v >>  8);
    dst[3] = (uint8_t)(v      );
}

/* Store an 8-byte big-endian uint64 at dst */
static void put64(uint8_t *dst, uint64_t v)
{
    put32(dst,     (uint32_t)(v >> 32));
    put32(dst + 4, (uint32_t)(v      ));
}

/* Store a 2-byte big-endian uint16 at dst */
static void put16(uint8_t *dst, uint16_t v)
{
    dst[0] = (uint8_t)(v >> 8);
    dst[1] = (uint8_t)(v     );
}

/* Load a 4-byte big-endian uint32 from src */
static uint32_t get32(const uint8_t *src)
{
    return ((uint32_t)src[0] << 24) | ((uint32_t)src[1] << 16)
         | ((uint32_t)src[2] <<  8) |  (uint32_t)src[3];
}

/* Load an 8-byte big-endian uint64 from src */
static uint64_t get64(const uint8_t *src)
{
    return ((uint64_t)get32(src) << 32) | get32(src + 4);
}

/* Load a 2-byte big-endian uint16 from src */
static uint16_t get16(const uint8_t *src)
{
    return ((uint16_t)src[0] << 8) | src[1];
}

void page_read_header(const uint8_t *page, PageHeader *hdr)
{
    hdr->checksum       = get32(page +  0);
    hdr->page_no        = get32(page +  4);
    hdr->prev_page      = get32(page +  8);
    hdr->next_page      = get32(page + 12);
    hdr->lsn            = get64(page + 16);
    hdr->page_type      = get16(page + 24);
    hdr->num_records    = get16(page + 26);
    hdr->free_offset    = get16(page + 28);
    hdr->garbage_offset = get16(page + 30);
    hdr->num_dir_slots  = get16(page + 32);
}

void page_write_header(uint8_t *page, const PageHeader *hdr)
{
    put32(page +  0, hdr->checksum);
    put32(page +  4, hdr->page_no);
    put32(page +  8, hdr->prev_page);
    put32(page + 12, hdr->next_page);
    put64(page + 16, hdr->lsn);
    put16(page + 24, hdr->page_type);
    put16(page + 26, hdr->num_records);
    put16(page + 28, hdr->free_offset);
    put16(page + 30, hdr->garbage_offset);
    put16(page + 32, hdr->num_dir_slots);
}

/* ------------------------------------------------------------------ */
/*  CRC-32C checksum over bytes [4 .. PAGE_SIZE-9]                    */
/*  (skips the checksum field itself and the trailer). It was a       */
/*  private FNV-1a copy here; it now uses the shared crc32c() from    */
/*  common_utils/checksum.h, like every other checksum in MyDB.       */
/* ------------------------------------------------------------------ */

void page_set_checksum(uint8_t *page)
{
    /* Checksum covers everything except the first 4 bytes (checksum field)
       and the last 8 bytes (trailer). */
    uint32_t cs = crc32c(page + 4, PAGE_SIZE - 4 - PAGE_TRAILER_SIZE);
    put32(page, cs);
    /* Mirror to trailer */
    put32(page + PAGE_SIZE - PAGE_TRAILER_SIZE, cs);
}

int page_verify_checksum(const uint8_t *page)
{
    uint32_t stored  = get32(page);
    uint32_t computed = crc32c(page + 4, PAGE_SIZE - 4 - PAGE_TRAILER_SIZE);
    //printf("Stored: %u\n", stored);
    //printf("Computed %u", computed);
    return (stored == computed) ? MYDB_OK : MYDB_ERR;
}

/* ------------------------------------------------------------------ */
/*  Page directory helpers                                            */
/* ------------------------------------------------------------------ */

uint16_t page_dir_get(const uint8_t *page, uint16_t i)
{
    return get16(page + DIR_SLOT_OFFSET(i));
}

static void page_dir_set(uint8_t *page, uint16_t i, uint16_t data_offset)
{
    put16(page + DIR_SLOT_OFFSET(i), data_offset);
}

uint16_t page_dir_count(const uint8_t *page)
{
    PageHeader hdr;
    page_read_header(page, &hdr);
    return hdr.num_dir_slots;
}

/* ------------------------------------------------------------------ */
/*  page_init                                                         */
/* ------------------------------------------------------------------ */
int page_init(uint8_t *page, uint32_t page_no, PageType type)
{
    if (!page) return MYDB_ERR;
    memset(page, 0, PAGE_SIZE);

    /* Write the page header */
    PageHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.page_no        = page_no;
    hdr.prev_page      = INVALID_PAGE;
    hdr.next_page      = INVALID_PAGE;
    hdr.page_type      = (uint16_t)type;
    hdr.free_offset    = USER_RECORDS_OFFSET;  /* first user record goes at offset 64 */
    hdr.garbage_offset = 0;
    hdr.num_records    = 0;
    hdr.num_dir_slots  = 0;
    page_write_header(page, &hdr);

    /*
     * Infimum record at offset 38 — the lower boundary pseudo-record.
     * Its next_offset initially points to Supremum's DATA offset (56).
     */
    RecordHeader inf;
    inf.info_flags  = 0;
    inf.heap_type   = (0 << 3) | REC_INFIMUM;  /* heap_no=0, type=INFIMUM */
    inf.next_offset = SUPREMUM_DATA;
    rec_hdr_encode(&inf, page + INFIMUM_OFFSET);
    /* Infimum data: 8 bytes "infimum\0" */
    memcpy(page + INFIMUM_DATA, "infimum\0", 8);

    /*
     * Supremum record at offset 51 — the upper boundary pseudo-record.
     * Its next_offset is 0 (end of chain).
     */
    RecordHeader sup;
    sup.info_flags  = 0;
    sup.heap_type   = (1 << 3) | REC_SUPREMUM;  /* heap_no=1, type=SUPREMUM */
    sup.next_offset = 0;
    rec_hdr_encode(&sup, page + SUPREMUM_OFFSET);
    memcpy(page + SUPREMUM_DATA, "supremum", 8);

    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  page_free_space                                                   */
/*                                                                    */
/*  Free space = gap between free_offset (top of records) and the     */
/*  bottom of the page directory.                                     */
/*                                                                    */
/*  Directory bottom = PAGE_SIZE - PAGE_TRAILER_SIZE - 2*(num_slots+1)*/
/*  (+1 because we need room for one more slot after inserting)       */
/* ------------------------------------------------------------------ */
uint16_t page_free_space(const uint8_t *page)
{
    PageHeader hdr;
    page_read_header(page, &hdr);

    /* Byte just below the lowest existing directory slot */
    uint16_t dir_top = (uint16_t)(PAGE_SIZE - PAGE_TRAILER_SIZE
                                  - 2 * hdr.num_dir_slots);
    if (dir_top <= hdr.free_offset) return 0;
    return dir_top - hdr.free_offset;
}

/* ------------------------------------------------------------------ */
/*  page_check_invariants — debug-only integrity oracle               */
/*                                                                    */
/*  After every mutator, asserts:                                     */
/*    - num_records == num_dir_slots                                  */
/*    - Walking the linked list (Infimum→Supremum) and skipping       */
/*      records with the deleted flag yields the same sequence of     */
/*      data offsets as iterating the directory dir[0..n-1].          */
/*                                                                    */
/*  This catches: slot-shift memmove bugs, drift between counters,    */
/*  out-of-sync mutations of list vs. directory, and next_offset      */
/*  corruption from any source.                                       */
/* ------------------------------------------------------------------ */

void page_check_invariants(const uint8_t *page)
{
    PageHeader hdr;
    page_read_header(page, &hdr);

    assert(hdr.num_records == hdr.num_dir_slots);

    RecordHeader inf_hdr;
    rec_hdr_decode(page + INFIMUM_OFFSET, &inf_hdr);

    uint16_t cur = inf_hdr.next_offset;
    uint16_t i   = 0;
    /* Bound the walk so corrupt next_offset can't loop forever */
    uint16_t steps = 0;
    while (cur != SUPREMUM_DATA && cur != 0 && steps < 8192) {
        RecordHeader rh;
        rec_hdr_decode(page + cur - RECORD_HEADER_SIZE, &rh);

        if (!(rh.info_flags & 0x01)) {  /* live */
            assert(i < hdr.num_dir_slots);
            assert(page_dir_get(page, i) == cur);
            i++;
        }
        cur = rh.next_offset;
        steps++;
    }
    assert(i == hdr.num_dir_slots);
}

/* ------------------------------------------------------------------ */
/*  page_insert_record — key-ordered slot directory                   */
/*                                                                    */
/*  Inserts a new record at directory slot `at_slot`. Existing slots  */
/*  [at_slot .. n-1] shift right by one. The linked list is spliced   */
/*  after the predecessor record (slot at_slot-1, or Infimum).        */
/*                                                                    */
/*  Physical layout: record bytes are still appended at free_offset.  */
/*  Only the directory and linked list reflect key order.             */
/* ------------------------------------------------------------------ */
int page_insert_record(uint8_t *page,
                       const uint8_t *record_data, uint16_t record_size,
                       uint16_t at_slot)
{
    if (!page || !record_data) return MYDB_ERR;

    PageHeader hdr;
    page_read_header(page, &hdr);

    if (at_slot > hdr.num_dir_slots) return MYDB_ERR;

    /* Total bytes needed: record header + data + one directory slot (2B) */
    uint16_t total = RECORD_HEADER_SIZE + record_size + 2;
    if (page_free_space(page) < total) return MYDB_ERR_FULL;

    /* Where we will write the new record physically */
    uint16_t rec_offset  = hdr.free_offset;
    uint16_t data_offset = rec_offset + RECORD_HEADER_SIZE;

    /* Determine predecessor in the linked list:
     *   at_slot == 0 → Infimum
     *   else        → record at slot (at_slot - 1)
     */
    uint16_t pred_data_off;
    if (at_slot == 0) {
        pred_data_off = INFIMUM_DATA;
    } else {
        pred_data_off = page_dir_get(page, at_slot - 1);
    }

    RecordHeader pred_hdr;
    rec_hdr_decode(page + pred_data_off - RECORD_HEADER_SIZE, &pred_hdr);
    uint16_t old_next = pred_hdr.next_offset;

    /* Build new record header. heap_no is informational only; using
     * num_records+2 here is just a stable label, no longer a unique id
     * (deletes don't reuse heap_no, but the directory carries identity). */
    RecordHeader new_hdr;
    new_hdr.info_flags  = 0;
    new_hdr.heap_type   = (uint16_t)(((hdr.num_records + 2) << 3) | REC_ORDINARY);
    new_hdr.next_offset = old_next;
    rec_hdr_encode(&new_hdr, page + rec_offset);

    /* Write payload */
    memcpy(page + data_offset, record_data, record_size);

    /* Splice into linked list */
    pred_hdr.next_offset = data_offset;
    rec_hdr_encode(&pred_hdr, page + pred_data_off - RECORD_HEADER_SIZE);

    /* Shift directory slots [at_slot .. n-1] right by one (toward lower
     * memory addresses, since slot i lives at PAGE_SIZE-trailer-2-2*i). */
    for (int i = (int)hdr.num_dir_slots; i > (int)at_slot; i--) {
        uint16_t v = page_dir_get(page, (uint16_t)(i - 1));
        page_dir_set(page, (uint16_t)i, v);
    }
    page_dir_set(page, at_slot, data_offset);

    /* Update header */
    hdr.free_offset   = data_offset + record_size;
    hdr.num_records++;
    hdr.num_dir_slots++;
    page_write_header(page, &hdr);

    page_check_invariants(page);
    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  page_delete_record                                                */
/*                                                                    */
/*  Sets the deleted flag on the record (physical bytes stay in place */
/*  until page_compact reclaims them) AND removes the slot from the   */
/*  directory. The linked list is left intact: the deleted record     */
/*  remains threaded with its delete bit set, but no directory slot   */
/*  references it. page_compact rebuilds both list and directory.     */
/* ------------------------------------------------------------------ */
int page_delete_record(uint8_t *page, uint16_t slot_no)
{
    if (!page) return MYDB_ERR;

    PageHeader hdr;
    page_read_header(page, &hdr);
    if (slot_no >= hdr.num_dir_slots) return MYDB_ERR_NOT_FOUND;

    uint16_t data_offset = page_dir_get(page, slot_no);
    uint16_t rec_offset  = data_offset - RECORD_HEADER_SIZE;

    RecordHeader rh;
    rec_hdr_decode(page + rec_offset, &rh);

    if (rh.info_flags & 0x01) return MYDB_ERR;  /* already deleted */

    rh.info_flags |= 0x01;
    rec_hdr_encode(&rh, page + rec_offset);

    /* Shift directory slots [slot_no+1 .. n-1] left by one */
    for (uint16_t i = slot_no; i + 1 < hdr.num_dir_slots; i++) {
        uint16_t v = page_dir_get(page, (uint16_t)(i + 1));
        page_dir_set(page, i, v);
    }

    hdr.num_records--;
    hdr.num_dir_slots--;
    page_write_header(page, &hdr);

    page_check_invariants(page);
    return MYDB_OK;
}





void print_page_header(const PageHeader *hdr)
{
    printf("PageHeader\n");
    printf("{\n");

    printf("  checksum       = %u   (size = %zu bytes)\n",
           hdr->checksum, sizeof(hdr->checksum));

    printf("  page_no        = %u   (size = %zu bytes)\n",
           hdr->page_no, sizeof(hdr->page_no));

    printf("  prev_page      = %u   (size = %zu bytes)\n",
           hdr->prev_page, sizeof(hdr->prev_page));

    printf("  next_page      = %u   (size = %zu bytes)\n",
           hdr->next_page, sizeof(hdr->next_page));

    printf("  lsn            = %llu (size = %zu bytes)\n",
           (unsigned long long)hdr->lsn,
           sizeof(hdr->lsn));

    printf("  page_type      = %u   (size = %zu bytes)\n",
           hdr->page_type, sizeof(hdr->page_type));

    printf("  num_records    = %u   (size = %zu bytes)\n",
           hdr->num_records, sizeof(hdr->num_records));

    printf("  free_offset    = %u   (size = %zu bytes)\n",
           hdr->free_offset, sizeof(hdr->free_offset));

    printf("  garbage_offset = %u   (size = %zu bytes)\n",
           hdr->garbage_offset, sizeof(hdr->garbage_offset));

    printf("  num_dir_slots  = %u   (size = %zu bytes)\n",
           hdr->num_dir_slots, sizeof(hdr->num_dir_slots));

    printf("}\n");

    printf("sizeof(PageHeader) = %zu bytes\n", sizeof(PageHeader));

    /* Serialized on-disk header size */
    printf("serialized header size = %u bytes\n", 38u);
}
/*
 * Compute the size of the record at data_offset `doff` in O(1).
 *
 * All B+ tree record formats start with [klen:2B][key_bytes], so klen
 * is always at doff+0..1. What follows depends on page type:
 *   PAGE_TYPE_DATA     (clustered leaf)  : [vlen:2B][val_bytes]
 *   PAGE_TYPE_INTERNAL (internal node)   : [child_page_no:4B]
 *   PAGE_TYPE_INDEX    (secondary leaf)  : [page_no:4B][slot_no:2B]
 */
static uint16_t record_phys_size(const uint8_t *page, uint16_t doff)
{
    //printf("\n=== record_phys_size() ===\n");
    /* Read and print page header */
    PageHeader hdr;
    page_read_header(page, &hdr);
    //print_page_header(&hdr);

    /* Compute record pointer */
    const uint8_t *rec = page + doff;

    /* Read key length */
    uint16_t klen = ((uint16_t)rec[0] << 8) | rec[1];

    /* INTERNAL page */
    if (hdr.page_type == PAGE_TYPE_INTERNAL) {
        uint16_t result = (uint16_t)(2 + klen + 4);
        /* printf("Physical size = 2 + %u + 4 = %u\n", klen, result); */
        return result;
    }

    /* INDEX page */
    if (hdr.page_type == PAGE_TYPE_INDEX) {
        uint16_t result = (uint16_t)(2 + klen + 6);
        /* printf("Physical size = 2 + %u + 6 = %u\n", klen, result); */
        return result;
    }

    uint16_t vlen = ((uint16_t)rec[2 + klen] << 8) |
                     rec[3 + klen];

    uint16_t result = (uint16_t)(4 + klen + vlen);

    /* printf("Physical size = 4 + %u + %u = %u\n", klen, vlen, result); */

    return result;
}

/* ------------------------------------------------------------------ */
/*  page_get_record                                                   */
/* ------------------------------------------------------------------ */
int page_get_record(const uint8_t *page, uint16_t slot_no,
                    uint16_t *data_offset, uint16_t *data_size)
{
    if (!page || !data_offset || !data_size) return MYDB_ERR;

    PageHeader hdr;
    page_read_header(page, &hdr);
    if (slot_no >= hdr.num_dir_slots) return MYDB_ERR_NOT_FOUND;

    uint16_t doff = page_dir_get(page, slot_no);

    RecordHeader rh;
    rec_hdr_decode(page + doff - RECORD_HEADER_SIZE, &rh);
    if (rh.info_flags & 0x01) return MYDB_ERR_NOT_FOUND;  /* deleted */

    *data_offset = doff;
    *data_size   = record_phys_size(page, doff);
    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  page_compact                                                      */
/*                                                                    */
/*  Rebuilds the page from scratch by iterating the directory (which  */
/*  is in key order, contains only live records). Each record is      */
/*  appended into a fresh page via page_insert_record, which produces */
/*  a clean linked list with no dead nodes and a tight physical       */
/*  layout.                                                           */
/* ------------------------------------------------------------------ */
int page_compact(uint8_t *page)
{
    if (!page) return MYDB_ERR;

    PageHeader hdr;
    page_read_header(page, &hdr);

    /* Work in a temporary buffer */
    uint8_t tmp[PAGE_SIZE];
    page_init(tmp, hdr.page_no, (PageType)hdr.page_type);

    /* Preserve leaf-chain pointers and page-level fields */
    PageHeader thdr;
    page_read_header(tmp, &thdr);
    thdr.prev_page = hdr.prev_page;
    thdr.next_page = hdr.next_page;
    thdr.lsn       = hdr.lsn;
    page_write_header(tmp, &thdr);

    uint16_t n = hdr.num_dir_slots;
    for (uint16_t i = 0; i < n; i++) {
        uint16_t doff, dsz;
        if (page_get_record(page, i, &doff, &dsz) != MYDB_OK) continue;
        page_insert_record(tmp, page + doff, dsz, i);
    }

    memcpy(page, tmp, PAGE_SIZE);
    page_check_invariants(page);
    return MYDB_OK;
}
