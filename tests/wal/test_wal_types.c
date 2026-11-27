#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "common.h"
#include "wal_types.h"
#include "wal_page.h"
#include "wal_segment.h"
#include "checksum.h"

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

/* ------------------------------------------------------------------ */

/* WalRecordHeader/WalPageHeader/WalSegmentHeader are plain (non-packed)
 * structs — see wal_types.h's header comment. Their sizeof() isn't
 * asserted here: the documented on-disk byte counts (44/24/48; the page
 * header was 36, then 32) are
 * enforced by wal_segment.c's serialize/deserialize functions via
 * explicit offsets, not by struct layout. What's actually load-bearing
 * is the arithmetic relationship between the WAL_* size constants
 * themselves. */
static void test_size_constants(void)
{
    printf("\n[test_size_constants]\n");
    CHECK(WAL_PAGE_USABLE == WAL_PAGE_SIZE - WAL_PAGE_HEADER_SIZE,
          "WAL_PAGE_USABLE == WAL_PAGE_SIZE - WAL_PAGE_HEADER_SIZE");
    /* The segment's header page is outside the LSN, so the first record
     * is just past the first log page's 24-byte header. */
    CHECK(WAL_FIRST_RECORD_LSN == WAL_PAGE_HEADER_SIZE,
          "WAL_FIRST_RECORD_LSN == 24, page 0's header only");
    CHECK(WAL_SEG_SIZE == (1u << 24) && WAL_SEG_PAGES == 4096,
          "a segment holds exactly 16MB of log, 4096 pages");
}

/* V2 §4 rule 6 and the Appendix E number checks: pos (record bytes only)
 * <-> LSN (page headers included), and the segment, page and offset of an
 * LSN. */
static void test_lsn_rule_6(void)
{
    printf("\n[test_lsn_rule_6]\n");
    CHECK(wal_byte_to_lsn(0) == 24,                 "pos 0    -> LSN 24");
    CHECK(wal_byte_to_lsn(4071) == 4095,            "pos 4071 -> LSN 4095 (last byte of page 0)");
    CHECK(wal_byte_to_lsn(4072) == 4096 + 24,       "pos 4072 -> LSN 4120 (past page 1's header)");
    CHECK(wal_lsn_to_byte(4120) == 4072,            "and back: LSN 4120 -> pos 4072");
    CHECK((uint64_t)WAL_SEG_PAGES * WAL_PAGE_USABLE == 16678912u,
          "4096 x 4072 = 16,678,912 record bytes per segment");

    /* Crossing a segment boundary skips nothing: the last byte of
     * segment 0 is pos 16,678,911, and the next one is segment 1's first
     * record, at (1 << 24) + 24. */
    CHECK(wal_byte_to_lsn(16678911) == (1u << 24) - 1, "last record byte of segment 0 is LSN 16MB - 1");
    CHECK(wal_byte_to_lsn(16678912) == (1u << 24) + 24, "the next is segment 1's first record");
    CHECK(wal_segment_first_lsn(1) == (1u << 24) + 24, "wal_segment_first_lsn(1) agrees");
    CHECK(wal_lsn_segment((1u << 24) + 24) == 1 && wal_lsn_page((1u << 24) + 24) == 0,
          "it is segment 1, page 0");

    CHECK(wal_lsn_advance(24, 4072) == 4096,          "4072 record bytes from LSN 24 end exactly at page 0's end");
    CHECK(wal_lsn_advance(24, 4073) == 4096 + 24 + 1, "one more goes past page 1's header");

    uint64_t lsn = (5ull << 24) + (235ull << 12) + 1501;   /* segment 5, page 235, offset 1501 */
    CHECK(wal_lsn_segment(lsn) == 5 && wal_lsn_page(lsn) == 235 && wal_lsn_offset(lsn) == 1501,
          "segment, page and offset come straight out of the bits");
    CHECK(wal_byte_to_lsn(wal_lsn_to_byte(lsn)) == lsn, "lsn_to_byte and byte_to_lsn are inverses");
}

static void test_page_header_round_trip(void)
{
    printf("\n[test_page_header_round_trip]\n");
    WalPageHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.id.file_type = FILETYPE_WAL_PAGE;
    hdr.start_lsn = 123456789ull;
    hdr.continuation_len = 1234;
    hdr.flags = WAL_PAGE_INCOMING_CONTINUATION;
    hdr.reserved[0] = 0x5C;

    uint8_t buf[WAL_PAGE_HEADER_SIZE];
    wal_page_header_serialize(&hdr, buf);

    WalPageHeader out;
    memset(&out, 0xAA, sizeof(out));
    int rc = wal_page_header_deserialize(buf, &out);

    CHECK(rc == MYDB_OK,                     "deserialize of a freshly-serialized page header succeeds");
    CHECK(out.id.magic == MYDB_MAGIC,        "magic round-trips");
    CHECK(out.id.file_type == FILETYPE_WAL_PAGE, "file_type round-trips");
    CHECK(out.start_lsn == hdr.start_lsn,    "start_lsn round-trips");
    CHECK(out.flags == hdr.flags,            "flags round-trips");
    CHECK(out.continuation_len == hdr.continuation_len,      "continuation_len round-trips");
    CHECK(out.reserved[0] == 0x5C,           "reserved[1] round-trips");

    /* The on-disk layout, byte by byte: id 0-7, start_lsn 8-15,
     * continuation_len 16-17, flags 18, reserved 19, CRC 20-23. */
    uint16_t cl;
    uint64_t sl;
    uint32_t cs;
    memcpy(&sl, buf + 8, 8);
    memcpy(&cl, buf + 16, 2);
    memcpy(&cs, buf + 20, 4);
    CHECK(sl == hdr.start_lsn && cl == 1234 && buf[18] == hdr.flags && buf[19] == 0x5C,
          "fields sit at their documented offsets");
    CHECK(cs == crc32c(buf, 20), "the CRC at 20-23 covers bytes 0-19");
}

static void test_page_header_detects_tamper(void)
{
    printf("\n[test_page_header_detects_tamper]\n");
    WalPageHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.id.file_type = FILETYPE_WAL_PAGE;
    hdr.start_lsn = 42;

    uint8_t buf[WAL_PAGE_HEADER_SIZE];
    wal_page_header_serialize(&hdr, buf);
    buf[9] ^= 0x01;   /* flip a byte inside start_lsn */

    WalPageHeader out;
    int rc = wal_page_header_deserialize(buf, &out);
    CHECK(rc == MYDB_ERR_BAD_CHECKSUM, "corrupted page header fails checksum verification");
}

static void test_page_header_wrong_file_type(void)
{
    printf("\n[test_page_header_wrong_file_type]\n");
    WalPageHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.id.file_type = FILETYPE_WAL_SEGMENT;   /* wrong type on purpose */

    uint8_t buf[WAL_PAGE_HEADER_SIZE];
    wal_page_header_serialize(&hdr, buf);

    WalPageHeader out;
    int rc = wal_page_header_deserialize(buf, &out);
    CHECK(rc == MYDB_ERR_BAD_FILE_TYPE, "a page header stamped with the wrong file_type is rejected");
}

static void test_segment_header_round_trip(void)
{
    printf("\n[test_segment_header_round_trip]\n");
    WalSegmentHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.id.file_type = FILETYPE_WAL_SEGMENT;
    hdr.segment_no = 7;
    hdr.start_lsn = 1000;
    hdr.end_lsn = 2000;
    hdr.partition_id = 3;
    hdr.data_pages = 512;
    hdr.state = SEG_ACTIVE;

    uint8_t buf[WAL_SEGMENT_HEADER_ON_DISK_SIZE];
    wal_segment_header_serialize(&hdr, buf);

    WalSegmentHeader out;
    memset(&out, 0xAA, sizeof(out));
    int rc = wal_segment_header_deserialize(buf, &out);

    CHECK(rc == MYDB_OK,                        "deserialize of a freshly-serialized segment header succeeds");
    CHECK(out.segment_no == hdr.segment_no,      "segment_no round-trips");
    CHECK(out.start_lsn == hdr.start_lsn,        "start_lsn round-trips");
    CHECK(out.end_lsn == hdr.end_lsn,            "end_lsn round-trips");
    CHECK(out.partition_id == hdr.partition_id,  "partition_id round-trips");
    CHECK(out.data_pages == hdr.data_pages,      "data_pages round-trips");
    CHECK(out.state == (uint8_t)SEG_ACTIVE,      "state round-trips");

    /* Trailing padding bytes (48..64) must be zeroed on disk. */
    int padding_zeroed = 1;
    for (int i = 48; i < WAL_SEGMENT_HEADER_ON_DISK_SIZE; i++)
        if (buf[i] != 0) padding_zeroed = 0;
    CHECK(padding_zeroed, "trailing 16 padding bytes are zeroed");
}

static void test_segment_header_detects_tamper(void)
{
    printf("\n[test_segment_header_detects_tamper]\n");
    WalSegmentHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.id.file_type = FILETYPE_WAL_SEGMENT;
    hdr.segment_no = 1;
    hdr.state = SEG_DONE;

    uint8_t buf[WAL_SEGMENT_HEADER_ON_DISK_SIZE];
    wal_segment_header_serialize(&hdr, buf);
    buf[20] ^= 0x01;   /* flip a byte inside end_lsn */

    WalSegmentHeader out;
    int rc = wal_segment_header_deserialize(buf, &out);
    CHECK(rc == MYDB_ERR_BAD_CHECKSUM, "corrupted segment header fails checksum verification");
}

/* A 400 KB body round-trips: the checksum has no size limit. */
static void test_record_header_large_body(void)
{
    printf("\n[test_record_header_large_body]\n");

    size_t body_len = 400 * 1024;
    uint8_t *body = malloc(body_len);
    for (size_t i = 0; i < body_len; i++) body[i] = (uint8_t)(i & 0xFF);

    uint8_t *buf = malloc(WAL_RECORD_HEADER_SIZE + body_len);

    WalRecordHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.lsn       = 4242;
    hdr.total_len = (uint32_t)(WAL_RECORD_HEADER_SIZE + body_len);
    hdr.rec_type  = WAL_REC_SCHEMA_UPDATE;
    wal_record_header_serialize(&hdr, body, body_len, buf);

    WalRecordHeader out;
    CHECK(wal_record_header_deserialize(buf, body_len, &out) == MYDB_OK,
          "a 400KB-body record serializes and validates");
    CHECK(out.lsn == 4242 && out.total_len == hdr.total_len, "its header fields round-trip");
    CHECK(memcmp(buf + WAL_RECORD_HEADER_SIZE, body, body_len) == 0, "its body round-trips");

    free(buf);
    free(body);
}

/* The on-disk header is the struct's first 44 bytes. Checked on the
 * packed bytes, since a round trip passes with any layout. */
static void test_record_header_layout(void)
{
    printf("\n[test_record_header_layout]\n");
    WalRecordHeader h;
    memset(&h, 0, sizeof(h));
    h.lsn = 0x1111111111111111ull; h.prev_lsn = 0x2222222222222222ull;
    h.txn_id = 0x3333333333333333ull; h.total_len = 0x44444444u;
    h.table_id = 0x55555555u; h.page_no = 0x66666666u;
    h.rec_type = 0x77; h.flags = 0x01; h.reserved[0] = 0; h.reserved[1] = 0;

    uint8_t b[WAL_RECORD_HEADER_SIZE];
    wal_record_header_pack_crc(&h, CRC32C_INIT, b);

    uint64_t u64; uint32_t u32;
    memcpy(&u64, b + 0,  8); CHECK(u64 == h.lsn,       "lsn at bytes 0-7");
    memcpy(&u64, b + 8,  8); CHECK(u64 == h.prev_lsn,  "prev_lsn at bytes 8-15");
    memcpy(&u64, b + 16, 8); CHECK(u64 == h.txn_id,    "txn_id at bytes 16-23");
    memcpy(&u32, b + 24, 4); CHECK(u32 == h.total_len, "total_len at bytes 24-27");
    memcpy(&u32, b + 28, 4); CHECK(u32 == h.table_id,  "table_id at bytes 28-31");
    memcpy(&u32, b + 32, 4); CHECK(u32 == h.page_no,   "page_no at bytes 32-35");
    CHECK(b[36] == 0x77 && b[37] == 0x01 && b[38] == 0 && b[39] == 0,
          "rec_type, flags, reserved at bytes 36-39");
    memcpy(&u32, b + 40, 4); CHECK(u32 == h.checksum,  "checksum at bytes 40-43");
    CHECK(sizeof(WalRecordHeader) == 48 && WAL_RECORD_HEADER_SIZE == 44,
          "sizeof is 48 (end padding), the on-disk size 44");

    /* Unaligned source: deserialize reads a header that does not start on
     * an 8-byte boundary, as one does after an odd-length body. */
    uint8_t odd[1 + WAL_RECORD_HEADER_SIZE];
    memcpy(odd + 1, b, WAL_RECORD_HEADER_SIZE);
    WalRecordHeader out;
    CHECK(wal_record_header_deserialize(odd + 1, 0, &out) == MYDB_OK &&
          out.lsn == h.lsn && out.txn_id == h.txn_id && out.total_len == h.total_len &&
          out.page_no == h.page_no && out.checksum == h.checksum,
          "a header at an odd address deserializes with every field intact");
}

/* pack_crc writes the same 44 header bytes serialize does, without
 * touching the body: the inserter copies a spanning record's body into
 * the ring in pieces. The body checksum comes from the caller, as the
 * thread that builds a record will compute it before calling insert. */
static void test_record_header_pack_crc(void)
{
    printf("\n[test_record_header_pack_crc]\n");
    uint8_t body[500];
    for (size_t i = 0; i < sizeof(body); i++) body[i] = (uint8_t)(i * 3);

    WalRecordHeader a, b;
    memset(&a, 0, sizeof(a));
    a.lsn = 777; a.txn_id = 9; a.rec_type = WAL_REC_INSERT;
    a.total_len = WAL_RECORD_HEADER_SIZE + sizeof(body);
    b = a;

    uint32_t body_checksum = crc32c_update(CRC32C_INIT, body, sizeof(body));

    uint8_t full[WAL_RECORD_HEADER_SIZE + sizeof(body)];
    uint8_t packed[WAL_RECORD_HEADER_SIZE];
    wal_record_header_serialize(&a, body, sizeof(body), full);
    wal_record_header_pack_crc(&b, body_checksum, packed);
    CHECK(memcmp(full, packed, WAL_RECORD_HEADER_SIZE) == 0, "pack_crc's 44 bytes equal serialize's header");
    CHECK(a.checksum == b.checksum, "both stamp the same checksum");

    WalRecordHeader out;
    memcpy(full, packed, WAL_RECORD_HEADER_SIZE);
    CHECK(wal_record_header_deserialize(full, sizeof(body), &out) == MYDB_OK,
          "a packed header followed by the body validates");
}

/* V2 §8.3: the caller takes the body's CRC before the record has an LSN,
 * and pack_crc finishes it with the header after. The stored checksum
 * must be CRC-32C over the body followed by the header's first 40 bytes,
 * and damage to either part must fail validation. */
static void test_record_crc_split(void)
{
    printf("\n[test_record_crc_split]\n");
    uint8_t body[1000];
    for (size_t i = 0; i < sizeof(body); i++) body[i] = (uint8_t)(i * 7 + 1);

    /* The body checksum happens first, before the fields an inserter only
     * learns after reserving (lsn, flags) are even set. */
    uint32_t body_checksum = crc32c_update(CRC32C_INIT, body, sizeof(body));

    WalRecordHeader a;
    memset(&a, 0, sizeof(a));
    a.lsn = 4120; a.prev_lsn = 24; a.txn_id = 42; a.table_id = 5; a.page_no = 7;
    a.rec_type = WAL_REC_INSERT;
    a.total_len = WAL_RECORD_HEADER_SIZE + sizeof(body);

    uint8_t split[WAL_RECORD_HEADER_SIZE];
    wal_record_header_pack_crc(&a, body_checksum, split);

    /* The stored value, recomputed from its definition: body first, then
     * the header up to the checksum field. */
    uint8_t span[sizeof(body) + 40];
    memcpy(span, body, sizeof(body));
    memcpy(span + sizeof(body), split, 40);
    CHECK(a.checksum == crc32c(span, sizeof(span)),
          "checksum == crc32c(body ++ header[0..40))");

    uint8_t rec[WAL_RECORD_HEADER_SIZE + sizeof(body)];
    memcpy(rec, split, WAL_RECORD_HEADER_SIZE);
    memcpy(rec + WAL_RECORD_HEADER_SIZE, body, sizeof(body));
    WalRecordHeader out;
    CHECK(wal_record_header_deserialize(rec, sizeof(body), &out) == MYDB_OK &&
          out.lsn == 4120 && out.prev_lsn == 24 && out.txn_id == 42,
          "the record validates and its fields round-trip");

    rec[WAL_RECORD_HEADER_SIZE + 500] ^= 0x01;
    CHECK(wal_record_header_deserialize(rec, sizeof(body), &out) == MYDB_ERR_BAD_CHECKSUM,
          "a flipped body bit fails the checksum");
    rec[WAL_RECORD_HEADER_SIZE + 500] ^= 0x01;
    rec[0] ^= 0x01;   /* inside lsn */
    CHECK(wal_record_header_deserialize(rec, sizeof(body), &out) == MYDB_ERR_BAD_CHECKSUM,
          "a flipped header bit fails the checksum");

    /* No body (BEGIN): the body checksum is just the initial state. */
    WalRecordHeader e;
    memset(&e, 0, sizeof(e));
    e.lsn = 24; e.rec_type = WAL_REC_BEGIN; e.total_len = WAL_RECORD_HEADER_SIZE;
    uint8_t ebuf[WAL_RECORD_HEADER_SIZE];
    wal_record_header_pack_crc(&e, CRC32C_INIT, ebuf);
    CHECK(wal_record_header_deserialize(ebuf, 0, &out) == MYDB_OK,
          "a record with no body (BEGIN) packs and validates");
}

/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_wal_types ===\n");

    test_size_constants();
    test_lsn_rule_6();
    test_record_header_large_body();
    test_record_header_layout();
    test_record_header_pack_crc();
    test_record_crc_split();
    test_page_header_round_trip();
    test_page_header_detects_tamper();
    test_page_header_wrong_file_type();
    test_segment_header_round_trip();
    test_segment_header_detects_tamper();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
