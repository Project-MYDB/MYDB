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
    /* (WAL_MAX_ROW_BODY == WAL_MAX_RECORD_SIZE - WAL_RECORD_HEADER_SIZE was
     * checked here; both constants went with T-039 — records span pages.) */
    CHECK(WAL_FIRST_RECORD_LSN == WAL_PAGE_SIZE + WAL_PAGE_HEADER_SIZE,
          "WAL_FIRST_RECORD_LSN == segment page 0 + page 1's header");
    CHECK(WAL_SEG_DATA_PAGES == WAL_SEG_SIZE / WAL_PAGE_SIZE - 1,
          "WAL_SEG_DATA_PAGES == pages per segment minus the header page");
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
     * continuation_len 16-17, flags 18, reserved 19, CRC 20-23. (It was
     * 32 bytes with data_len at 16-17 until data_len was dropped.) */
    uint16_t cl;
    uint64_t sl;
    uint32_t cs;
    memcpy(&sl, buf + 8, 8);
    memcpy(&cl, buf + 16, 2);
    memcpy(&cs, buf + 20, 4);
    CHECK(sl == hdr.start_lsn && cl == 1234 && buf[18] == hdr.flags && buf[19] == 0x5C,
          "fields sit at their documented offsets");
    CHECK(cs == crc32(buf, 20), "the CRC at 20-23 covers bytes 0-19");
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

/* A record body far past WAL_MAX_ROW_BODY must round-trip. This used to
 * be impossible: serialize/deserialize copied header+body into a stack
 * scratch sized `40 + WAL_MAX_ROW_BODY` (4020; the constant is gone
 * now), so any LARGE_WAL-sized body
 * (or a record spanning pages) overran it. Both now fold the two spans in with crc32_update instead,
 * with no scratch and no size ceiling. */
static void test_record_header_large_body(void)
{
    printf("\n[test_record_header_large_body]\n");

    size_t body_len = 400 * 1024;          /* ~100x the old 4020-byte WAL_MAX_ROW_BODY */
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

/* patch_flags rewrites the flags byte of an already-serialized record
 * and recomputes its checksum, so the record still validates. This is
 * what let the LARGE_WAL writer mark a record as spanning pages at
 * pack time without mutating the caller's const blob. Its only caller
 * left with large_wal; whether it stays is decided in T-039. */
/* pack writes the same 44 header bytes serialize does, without touching
 * the body — the ring copies a spanning record's body in pieces. */
static void test_record_header_pack(void)
{
    printf("\n[test_record_header_pack]\n");
    uint8_t body[500];
    for (size_t i = 0; i < sizeof(body); i++) body[i] = (uint8_t)(i * 3);

    WalRecordHeader a, b;
    memset(&a, 0, sizeof(a));
    a.lsn = 777; a.txn_id = 9; a.rec_type = WAL_REC_INSERT;
    a.total_len = WAL_RECORD_HEADER_SIZE + sizeof(body);
    b = a;

    uint8_t full[WAL_RECORD_HEADER_SIZE + sizeof(body)];
    uint8_t packed[WAL_RECORD_HEADER_SIZE];
    wal_record_header_serialize(&a, body, sizeof(body), full);
    wal_record_header_pack(&b, body, sizeof(body), packed);
    CHECK(memcmp(full, packed, WAL_RECORD_HEADER_SIZE) == 0, "pack's 44 bytes equal serialize's header");
    CHECK(a.checksum == b.checksum, "both stamp the same checksum");

    WalRecordHeader out;
    memcpy(full, packed, WAL_RECORD_HEADER_SIZE);
    CHECK(wal_record_header_deserialize(full, sizeof(body), &out) == MYDB_OK,
          "a packed header followed by the body validates");
}

static void test_record_header_patch_flags(void)
{
    printf("\n[test_record_header_patch_flags]\n");

    uint8_t body[300];
    memset(body, 0xC3, sizeof(body));
    uint8_t buf[WAL_RECORD_HEADER_SIZE + sizeof(body)];

    WalRecordHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.lsn       = 77;
    hdr.total_len = WAL_RECORD_HEADER_SIZE + sizeof(body);
    hdr.rec_type  = WAL_REC_SCHEMA_UPDATE;
    wal_record_header_serialize(&hdr, body, sizeof(body), buf);

    WalRecordHeader before;
    CHECK(wal_record_header_deserialize(buf, sizeof(body), &before) == MYDB_OK,
          "the record validates before patching");
    CHECK(before.flags == 0, "flags start clear");

    wal_record_header_patch_flags(buf, buf + WAL_RECORD_HEADER_SIZE, sizeof(body),
                                   WAL_RECORD_FLAG_CONTINUES_ON_NEW_PAGE);

    WalRecordHeader after;
    CHECK(wal_record_header_deserialize(buf, sizeof(body), &after) == MYDB_OK,
          "the record STILL validates after patching — the checksum was recomputed");
    CHECK((after.flags & WAL_RECORD_FLAG_CONTINUES_ON_NEW_PAGE) != 0, "the flag bit is set");
    CHECK(after.lsn == 77 && after.total_len == hdr.total_len,
          "every other header field is untouched");
    CHECK(after.checksum != before.checksum, "the checksum genuinely changed");
    CHECK(memcmp(buf + WAL_RECORD_HEADER_SIZE, body, sizeof(body)) == 0,
          "the body was not touched");
}

/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_wal_types ===\n");

    test_size_constants();
    test_record_header_large_body();
    test_record_header_pack();
    test_record_header_patch_flags();
    test_page_header_round_trip();
    test_page_header_detects_tamper();
    test_page_header_wrong_file_type();
    test_segment_header_round_trip();
    test_segment_header_detects_tamper();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
