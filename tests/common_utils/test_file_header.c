#include <stdio.h>
#include <string.h>

#include "common.h"
#include "file_header.h"

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

/* ------------------------------------------------------------------ */

static void test_round_trip(void)
{
    printf("\n[test_round_trip]\n");
    uint8_t buf[16];
    memset(buf, 0, sizeof(buf));

    file_header_write_id(buf, FILETYPE_DATABASE);

    FileHeaderId id;
    file_header_read_id(buf, &id);

    CHECK(id.magic     == MYDB_MAGIC,           "magic round-trips");
    CHECK(id.version   == MYDB_FORMAT_VERSION,  "version round-trips");
    CHECK(id.file_type == FILETYPE_DATABASE,    "file_type round-trips");
}

static void test_check_id_passes(void)
{
    printf("\n[test_check_id_passes]\n");
    uint8_t buf[16] = {0};
    file_header_write_id(buf, FILETYPE_CATALOG);
    CHECK(file_header_check_id(buf, FILETYPE_CATALOG) == MYDB_OK,
          "check_id accepts a freshly-written buffer");
}

static void test_check_id_bad_magic(void)
{
    printf("\n[test_check_id_bad_magic]\n");
    uint8_t buf[16] = {0};
    file_header_write_id(buf, FILETYPE_DATABASE);
    buf[0] = 0xFF;  /* corrupt low byte of magic */
    CHECK(file_header_check_id(buf, FILETYPE_DATABASE) == MYDB_ERR_BAD_MAGIC,
          "bad magic returns MYDB_ERR_BAD_MAGIC");
}

static void test_check_id_bad_version(void)
{
    printf("\n[test_check_id_bad_version]\n");
    uint8_t buf[16] = {0};
    file_header_write_id(buf, FILETYPE_DATABASE);
    /* Force version to a value above MYDB_FORMAT_VERSION */
    uint16_t bogus = (uint16_t)(MYDB_FORMAT_VERSION + 1);
    memcpy(buf + 4, &bogus, 2);
    CHECK(file_header_check_id(buf, FILETYPE_DATABASE) == MYDB_ERR_BAD_VERSION,
          "future version returns MYDB_ERR_BAD_VERSION");
}

static void test_check_id_wrong_file_type(void)
{
    printf("\n[test_check_id_wrong_file_type]\n");
    uint8_t buf[16] = {0};
    file_header_write_id(buf, FILETYPE_DATABASE);
    CHECK(file_header_check_id(buf, FILETYPE_CATALOG) == MYDB_ERR_BAD_FILE_TYPE,
          "wrong file_type returns MYDB_ERR_BAD_FILE_TYPE");
}

static void test_distinguishes_all_file_types(void)
{
    printf("\n[test_distinguishes_all_file_types]\n");
    static const uint16_t types[] = {
        FILETYPE_DATABASE, FILETYPE_CATALOG, FILETYPE_SCHEMA,
        FILETYPE_RELATION, FILETYPE_USERS,   FILETYPE_PRIVILEGES
    };
    const int n = (int)(sizeof(types) / sizeof(types[0]));

    for (int written = 0; written < n; written++) {
        uint8_t buf[16] = {0};
        file_header_write_id(buf, types[written]);
        for (int probe = 0; probe < n; probe++) {
            int rc = file_header_check_id(buf, types[probe]);
            int expected = (written == probe) ? MYDB_OK : MYDB_ERR_BAD_FILE_TYPE;
            char msg[80];
            snprintf(msg, sizeof(msg),
                     "wrote=%u probe=%u → %s",
                     types[written], types[probe],
                     (expected == MYDB_OK) ? "OK" : "BAD_FILE_TYPE");
            CHECK(rc == expected, msg);
        }
    }
}

/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_file_header ===\n");

    test_round_trip();
    test_check_id_passes();
    test_check_id_bad_magic();
    test_check_id_bad_version();
    test_check_id_wrong_file_type();
    test_distinguishes_all_file_types();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
