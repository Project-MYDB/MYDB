#include <stdio.h>
#include <string.h>

#include "checksum.h"

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

/* ------------------------------------------------------------------ */

/* The standard CRC-32C check value. CRC-32 IEEE would give 0xCBF43926
 * here, so this also proves the polynomial is Castagnoli, not IEEE. */
static void test_check_value(void)
{
    printf("\n[test_check_value]\n");
    CHECK(crc32c("123456789", 9) == 0xE3069283u, "crc32c(\"123456789\") == 0xE3069283");
    CHECK(crc32c("123456789", 9) != 0xCBF43926u, "not the CRC-32 IEEE value");
    CHECK(crc32c("", 0) == 0x00000000u,          "crc32c of no bytes is 0");
}

/* Splitting the input anywhere gives the same checksum as one call. */
static void test_incremental(void)
{
    printf("\n[test_incremental]\n");
    uint8_t buf[1000];
    for (size_t i = 0; i < sizeof(buf); i++) buf[i] = (uint8_t)(i * 7 + 3);

    uint32_t whole = crc32c(buf, sizeof(buf));
    int all_match = 1;
    for (size_t cut = 0; cut <= sizeof(buf); cut++) {
        uint32_t c = CRC32C_INIT;
        c = crc32c_update(c, buf, cut);
        c = crc32c_update(c, buf + cut, sizeof(buf) - cut);
        if (crc32c_final(c) != whole) { all_match = 0; break; }
    }
    CHECK(all_match, "two-part update equals one-shot crc32c at every cut");
}

/* Reference CRC-32C worked bit by bit straight from the definition
 * (reflected, polynomial 0x82F63B78). It lives only here, as an
 * independent check on both product paths: the hardware one and the
 * table fallback. */
static uint32_t ref_crc32c_update(uint32_t crc, const uint8_t *p, size_t len)
{
    for (size_t i = 0; i < len; i++) {
        crc ^= p[i];
        for (int bit = 0; bit < 8; bit++)
            crc = (crc >> 1) ^ (0x82F63B78u & -(crc & 1u));
    }
    return crc;
}

/* Both paths match the reference for every length 0..300 (so every
 * 8/4/1-byte tail mix of the hardware path) at every start offset 0..7
 * (unaligned loads), over pseudo-random bytes, plus an unaligned 16 KB
 * data page. crc32c_update takes the hardware path on this CPU when it
 * can; crc32c_update_sw is the fallback, tested directly since this
 * machine would otherwise never run it. */
static void test_matches_reference(void)
{
    printf("\n[test_matches_reference]\n");
    printf("  crc32c path in use: %s\n",
           crc32c_uses_hardware() ? "hardware (SSE4.2 crc32)" : "software (table)");

    static uint8_t buf[16384 + 8];
    uint32_t x = 0x12345678u;
    for (size_t i = 0; i < sizeof(buf); i++) {
        x = x * 1103515245u + 12345u;
        buf[i] = (uint8_t)(x >> 16);
    }

    int all_match = 1;
    for (size_t off = 0; off < 8 && all_match; off++)
        for (size_t len = 0; len <= 300; len++) {
            if (crc32c_update(CRC32C_INIT, buf + off, len) !=
                ref_crc32c_update(CRC32C_INIT, buf + off, len)) { all_match = 0; break; }
        }
    CHECK(all_match, "crc32c_update matches the bit-by-bit reference for len 0..300, offsets 0..7");

    int sw_match = 1;
    for (size_t off = 0; off < 8 && sw_match; off++)
        for (size_t len = 0; len <= 300; len++) {
            if (crc32c_update_sw(CRC32C_INIT, buf + off, len) !=
                ref_crc32c_update(CRC32C_INIT, buf + off, len)) { sw_match = 0; break; }
        }
    CHECK(sw_match, "crc32c_update_sw (fallback) matches the reference for len 0..300, offsets 0..7");

    CHECK(crc32c_update(CRC32C_INIT, buf + 3, 16384) == ref_crc32c_update(CRC32C_INIT, buf + 3, 16384),
          "crc32c_update matches the reference on an unaligned 16 KB page");
    CHECK(crc32c_update_sw(CRC32C_INIT, buf + 3, 16384) == ref_crc32c_update(CRC32C_INIT, buf + 3, 16384),
          "crc32c_update_sw matches the reference on an unaligned 16 KB page");
}

/* One flipped bit changes the checksum. */
static void test_detects_bit_flip(void)
{
    printf("\n[test_detects_bit_flip]\n");
    uint8_t buf[64];
    memset(buf, 0xAB, sizeof(buf));
    uint32_t before = crc32c(buf, sizeof(buf));
    buf[37] ^= 0x10;
    CHECK(crc32c(buf, sizeof(buf)) != before, "a flipped bit changes the checksum");
}

int main(void)
{
    printf("=== test_checksum ===\n");
    test_check_value();
    test_incremental();
    test_matches_reference();
    test_detects_bit_flip();
    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
