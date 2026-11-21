#include "crypto.h"
#include "common.h"

#include <fcntl.h>
#include <unistd.h>
#include <string.h>

/* ====================================================================
 *  SHA-256 — straight FIPS-180-4 implementation
 *
 *  Big-endian byte order on the wire / in the digest. All arithmetic
 *  is on 32-bit unsigned integers; rotates use the standard mask
 *  trick so the compiler emits a real ROTR instruction.
 * ==================================================================== */

static const uint32_t SHA256_K[64] = {
    0x428a2f98u, 0x71374491u, 0xb5c0fbcfu, 0xe9b5dba5u,
    0x3956c25bu, 0x59f111f1u, 0x923f82a4u, 0xab1c5ed5u,
    0xd807aa98u, 0x12835b01u, 0x243185beu, 0x550c7dc3u,
    0x72be5d74u, 0x80deb1feu, 0x9bdc06a7u, 0xc19bf174u,
    0xe49b69c1u, 0xefbe4786u, 0x0fc19dc6u, 0x240ca1ccu,
    0x2de92c6fu, 0x4a7484aau, 0x5cb0a9dcu, 0x76f988dau,
    0x983e5152u, 0xa831c66du, 0xb00327c8u, 0xbf597fc7u,
    0xc6e00bf3u, 0xd5a79147u, 0x06ca6351u, 0x14292967u,
    0x27b70a85u, 0x2e1b2138u, 0x4d2c6dfcu, 0x53380d13u,
    0x650a7354u, 0x766a0abbu, 0x81c2c92eu, 0x92722c85u,
    0xa2bfe8a1u, 0xa81a664bu, 0xc24b8b70u, 0xc76c51a3u,
    0xd192e819u, 0xd6990624u, 0xf40e3585u, 0x106aa070u,
    0x19a4c116u, 0x1e376c08u, 0x2748774cu, 0x34b0bcb5u,
    0x391c0cb3u, 0x4ed8aa4au, 0x5b9cca4fu, 0x682e6ff3u,
    0x748f82eeu, 0x78a5636fu, 0x84c87814u, 0x8cc70208u,
    0x90befffau, 0xa4506cebu, 0xbef9a3f7u, 0xc67178f2u,
};

static inline uint32_t rotr32(uint32_t x, unsigned n)
{
    return (x >> n) | (x << (32u - n));
}

static void sha256_compress(uint32_t state[8], const uint8_t block[64])
{
    uint32_t W[64];

    /* Big-endian load of the 16 message words. */
    for (int i = 0; i < 16; i++) {
        W[i] = ((uint32_t)block[i*4    ] << 24)
             | ((uint32_t)block[i*4 + 1] << 16)
             | ((uint32_t)block[i*4 + 2] <<  8)
             | ((uint32_t)block[i*4 + 3]);
    }
    /* Message schedule. */
    for (int i = 16; i < 64; i++) {
        uint32_t s0 = rotr32(W[i-15], 7) ^ rotr32(W[i-15], 18) ^ (W[i-15] >> 3);
        uint32_t s1 = rotr32(W[i-2], 17) ^ rotr32(W[i-2],  19) ^ (W[i-2] >> 10);
        W[i] = W[i-16] + s0 + W[i-7] + s1;
    }

    uint32_t a = state[0], b = state[1], c = state[2], d = state[3];
    uint32_t e = state[4], f = state[5], g = state[6], h = state[7];

    for (int i = 0; i < 64; i++) {
        uint32_t S1    = rotr32(e, 6) ^ rotr32(e, 11) ^ rotr32(e, 25);
        uint32_t ch    = (e & f) ^ (~e & g);
        uint32_t temp1 = h + S1 + ch + SHA256_K[i] + W[i];
        uint32_t S0    = rotr32(a, 2) ^ rotr32(a, 13) ^ rotr32(a, 22);
        uint32_t maj   = (a & b) ^ (a & c) ^ (b & c);
        uint32_t temp2 = S0 + maj;

        h = g; g = f; f = e;
        e = d + temp1;
        d = c; c = b; b = a;
        a = temp1 + temp2;
    }

    state[0] += a; state[1] += b; state[2] += c; state[3] += d;
    state[4] += e; state[5] += f; state[6] += g; state[7] += h;
}

void sha256(const void *msg, size_t len, uint8_t out[SHA256_DIGEST_LEN])
{
    uint32_t state[8] = {
        0x6a09e667u, 0xbb67ae85u, 0x3c6ef372u, 0xa54ff53au,
        0x510e527fu, 0x9b05688cu, 0x1f83d9abu, 0x5be0cd19u,
    };

    const uint8_t *p = (const uint8_t *)msg;
    size_t remaining = len;

    /* Process complete 64-byte blocks. */
    while (remaining >= 64) {
        sha256_compress(state, p);
        p += 64;
        remaining -= 64;
    }

    /* Final block(s) — copy the tail, append 0x80, zero-pad,
     * write the bit-length as a big-endian 64-bit suffix. */
    uint8_t tail[128] = {0};
    memcpy(tail, p, remaining);
    tail[remaining] = 0x80;

    /* If the tail+0x80+8-byte length doesn't fit in one 64-byte
     * block, use two. */
    size_t pad_block_len = (remaining + 1 <= 56) ? 64 : 128;
    uint64_t bit_len = (uint64_t)len * 8u;
    for (int i = 0; i < 8; i++) {
        tail[pad_block_len - 1 - i] = (uint8_t)(bit_len >> (i * 8));
    }
    sha256_compress(state, tail);
    if (pad_block_len == 128) sha256_compress(state, tail + 64);

    /* Big-endian store. */
    for (int i = 0; i < 8; i++) {
        out[i*4    ] = (uint8_t)(state[i] >> 24);
        out[i*4 + 1] = (uint8_t)(state[i] >> 16);
        out[i*4 + 2] = (uint8_t)(state[i] >>  8);
        out[i*4 + 3] = (uint8_t)(state[i]);
    }
}


/* ====================================================================
 *  Salt + password hashing
 * ==================================================================== */

void crypto_hash_password(const char *password,
                          const uint8_t salt[SALT_LEN],
                          uint8_t out[SHA256_DIGEST_LEN])
{
    /* SHA-256(salt || password) — salt first so an attacker who pre-
     * computes hashes of common passwords can't reuse them. */
    size_t pw_len = strlen(password);
    uint8_t buf[SALT_LEN + 256];          /* passwords are short; 256 is plenty */
    if (pw_len > sizeof(buf) - SALT_LEN) {
        pw_len = sizeof(buf) - SALT_LEN;  /* truncate defensively */
    }
    memcpy(buf, salt, SALT_LEN);
    memcpy(buf + SALT_LEN, password, pw_len);
    sha256(buf, SALT_LEN + pw_len, out);
}

int crypto_random_bytes(uint8_t *buf, size_t len)
{
    if (!buf) return MYDB_ERR;
    int fd = open("/dev/urandom", O_RDONLY);
    if (fd < 0) return MYDB_ERR;

    size_t got = 0;
    while (got < len) {
        ssize_t n = read(fd, buf + got, len - got);
        if (n <= 0) { close(fd); return MYDB_ERR; }
        got += (size_t)n;
    }
    close(fd);
    return MYDB_OK;
}

int crypto_random_salt(uint8_t salt[SALT_LEN])
{
    return crypto_random_bytes(salt, SALT_LEN);
}
