#ifndef CRYPTO_H
#define CRYPTO_H

#include <stddef.h>
#include <stdint.h>

/* ------------------------------------------------------------------ */
/*  Tiny crypto utilities used by the engine bootstrap + login flow   */
/*  and by the network client's challenge-response handshake.         */
/*                                                                    */
/*  No external dependencies (no libcrypto / openssl).  Lifted to a   */
/*  top-level module when the network client became a second consumer */
/*  (it must compute SHA-256(salt || password) with no engine inside  */
/*  it) — the lift policy this header had always anticipated.         */
/* ------------------------------------------------------------------ */

#define SHA256_DIGEST_LEN  32   /* matches USER_PASSWORD_HASH_LEN */
#define SALT_LEN           16   /* matches USER_PASSWORD_SALT_LEN */
#define MYDB_NONCE_LEN     32   /* server auth challenge nonce */

/* Compute SHA-256 over `len` bytes at `msg` and write the 32-byte
 * digest to `out`. Implementation follows FIPS-180-4 verbatim. */
void sha256(const void *msg, size_t len, uint8_t out[SHA256_DIGEST_LEN]);

/* Hash a password with a per-user salt:
 *
 *   out = SHA-256(salt || password)
 *
 * `password` is treated as a NUL-terminated C string (its NUL is
 * NOT included in the hash). Bit-wise stable so it can be persisted
 * and compared verbatim across runs. */
void crypto_hash_password(const char *password,
                          const uint8_t salt[SALT_LEN],
                          uint8_t out[SHA256_DIGEST_LEN]);

/* Read `len` cryptographically-random bytes from /dev/urandom into `buf`.
 * Returns 0 on success, -1 (MYDB_ERR) on failure (unavailable / short read). */
int  crypto_random_bytes(uint8_t *buf, size_t len);

/* Read SALT_LEN bytes from /dev/urandom into `salt`. Returns 0 on
 * success, -1 (MYDB_ERR) on failure (e.g. /dev/urandom unavailable
 * or short read).  Thin wrapper over crypto_random_bytes. */
int  crypto_random_salt(uint8_t salt[SALT_LEN]);

#endif /* CRYPTO_H */
