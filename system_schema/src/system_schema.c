#include "system_schema.h"
#include "file_header.h"
#include "checksum.h"

#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <stdio.h>

/* ====================================================================
 *  On-disk layouts
 *
 *  users.mydb (USERS_FILE_SIZE = 8192)
 *  ----------------------------------
 *    0..7    : FileHeaderId (magic + version + file_type=USERS)
 *    8..11   : next_user_id (uint32)
 *    12      : num_users (uint8)
 *    13..63  : reserved (51 B, zero)
 *    64..7743: 32 x UserSlot (240 B each)
 *    7744..8187: reserved (444 B)
 *    8188..8191: CRC-32C checksum over bytes 0..8187
 *
 *  UserSlot on-disk (240 B):
 *    0..3     : user_id (uint32)
 *    4..35    : username (32 B, NUL-padded)
 *    36..67   : password_hash (32 B raw)
 *    68..83   : password_salt (16 B raw)
 *    84       : hash_algorithm (uint8)
 *    85       : is_active (uint8)
 *    86       : is_valid (uint8)
 *    87       : _pad (uint8, zero)
 *    88..95   : created_at (uint64)
 *    96..103  : last_login (uint64)
 *    104..239 : reserved (136 B)
 *
 *  privileges.mydb (PRIVILEGES_FILE_SIZE = 16384)
 *  ----------------------------------------------
 *    0..7     : FileHeaderId (file_type=PRIVILEGES)
 *    8..11    : next_privilege_id (uint32)
 *    12..13   : num_privileges (uint16)
 *    14..63   : reserved (50 B)
 *    64..15423: 256 x PrivilegeSlot (60 B each)
 *    15424..16379: reserved (956 B)
 *    16380..16383: CRC-32C checksum over bytes 0..16379
 *
 *  PrivilegeSlot on-disk (60 B):
 *    0..3   : privilege_id (uint32)
 *    4..7   : grantee_id (uint32)
 *    8..11  : partition_id (uint32)
 *    12..15 : granted_by (uint32)
 *    16..23 : granted_at (uint64)
 *    24..55 : schema_name (32 B, NUL-padded)
 *    56     : is_valid (uint8)
 *    57..59 : _reserved (3 B)
 * ==================================================================== */

#define US_HEADER_SIZE         64
#define US_SLOT_OFFSET         64
#define US_SLOT_SIZE          240
#define US_CHECKSUM_OFFSET   8188

#define PR_HEADER_SIZE         64
#define PR_SLOT_OFFSET         64
#define PR_SLOT_SIZE           60
#define PR_CHECKSUM_OFFSET  16380

#define HASH_EMPTY  ((int16_t)-1)


/* ------------------------------------------------------------------ */
/*  Compile-time sanity                                               */
/* ------------------------------------------------------------------ */
_Static_assert(US_SLOT_OFFSET + USERS_MAX_SLOTS * US_SLOT_SIZE
               <= US_CHECKSUM_OFFSET,
               "users.mydb slot region overruns checksum trailer");
_Static_assert(PR_SLOT_OFFSET + PRIVILEGES_MAX_SLOTS * PR_SLOT_SIZE
               <= PR_CHECKSUM_OFFSET,
               "privileges.mydb slot region overruns checksum trailer");
_Static_assert((USERS_HASH_CAPACITY      & (USERS_HASH_CAPACITY - 1))      == 0,
               "USERS_HASH_CAPACITY must be a power of two");
_Static_assert((PRIVILEGES_HASH_CAPACITY & (PRIVILEGES_HASH_CAPACITY - 1)) == 0,
               "PRIVILEGES_HASH_CAPACITY must be a power of two");


/* ====================================================================
 *  Small helpers
 * ==================================================================== */

static uint64_t now_yyyymmddhhmmss(void)
{
    time_t t = time(NULL);
    struct tm tm;
    localtime_r(&t, &tm);
    return (uint64_t)(tm.tm_year + 1900) * 10000000000ULL
         + (uint64_t)(tm.tm_mon + 1)     * 100000000ULL
         + (uint64_t)tm.tm_mday          * 1000000ULL
         + (uint64_t)tm.tm_hour          * 10000ULL
         + (uint64_t)tm.tm_min           * 100ULL
         + (uint64_t)tm.tm_sec;
}

/* Build "<root_dir>/system_schema/<name>". Returns 0 on success,
 * -1 if the resulting path would overflow `out`. */
static int join_path(char *out, size_t cap,
                     const char *root_dir, const char *name)
{
    int n = snprintf(out, cap, "%s/system_schema/%s", root_dir, name);
    return (n < 0 || (size_t)n >= cap) ? -1 : 0;
}


/* ====================================================================
 *  USERS file — marshalling
 * ==================================================================== */

static void user_serialize(uint8_t *buf, const UserSlot *u)
{
    memcpy(buf + 0,   &u->user_id,        4);
    memcpy(buf + 4,    u->username,      32);
    memcpy(buf + 36,   u->password_hash, USER_PASSWORD_HASH_LEN);
    memcpy(buf + 68,   u->password_salt, USER_PASSWORD_SALT_LEN);
    buf[84] = u->hash_algorithm;
    buf[85] = u->is_active;
    buf[86] = u->is_valid;
    buf[87] = 0;
    memcpy(buf + 88,  &u->created_at, 8);
    memcpy(buf + 96,  &u->last_login, 8);
    /* bytes 104..239 zeroed by caller's memset */
}

static void user_deserialize(const uint8_t *buf, UserSlot *u)
{
    memcpy(&u->user_id,        buf + 0,   4);
    memcpy(u->username,        buf + 4,  32);
    u->username[MAX_USERNAME - 1] = '\0';                   /* defensive NUL */
    memcpy(u->password_hash,   buf + 36, USER_PASSWORD_HASH_LEN);
    memcpy(u->password_salt,   buf + 68, USER_PASSWORD_SALT_LEN);
    u->hash_algorithm = buf[84];
    u->is_active      = buf[85];
    u->is_valid       = buf[86];
    memcpy(&u->created_at, buf + 88, 8);
    memcpy(&u->last_login, buf + 96, 8);
}

/* Pack the in-memory UsersFile into an 8 KB buffer + trailer checksum. */
static void users_pack(const UsersFile *uf, uint8_t *buf)
{
    memset(buf, 0, USERS_FILE_SIZE);
    file_header_write_id(buf, FILETYPE_USERS);
    memcpy(buf + 8, &uf->next_user_id, 4);
    buf[12] = uf->num_users;
    for (int i = 0; i < USERS_MAX_SLOTS; i++) {
        user_serialize(buf + US_SLOT_OFFSET + i * US_SLOT_SIZE,
                       &uf->slots[i]);
    }
    uint32_t cs = crc32c(buf, US_CHECKSUM_OFFSET);
    memcpy(buf + US_CHECKSUM_OFFSET, &cs, 4);
}

/* Unpack and validate. Touches uf->slots, uf->next_user_id,
 * uf->num_users. Caller fills fd / path / hash map. */
static int users_unpack(const uint8_t *buf, UsersFile *uf)
{
    int rc = file_header_check_id(buf, FILETYPE_USERS);
    if (rc != MYDB_OK) return rc;

    uint32_t stored;
    memcpy(&stored, buf + US_CHECKSUM_OFFSET, 4);
    if (stored != crc32c(buf, US_CHECKSUM_OFFSET))
        return MYDB_ERR_BAD_CHECKSUM;

    memcpy(&uf->next_user_id, buf + 8, 4);
    uf->num_users = buf[12];
    for (int i = 0; i < USERS_MAX_SLOTS; i++) {
        user_deserialize(buf + US_SLOT_OFFSET + i * US_SLOT_SIZE,
                         &uf->slots[i]);
    }
    return MYDB_OK;
}

/* Write current state to disk + fsync. */
static int users_save(UsersFile *uf)
{
    if (!uf || uf->fd < 0) return MYDB_ERR;
    uint8_t buf[USERS_FILE_SIZE];
    users_pack(uf, buf);
    if (pwrite(uf->fd, buf, USERS_FILE_SIZE, 0) != (ssize_t)USERS_FILE_SIZE)
        return MYDB_ERR;
    if (fsync(uf->fd) < 0) return MYDB_ERR;
    return MYDB_OK;
}


/* ====================================================================
 *  USERS file — username hash map (open addressing, linear probing)
 * ==================================================================== */

static uint32_t name_hash(const char *username)
{
    /* Hash up to MAX_USERNAME bytes, stopping at the first NUL. */
    size_t len = strnlen(username, MAX_USERNAME);
    return crc32c(username, len);
}

/* Probe the username index for `username`. Returns the bucket position
 * either holding a matching slot (out_slot != -1) or the first empty
 * bucket along the probe chain (out_slot == -1). */
static int users_probe(const UsersFile *uf, const char *username,
                       int *out_pos, int *out_slot)
{
    uint32_t h    = name_hash(username);
    uint32_t mask = USERS_HASH_CAPACITY - 1;
    uint32_t pos  = h & mask;

    for (uint32_t i = 0; i < USERS_HASH_CAPACITY; i++) {
        uint32_t p = (pos + i) & mask;
        int16_t  s = uf->name_idx[p].slot;
        if (s == HASH_EMPTY) {
            *out_pos = (int)p;
            *out_slot = -1;
            return MYDB_OK;
        }
        if (strncmp(uf->slots[s].username, username, MAX_USERNAME) == 0) {
            *out_pos = (int)p;
            *out_slot = s;
            return MYDB_OK;
        }
    }
    /* Table full — should not happen because capacity > USERS_MAX_SLOTS. */
    return MYDB_ERR_FULL;
}

/* Insert (slot_index, username) into the empty bucket on the probe
 * chain. Caller has already verified that the username does not exist. */
static void users_index_insert(UsersFile *uf, int16_t slot_index)
{
    const char *username = uf->slots[slot_index].username;
    uint32_t h    = name_hash(username);
    uint32_t mask = USERS_HASH_CAPACITY - 1;
    uint32_t pos  = h & mask;
    for (uint32_t i = 0; i < USERS_HASH_CAPACITY; i++) {
        uint32_t p = (pos + i) & mask;
        if (uf->name_idx[p].slot == HASH_EMPTY) {
            uf->name_idx[p].slot = slot_index;
            return;
        }
    }
    /* Unreachable given the capacity invariant. */
}

/* Rebuild the username index from the slots array. Used at open time
 * and after any delete (cheapest correct way to maintain a probe chain
 * at this scale — 64 buckets). */
static void users_index_rebuild(UsersFile *uf)
{
    for (int i = 0; i < USERS_HASH_CAPACITY; i++)
        uf->name_idx[i].slot = HASH_EMPTY;
    for (int i = 0; i < USERS_MAX_SLOTS; i++) {
        if (uf->slots[i].is_valid)
            users_index_insert(uf, (int16_t)i);
    }
}


/* ====================================================================
 *  PRIVILEGES file — marshalling
 * ==================================================================== */

static void priv_serialize(uint8_t *buf, const PrivilegeSlot *p)
{
    memcpy(buf + 0,  &p->privilege_id, 4);
    memcpy(buf + 4,  &p->grantee_id,   4);
    memcpy(buf + 8,  &p->partition_id, 4);
    memcpy(buf + 12, &p->granted_by,   4);
    memcpy(buf + 16, &p->granted_at,   8);
    memcpy(buf + 24,  p->schema_name, 32);
    buf[56] = p->is_valid;
    /* bytes 57..59 zeroed by caller */
}

static void priv_deserialize(const uint8_t *buf, PrivilegeSlot *p)
{
    memcpy(&p->privilege_id, buf + 0,  4);
    memcpy(&p->grantee_id,   buf + 4,  4);
    memcpy(&p->partition_id, buf + 8,  4);
    memcpy(&p->granted_by,   buf + 12, 4);
    memcpy(&p->granted_at,   buf + 16, 8);
    memcpy(p->schema_name,   buf + 24, 32);
    p->schema_name[31] = '\0';                              /* defensive NUL */
    p->is_valid = buf[56];
}

static void priv_pack(const PrivilegesFile *pf, uint8_t *buf)
{
    memset(buf, 0, PRIVILEGES_FILE_SIZE);
    file_header_write_id(buf, FILETYPE_PRIVILEGES);
    memcpy(buf + 8,  &pf->next_privilege_id, 4);
    memcpy(buf + 12, &pf->num_privileges,    2);
    for (int i = 0; i < PRIVILEGES_MAX_SLOTS; i++) {
        priv_serialize(buf + PR_SLOT_OFFSET + i * PR_SLOT_SIZE,
                       &pf->slots[i]);
    }
    uint32_t cs = crc32c(buf, PR_CHECKSUM_OFFSET);
    memcpy(buf + PR_CHECKSUM_OFFSET, &cs, 4);
}

static int priv_unpack(const uint8_t *buf, PrivilegesFile *pf)
{
    int rc = file_header_check_id(buf, FILETYPE_PRIVILEGES);
    if (rc != MYDB_OK) return rc;

    uint32_t stored;
    memcpy(&stored, buf + PR_CHECKSUM_OFFSET, 4);
    if (stored != crc32c(buf, PR_CHECKSUM_OFFSET))
        return MYDB_ERR_BAD_CHECKSUM;

    memcpy(&pf->next_privilege_id, buf + 8,  4);
    memcpy(&pf->num_privileges,    buf + 12, 2);
    for (int i = 0; i < PRIVILEGES_MAX_SLOTS; i++) {
        priv_deserialize(buf + PR_SLOT_OFFSET + i * PR_SLOT_SIZE,
                         &pf->slots[i]);
    }
    return MYDB_OK;
}

static int privs_save(PrivilegesFile *pf)
{
    if (!pf || pf->fd < 0) return MYDB_ERR;
    uint8_t buf[PRIVILEGES_FILE_SIZE];
    priv_pack(pf, buf);
    if (pwrite(pf->fd, buf, PRIVILEGES_FILE_SIZE, 0)
        != (ssize_t)PRIVILEGES_FILE_SIZE) return MYDB_ERR;
    if (fsync(pf->fd) < 0) return MYDB_ERR;
    return MYDB_OK;
}


/* ====================================================================
 *  PRIVILEGES — composite-key hash map
 * ==================================================================== */

/* Hash the composite key (grantee_id, partition_id, schema_name) by
 * packing the integers and the (NUL-truncated) schema name into a
 * scratch buffer and running crc32c over the whole thing. */
static uint32_t priv_key_hash(uint32_t grantee_id, uint32_t partition_id,
                              const char *schema_name)
{
    uint8_t buf[8 + 32];
    memcpy(buf + 0, &grantee_id,   4);
    memcpy(buf + 4, &partition_id, 4);
    size_t len = strnlen(schema_name, 32);
    memcpy(buf + 8, schema_name, len);
    return crc32c(buf, 8 + len);
}

static int priv_key_match(const PrivilegeSlot *s,
                          uint32_t gid, uint32_t pid, const char *sn)
{
    return s->grantee_id   == gid
        && s->partition_id == pid
        && strncmp(s->schema_name, sn, 32) == 0;
}

static int privs_probe(const PrivilegesFile *pf,
                       uint32_t gid, uint32_t pid, const char *sn,
                       int *out_pos, int *out_slot)
{
    uint32_t h    = priv_key_hash(gid, pid, sn);
    uint32_t mask = PRIVILEGES_HASH_CAPACITY - 1;
    uint32_t pos  = h & mask;

    for (uint32_t i = 0; i < PRIVILEGES_HASH_CAPACITY; i++) {
        uint32_t p = (pos + i) & mask;
        int16_t  s = pf->key_idx[p].slot;
        if (s == HASH_EMPTY) {
            *out_pos = (int)p;
            *out_slot = -1;
            return MYDB_OK;
        }
        if (priv_key_match(&pf->slots[s], gid, pid, sn)) {
            *out_pos = (int)p;
            *out_slot = s;
            return MYDB_OK;
        }
    }
    return MYDB_ERR_FULL;
}

static void privs_index_insert(PrivilegesFile *pf, int16_t slot_index)
{
    const PrivilegeSlot *s = &pf->slots[slot_index];
    uint32_t h    = priv_key_hash(s->grantee_id, s->partition_id, s->schema_name);
    uint32_t mask = PRIVILEGES_HASH_CAPACITY - 1;
    uint32_t pos  = h & mask;
    for (uint32_t i = 0; i < PRIVILEGES_HASH_CAPACITY; i++) {
        uint32_t p = (pos + i) & mask;
        if (pf->key_idx[p].slot == HASH_EMPTY) {
            pf->key_idx[p].slot = slot_index;
            return;
        }
    }
}

static void privs_index_rebuild(PrivilegesFile *pf)
{
    for (int i = 0; i < PRIVILEGES_HASH_CAPACITY; i++)
        pf->key_idx[i].slot = HASH_EMPTY;
    for (int i = 0; i < PRIVILEGES_MAX_SLOTS; i++) {
        if (pf->slots[i].is_valid)
            privs_index_insert(pf, (int16_t)i);
    }
}


/* ====================================================================
 *  Aggregate lifecycle
 * ==================================================================== */

int system_schema_create(const char *root_dir, SystemSchema *out)
{
    if (!root_dir || !out) return MYDB_ERR;

    char users_path[256], priv_path[256];
    if (join_path(users_path, sizeof(users_path), root_dir, "users.mydb") < 0)
        return MYDB_ERR;
    if (join_path(priv_path,  sizeof(priv_path),  root_dir, "privileges.mydb") < 0)
        return MYDB_ERR;

    memset(out, 0, sizeof(*out));
    out->users.fd = -1;
    out->privileges.fd = -1;

    /* ---- users.mydb ---- */
    int ufd = open(users_path, O_CREAT | O_EXCL | O_RDWR, 0644);
    if (ufd < 0) return MYDB_ERR;
    if (ftruncate(ufd, USERS_FILE_SIZE) < 0) {
        close(ufd); unlink(users_path); return MYDB_ERR;
    }
    out->users.fd = ufd;
    strncpy(out->users.path, users_path, sizeof(out->users.path) - 1);
    out->users.next_user_id = 1;        /* user_id starts at 1 */
    out->users.num_users    = 0;
    users_index_rebuild(&out->users);   /* all empty */
    if (users_save(&out->users) != MYDB_OK) {
        close(ufd); unlink(users_path); out->users.fd = -1;
        return MYDB_ERR;
    }

    /* ---- privileges.mydb ---- */
    int pfd = open(priv_path, O_CREAT | O_EXCL | O_RDWR, 0644);
    if (pfd < 0) {
        close(ufd); unlink(users_path); out->users.fd = -1;
        return MYDB_ERR;
    }
    if (ftruncate(pfd, PRIVILEGES_FILE_SIZE) < 0) {
        close(ufd); unlink(users_path); out->users.fd = -1;
        close(pfd); unlink(priv_path);
        return MYDB_ERR;
    }
    out->privileges.fd = pfd;
    strncpy(out->privileges.path, priv_path, sizeof(out->privileges.path) - 1);
    out->privileges.next_privilege_id = 1;
    out->privileges.num_privileges    = 0;
    privs_index_rebuild(&out->privileges);
    if (privs_save(&out->privileges) != MYDB_OK) {
        close(ufd); unlink(users_path); out->users.fd = -1;
        close(pfd); unlink(priv_path);  out->privileges.fd = -1;
        return MYDB_ERR;
    }
    return MYDB_OK;
}

int system_schema_open(const char *root_dir, SystemSchema *out)
{
    if (!root_dir || !out) return MYDB_ERR;

    char users_path[256], priv_path[256];
    if (join_path(users_path, sizeof(users_path), root_dir, "users.mydb") < 0)
        return MYDB_ERR;
    if (join_path(priv_path,  sizeof(priv_path),  root_dir, "privileges.mydb") < 0)
        return MYDB_ERR;

    memset(out, 0, sizeof(*out));
    out->users.fd = -1;
    out->privileges.fd = -1;

    /* ---- users.mydb ---- */
    int ufd = open(users_path, O_RDWR);
    if (ufd < 0) return MYDB_ERR;
    {
        uint8_t buf[USERS_FILE_SIZE];
        if (pread(ufd, buf, USERS_FILE_SIZE, 0) != (ssize_t)USERS_FILE_SIZE) {
            close(ufd); return MYDB_ERR;
        }
        int rc = users_unpack(buf, &out->users);
        if (rc != MYDB_OK) { close(ufd); return rc; }
    }
    out->users.fd = ufd;
    strncpy(out->users.path, users_path, sizeof(out->users.path) - 1);
    users_index_rebuild(&out->users);

    /* ---- privileges.mydb ---- */
    int pfd = open(priv_path, O_RDWR);
    if (pfd < 0) { close(ufd); out->users.fd = -1; return MYDB_ERR; }
    {
        uint8_t buf[PRIVILEGES_FILE_SIZE];
        if (pread(pfd, buf, PRIVILEGES_FILE_SIZE, 0)
            != (ssize_t)PRIVILEGES_FILE_SIZE) {
            close(ufd); close(pfd); out->users.fd = -1;
            return MYDB_ERR;
        }
        int rc = priv_unpack(buf, &out->privileges);
        if (rc != MYDB_OK) {
            close(ufd); close(pfd); out->users.fd = -1;
            return rc;
        }
    }
    out->privileges.fd = pfd;
    strncpy(out->privileges.path, priv_path, sizeof(out->privileges.path) - 1);
    privs_index_rebuild(&out->privileges);

    return MYDB_OK;
}

int system_schema_close(SystemSchema *ss)
{
    if (!ss) return MYDB_ERR;
    int rc = MYDB_OK;
    if (ss->users.fd >= 0) {
        if (close(ss->users.fd) < 0) rc = MYDB_ERR;
        ss->users.fd = -1;
    }
    if (ss->privileges.fd >= 0) {
        if (close(ss->privileges.fd) < 0) rc = MYDB_ERR;
        ss->privileges.fd = -1;
    }
    return rc;
}


/* ====================================================================
 *  USERS — public API
 * ==================================================================== */

int users_insert(UsersFile *uf, const UserSlot *slot, uint32_t *out_user_id)
{
    if (!uf || !slot || !out_user_id) return MYDB_ERR;
    if (slot->username[0] == '\0')    return MYDB_ERR;
    if (uf->num_users >= USERS_MAX_SLOTS) return MYDB_ERR_FULL;

    /* Reject duplicate username via hash-map probe. */
    int pos = -1, found = -1;
    int rc = users_probe(uf, slot->username, &pos, &found);
    if (rc != MYDB_OK) return rc;
    if (found != -1) return MYDB_ERR_DUPLICATE;

    /* Find the first free slot in the array. */
    int free_slot = -1;
    for (int i = 0; i < USERS_MAX_SLOTS; i++) {
        if (!uf->slots[i].is_valid) { free_slot = i; break; }
    }
    if (free_slot < 0) return MYDB_ERR_FULL;

    UserSlot *dst = &uf->slots[free_slot];
    *dst = *slot;
    dst->user_id  = uf->next_user_id++;
    dst->is_valid = 1;
    if (dst->created_at == 0) dst->created_at = now_yyyymmddhhmmss();
    /* `pos` is the empty bucket we landed on during probing — reuse it. */
    uf->name_idx[pos].slot = (int16_t)free_slot;
    uf->num_users++;

    *out_user_id = dst->user_id;
    return users_save(uf);
}

int users_find_by_name(const UsersFile *uf, const char *username, UserSlot *out)
{
    if (!uf || !username || !out) return MYDB_ERR;
    int pos = -1, found = -1;
    int rc = users_probe(uf, username, &pos, &found);
    if (rc != MYDB_OK) return rc;
    if (found == -1) return MYDB_ERR_NOT_FOUND;
    *out = uf->slots[found];
    return MYDB_OK;
}

int users_find_by_id(const UsersFile *uf, uint32_t user_id, UserSlot *out)
{
    if (!uf || !out) return MYDB_ERR;
    /* Linear scan — by-id is not on the hot login path (Option B). */
    for (int i = 0; i < USERS_MAX_SLOTS; i++) {
        if (uf->slots[i].is_valid && uf->slots[i].user_id == user_id) {
            *out = uf->slots[i];
            return MYDB_OK;
        }
    }
    return MYDB_ERR_NOT_FOUND;
}

int users_update(UsersFile *uf, const UserSlot *new_slot)
{
    if (!uf || !new_slot) return MYDB_ERR;

    int target = -1;
    for (int i = 0; i < USERS_MAX_SLOTS; i++) {
        if (uf->slots[i].is_valid && uf->slots[i].user_id == new_slot->user_id) {
            target = i; break;
        }
    }
    if (target < 0) return MYDB_ERR_NOT_FOUND;

    /* If the username changed, ensure the new one isn't taken by
     * someone else. */
    if (strncmp(uf->slots[target].username, new_slot->username, MAX_USERNAME) != 0) {
        int pos = -1, found = -1;
        int rc = users_probe(uf, new_slot->username, &pos, &found);
        if (rc != MYDB_OK) return rc;
        if (found != -1 && found != target) return MYDB_ERR_DUPLICATE;
    }

    UserSlot saved = *new_slot;
    saved.is_valid = 1;
    uf->slots[target] = saved;
    users_index_rebuild(uf);   /* username may have changed */
    return users_save(uf);
}

int users_delete(UsersFile *uf, uint32_t user_id)
{
    if (!uf) return MYDB_ERR;
    for (int i = 0; i < USERS_MAX_SLOTS; i++) {
        if (uf->slots[i].is_valid && uf->slots[i].user_id == user_id) {
            memset(&uf->slots[i], 0, sizeof(uf->slots[i]));
            if (uf->num_users > 0) uf->num_users--;
            users_index_rebuild(uf);
            return users_save(uf);
        }
    }
    return MYDB_ERR_NOT_FOUND;
}


/* ====================================================================
 *  PRIVILEGES — public API
 * ==================================================================== */

int privileges_insert(PrivilegesFile *pf, const PrivilegeSlot *slot,
                      uint32_t *out_privilege_id)
{
    if (!pf || !slot || !out_privilege_id) return MYDB_ERR;
    if (slot->schema_name[0] == '\0')      return MYDB_ERR;
    if (pf->num_privileges >= PRIVILEGES_MAX_SLOTS) return MYDB_ERR_FULL;

    int pos = -1, found = -1;
    int rc = privs_probe(pf, slot->grantee_id, slot->partition_id,
                         slot->schema_name, &pos, &found);
    if (rc != MYDB_OK) return rc;
    if (found != -1) return MYDB_ERR_DUPLICATE;

    int free_slot = -1;
    for (int i = 0; i < PRIVILEGES_MAX_SLOTS; i++) {
        if (!pf->slots[i].is_valid) { free_slot = i; break; }
    }
    if (free_slot < 0) return MYDB_ERR_FULL;

    PrivilegeSlot *dst = &pf->slots[free_slot];
    *dst = *slot;
    dst->privilege_id = pf->next_privilege_id++;
    dst->is_valid = 1;
    if (dst->granted_at == 0) dst->granted_at = now_yyyymmddhhmmss();
    pf->key_idx[pos].slot = (int16_t)free_slot;
    pf->num_privileges++;

    *out_privilege_id = dst->privilege_id;
    return privs_save(pf);
}

int privileges_find(const PrivilegesFile *pf,
                    uint32_t grantee_id, uint32_t partition_id,
                    const char *schema_name, PrivilegeSlot *out)
{
    if (!pf || !schema_name || !out) return MYDB_ERR;
    int pos = -1, found = -1;
    int rc = privs_probe(pf, grantee_id, partition_id, schema_name,
                         &pos, &found);
    if (rc != MYDB_OK) return rc;
    if (found == -1) return MYDB_ERR_NOT_FOUND;
    *out = pf->slots[found];
    return MYDB_OK;
}

int privileges_find_by_id(const PrivilegesFile *pf, uint32_t privilege_id,
                          PrivilegeSlot *out)
{
    if (!pf || !out) return MYDB_ERR;
    for (int i = 0; i < PRIVILEGES_MAX_SLOTS; i++) {
        if (pf->slots[i].is_valid && pf->slots[i].privilege_id == privilege_id) {
            *out = pf->slots[i];
            return MYDB_OK;
        }
    }
    return MYDB_ERR_NOT_FOUND;
}

int privileges_delete(PrivilegesFile *pf,
                      uint32_t grantee_id, uint32_t partition_id,
                      const char *schema_name)
{
    if (!pf || !schema_name) return MYDB_ERR;
    int pos = -1, found = -1;
    int rc = privs_probe(pf, grantee_id, partition_id, schema_name,
                         &pos, &found);
    if (rc != MYDB_OK) return rc;
    if (found == -1) return MYDB_ERR_NOT_FOUND;

    memset(&pf->slots[found], 0, sizeof(pf->slots[found]));
    if (pf->num_privileges > 0) pf->num_privileges--;
    privs_index_rebuild(pf);
    return privs_save(pf);
}
