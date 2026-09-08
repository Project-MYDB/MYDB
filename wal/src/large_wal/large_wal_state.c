#include "large_wal/large_wal_state.h"
#include "file_header.h"
#include "checksum.h"

#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

/* Wire format: [FileHeaderId(8)][flush_lsn(8)][checksum(4)] = 20 bytes. */
#define LARGE_WAL_STATE_FILE_SIZE     20
#define LARGE_WAL_STATE_CHECKSUM_OFF  16

static int pwrite_all(int fd, const void *buf, size_t n, off_t offset)
{
    const char *ptr = (const char *)buf;
    size_t total_written = 0;

    while (total_written < n) {
        ssize_t written = pwrite(fd,
                                 ptr + total_written,
                                 n - total_written,
                                 offset + total_written);

        if (written <= 0) {
            // Interrupted by a signal; retry the write
            if (written < 0 && errno == EINTR) {
                continue;
            }
            // Real error (e.g., ENOSPC for out-of-disk-space, EBADF) or unexpected 0 bytes
            return MYDB_ERR;
        }

        total_written += (size_t)written;
    }

    return MYDB_OK;
}

static int pread_all(int fd, void *buf, size_t n, off_t offset)
{
    char *ptr = (char *)buf;
    size_t total_read = 0;

    while (total_read < n) {
        ssize_t got = pread(fd,
                            ptr + total_read,
                            n - total_read,
                            offset + total_read);

        if (got < 0) {
            // Interrupted by a signal; retry the read
            if (errno == EINTR) {
                continue;
            }
            // Fatal read error (e.g., EIO, EBADF)
            return MYDB_ERR;
        }

        if (got == 0) {
            // Reached End-Of-File (EOF) before reading all n bytes
            return MYDB_ERR;
        }

        total_read += (size_t)got;
    }

    return MYDB_OK;
}

static int save(LargeWalState *st)
{
    uint8_t buf[LARGE_WAL_STATE_FILE_SIZE];
    file_header_write_id(buf, FILETYPE_LARGE_WAL_STATE);
    memcpy(buf + 8, &st->flush_lsn, 8);

    uint32_t cs = crc32(buf, LARGE_WAL_STATE_CHECKSUM_OFF);
    memcpy(buf + LARGE_WAL_STATE_CHECKSUM_OFF, &cs, 4);

    if (pwrite_all(st->fd, buf, LARGE_WAL_STATE_FILE_SIZE, 0) != MYDB_OK) return MYDB_ERR;
    /* fdatasync, not fsync: this file is a fixed LARGE_WAL_STATE_FILE_SIZE
     * from its very first save() onward (created via O_CREAT|O_EXCL, then
     * pwritten to that exact length once and never resized again), and
     * this runs on every submit() (a real hot path) — same reasoning as
     * the segment pools' own fdatasync switch: no essential size metadata
     * for fdatasync to need to flush beyond the data itself. */
    if (fdatasync(st->fd) < 0) return MYDB_ERR;
    return MYDB_OK;
}

int large_wal_state_open(LargeWalState *st, const char *wal_dir)
{
    if (!st || !wal_dir) return MYDB_ERR;
    memset(st, 0, sizeof(*st));
    st->fd = -1;
    if (pthread_mutex_init(&st->lock, NULL) != 0) return MYDB_ERR;
    snprintf(st->path, sizeof(st->path), "%s/large_wal_state.mydb", wal_dir);

    int fd = open(st->path, O_RDWR);
    if (fd >= 0) {
        uint8_t buf[LARGE_WAL_STATE_FILE_SIZE];
        if (pread_all(fd, buf, LARGE_WAL_STATE_FILE_SIZE, 0) != MYDB_OK) {
            close(fd);
            return MYDB_ERR;
        }

        int rc = file_header_check_id(buf, FILETYPE_LARGE_WAL_STATE);
        if (rc != MYDB_OK) { close(fd); return rc; }

        uint32_t stored;
        memcpy(&stored, buf + LARGE_WAL_STATE_CHECKSUM_OFF, 4);
        if (stored != crc32(buf, LARGE_WAL_STATE_CHECKSUM_OFF)) {
            close(fd);
            return MYDB_ERR_BAD_CHECKSUM;
        }

        memcpy(&st->flush_lsn, buf + 8, 8);
        st->fd = fd;
        return MYDB_OK;
    }
    if (errno != ENOENT) return MYDB_ERR;

    fd = open(st->path, O_RDWR | O_CREAT | O_EXCL, 0644);
    if (fd < 0) return MYDB_ERR;
    st->fd        = fd;
    st->flush_lsn = 0;

    int rc = save(st);
    if (rc != MYDB_OK) {
        close(fd);
        st->fd = -1;
        return rc;
    }
    return MYDB_OK;
}

int large_wal_state_close(LargeWalState *st)
{
    if (!st) return MYDB_ERR;
    if (st->fd >= 0) close(st->fd);
    st->fd = -1;
    pthread_mutex_destroy(&st->lock);
    return MYDB_OK;
}

int large_wal_state_advance(LargeWalState *st, uint64_t new_flush_lsn)
{
    if (!st || st->fd < 0) return MYDB_ERR;

    pthread_mutex_lock(&st->lock);

    /* The monotonicity check has to be inside the lock, not before it:
     * two concurrent advances that both read the old flush_lsn would
     * otherwise each pass the check and then race to write, letting the
     * lower one land last. */
    if (new_flush_lsn < st->flush_lsn) {
        pthread_mutex_unlock(&st->lock);
        return MYDB_ERR;
    }

    uint64_t prev = st->flush_lsn;
    st->flush_lsn = new_flush_lsn;

    int rc = save(st);
    if (rc != MYDB_OK) st->flush_lsn = prev;

    pthread_mutex_unlock(&st->lock);
    return rc;
}
