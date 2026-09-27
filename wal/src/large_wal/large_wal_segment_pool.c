#include "large_wal/large_wal_segment_pool.h"
#include "large_wal/large_wal_page.h"
#include "common.h"

#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>

/* ------------------------------------------------------------------
 * Internal helpers — same shape as normal_wal/wal_segment_pool.c's own
 * copies (each on-disk-I/O file in this codebase keeps its own static
 * copy rather than sharing one).
 * ------------------------------------------------------------------ */

static int pwrite_all(int fd, const void *buf, size_t n, off_t offset)
{
    ssize_t written = pwrite(fd, buf, n, offset);
    return (written == (ssize_t)n) ? MYDB_OK : MYDB_ERR;
}

static int pread_all(int fd, void *buf, size_t n, off_t offset)
{
    ssize_t got = pread(fd, buf, n, offset);
    return (got == (ssize_t)n) ? MYDB_OK : MYDB_ERR;
}

/* mkdir, ignoring EEXIST. Returns 0 if the directory now exists. */
static int ensure_dir(const char *path)
{
    if (mkdir(path, 0755) == 0) return 0;
    if (errno == EEXIST) {
        struct stat st;
        if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) return 0;
    }
    return -1;
}

static int page_no_valid(uint32_t page_no)
{
    return page_no >= 1 && page_no < LARGE_WAL_SEGMENT_PAGES_PER_FILE;
}

/* ------------------------------------------------------------------
 * Locked content writes.
 *
 * Every pwrite into a slot's *content* pages goes through here, so it
 * happens under that segment's registry node lock. Without it a
 * large_wal_get preading the same page mid-pwrite could deserialize a
 * half-updated 16KB page and report a spurious checksum failure — the
 * case do_write's read-back-and-rewrite of a partially filled tail page
 * makes real rather than theoretical.
 *
 * Two fallbacks write unlocked, and both are safe for the same reason:
 * a reader can only ever reach a segment *through* the registry, so a
 * pool with no registry, or a segment not registered in one, has no
 * possible concurrent reader to tear a page for.
 * ------------------------------------------------------------------ */
static int locked_pwrite(LargeWalSegmentPool *pool, uint32_t slot_index,
                          const void *buf, size_t n, off_t offset)
{
    int fd = pool->slots[slot_index].fd;

    if (!pool->registry) return pwrite_all(fd, buf, n, offset);

    /* pool->lock and the registry lock are taken in sequence, never
     * nested — see claim_next's note on why nesting them the other way
     * would close a cycle against copy_out. */
    pthread_mutex_lock(&pool->lock);
    uint64_t segment_no = pool->slots[slot_index].header.segment_no;
    pthread_mutex_unlock(&pool->lock);

    LargeWalRegistryNode *node = NULL;
    int                   reg_fd = -1;
    if (large_wal_registry_acquire(pool->registry, segment_no, &node, &reg_fd) != MYDB_OK)
        return pwrite_all(fd, buf, n, offset);

    int rc = pwrite_all(fd, buf, n, offset);
    large_wal_registry_release(pool->registry, node);
    return rc;
}

/* ------------------------------------------------------------------
 * Per-slot open-or-create.
 *
 * A segment file's page-0 slot is a full PAGE_SIZE (16KB) page (not just
 * LARGE_WAL_SEGMENT_HEADER_ON_DISK_SIZE bytes) — 64 meaningful bytes
 * followed by zero padding to fill the page, same "page N starts at
 * N * PAGE_SIZE" convention normal_wal already follows (proportionally
 * more wasteful here than normal_wal's 4KB pages, kept consistent
 * anyway). Real content pages therefore start at page_no = 1.
 * ------------------------------------------------------------------ */
static int open_existing_or_create_slot(LargeWalSegmentPool *pool, uint32_t slot_no)
{
    char path[300];
    snprintf(path, sizeof(path), "%s/large_wal_%u.mydb", pool->wal_dir, slot_no);

    LargeWalSlot *slot = &pool->slots[slot_no];
    slot->slot_no = (uint8_t)slot_no;
    slot->fd = -1;

    int fd = open(path, O_RDWR);
    if (fd >= 0) {
        /* Existing segment file from a prior run — reload its header. */
        uint8_t page_buf[PAGE_SIZE];
        if (pread_all(fd, page_buf, PAGE_SIZE, 0) != MYDB_OK) {
            close(fd);
            return MYDB_ERR;
        }
        int rc = large_wal_segment_header_deserialize(page_buf, &slot->header);
        if (rc != MYDB_OK) {
            close(fd);
            return rc;
        }
        slot->fd = fd;
        return MYDB_OK;
    }
    if (errno != ENOENT) return MYDB_ERR;

    /* Doesn't exist yet — create, pre-allocate, write an initial
     * LSEG_FREE header. posix_fallocate() — same portable-POSIX
     * substitution for the design doc's "via fallocate()" normal_wal's
     * pool already uses. */
    fd = open(path, O_RDWR | O_CREAT | O_EXCL, 0644);
    if (fd < 0) return MYDB_ERR;

    if (posix_fallocate(fd, 0, LARGE_WAL_SEGMENT_FILE_SIZE) != 0) {
        close(fd);
        return MYDB_ERR;
    }

    LargeWalSegmentHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.id.file_type = FILETYPE_LARGE_WAL_SEGMENT;
    hdr.segment_no   = 0;   /* placeholder — the real value is stamped at claim time */
    hdr.partition_id = pool->partition_id;
    hdr.state        = LSEG_FREE;

    uint8_t page_buf[PAGE_SIZE];
    memset(page_buf, 0, PAGE_SIZE);
    large_wal_segment_header_serialize(&hdr, page_buf);

    if (pwrite_all(fd, page_buf, PAGE_SIZE, 0) != MYDB_OK) {
        close(fd);
        return MYDB_ERR;
    }
    if (fdatasync(fd) < 0) {
        close(fd);
        return MYDB_ERR;
    }

    /* Read back rather than trust the local hdr — picks up the real
     * checksum large_wal_segment_header_serialize stamped into page_buf. */
    large_wal_segment_header_deserialize(page_buf, &slot->header);
    slot->fd = fd;
    return MYDB_OK;
}

/* ------------------------------------------------------------------
 * Lifecycle
 * ------------------------------------------------------------------ */

int large_wal_segment_pool_init(LargeWalSegmentPool *pool, const char *wal_dir,
                                 uint32_t partition_id, LargeWalRegistry *registry)
{
    if (!pool || !wal_dir) return MYDB_ERR;
    if (ensure_dir(wal_dir) != 0) return MYDB_ERR;

    memset(pool, 0, sizeof(*pool));
    if (pthread_mutex_init(&pool->lock, NULL) != 0) return MYDB_ERR;
    if (pthread_cond_init(&pool->slot_done_cv, NULL) != 0) {
        pthread_mutex_destroy(&pool->lock);
        return MYDB_ERR;
    }
    if (pthread_cond_init(&pool->slot_free_cv, NULL) != 0) {
        pthread_cond_destroy(&pool->slot_done_cv);
        pthread_mutex_destroy(&pool->lock);
        return MYDB_ERR;
    }
    snprintf(pool->wal_dir, sizeof(pool->wal_dir), "%s", wal_dir);
    pool->partition_id = partition_id;
    pool->registry     = registry;

    for (uint32_t i = 0; i < LARGE_WAL_SEGMENT_POOL_SLOTS; i++) {
        int rc = open_existing_or_create_slot(pool, i);
        if (rc != MYDB_OK) {
            /* Close whatever we already opened before bailing out. */
            for (uint32_t j = 0; j < i; j++) {
                if (pool->slots[j].fd >= 0) close(pool->slots[j].fd);
            }
            return rc;
        }
    }

    /* Any slot reloaded LSEG_ACTIVE has a stale on-disk data_pages by
     * definition — recover the true count now. */
    for (uint32_t i = 0; i < LARGE_WAL_SEGMENT_POOL_SLOTS; i++) {
        if (pool->slots[i].header.state == LSEG_ACTIVE) {
            uint32_t data_pages = 0;
            if (large_wal_segment_pool_tail_scan(pool, i, &data_pages) == MYDB_OK)
                pool->slots[i].header.data_pages = data_pages;
        }
    }

    /* Reseed the shared claim counter from whatever's on disk — same
     * scheme as WalSegmentPool (one pool-level counter, assigned at
     * claim time). */
    uint64_t max_used = 0;
    int      any_used = 0;
    for (uint32_t i = 0; i < LARGE_WAL_SEGMENT_POOL_SLOTS; i++) {
        if (pool->slots[i].header.state != LSEG_FREE) {
            if (!any_used || pool->slots[i].header.segment_no > max_used)
                max_used = pool->slots[i].header.segment_no;
            any_used = 1;
        }
    }
    pool->next_segment_no = any_used ? (max_used + 1) : 0;

    return MYDB_OK;
}

int large_wal_segment_pool_shutdown(LargeWalSegmentPool *pool)
{
    if (!pool) return MYDB_ERR;

    /* Wake anything still blocked in claim_next_wait BEFORE closing the
     * fds out from under it. shutting_down is what turns that wake into
     * a clean MYDB_ERR rather than another trip round the retry loop. */
    pthread_mutex_lock(&pool->lock);
    pool->shutting_down = 1;
    pthread_cond_broadcast(&pool->slot_free_cv);
    pthread_cond_broadcast(&pool->slot_done_cv);
    pthread_mutex_unlock(&pool->lock);

    for (uint32_t i = 0; i < LARGE_WAL_SEGMENT_POOL_SLOTS; i++) {
        if (pool->slots[i].fd >= 0) {
            close(pool->slots[i].fd);
            pool->slots[i].fd = -1;
        }
    }
    pthread_cond_destroy(&pool->slot_free_cv);
    pthread_cond_destroy(&pool->slot_done_cv);
    pthread_mutex_destroy(&pool->lock);
    return MYDB_OK;
}

/* ------------------------------------------------------------------
 * Claiming
 * ------------------------------------------------------------------ */

/* The claim itself, with pool->lock ALREADY HELD by the caller.
 *
 * Split out of claim_next so claim_next_wait can retry it directly
 * inside the same locked region it waits in. Doing the retry from
 * outside the lock instead would open a gap between "the claim failed"
 * and "we started waiting", and a free_slot landing in that gap would
 * broadcast to nobody -- the writer would then sit out its whole
 * timeout despite a slot being free the entire time. Not a deadlock,
 * but a multi-millisecond stall for no reason.
 *
 * Returns MYDB_ERR_FULL when the target slot simply isn't free yet
 * (retryable -- this is what claim_next_wait waits on) and MYDB_ERR for
 * a real I/O failure (not retryable). claim_next collapses both back to
 * MYDB_ERR to keep its long-standing public contract unchanged.
 *
 * On success the two out-params carry what the caller needs to register
 * AFTER releasing pool->lock -- registering here would take the
 * registry write lock while holding pool->lock, the one nesting the
 * global lock order forbids. */
static int claim_next_locked(LargeWalSegmentPool *pool, uint32_t *out_slot_index,
                              uint64_t *out_segment_no, int *out_fd)
{
    uint32_t target = (uint32_t)(pool->next_segment_no % LARGE_WAL_SEGMENT_POOL_SLOTS);
    LargeWalSlot *slot = &pool->slots[target];

    if (slot->header.state != LSEG_FREE) return MYDB_ERR_FULL;

    /* Mutate a local copy, not slot->header: if the write below fails we
     * must leave the in-memory header exactly as it was, or memory and
     * disk disagree about this slot's state and every later decision
     * trusts the wrong one. slot->header is only committed once the
     * write has actually succeeded. */
    LargeWalSegmentHeader next = slot->header;
    next.segment_no   = pool->next_segment_no;
    next.start_lsn    = 0;
    next.end_lsn      = 0;
    next.data_pages   = 0;
    next.state        = LSEG_ACTIVE;
    next.partition_id = pool->partition_id;

    uint8_t page_buf[PAGE_SIZE];
    memset(page_buf, 0, PAGE_SIZE);
    large_wal_segment_header_serialize(&next, page_buf);

    if (pwrite_all(slot->fd, page_buf, PAGE_SIZE, 0) != MYDB_OK) return MYDB_ERR;
    /* No fdatasync here — see the header's doc comment for why claim_next
     * never needs one. */

    large_wal_segment_header_deserialize(page_buf, &slot->header);

    *out_segment_no = slot->header.segment_no;
    *out_fd         = slot->fd;

    pool->next_segment_no++;

    *out_slot_index = target;
    return MYDB_OK;
}

/* Registers a freshly claimed segment. Split out purely so claim_next
 * and claim_next_wait share it — both must call it with pool->lock
 * already RELEASED. */
static int register_claimed(LargeWalSegmentPool *pool, uint64_t claimed_segment_no,
                             int claimed_fd)
{
    /* The segment_no now exists, so make it resolvable immediately —
     * see the header's doc comment for why this lives here rather than
     * in whoever called claim_next. owns_fd = 0: this fd belongs to the
     * pool's own slot and is closed by pool shutdown, never by the
     * registry.
     *
     * Deliberately outside pool->lock: this takes the registry's WRITE
     * lock, and copy_out holds a registry node lock while calling
     * free_slot, which takes pool->lock — nesting these the other way
     * round would close that cycle. Safe to run unlocked because the
     * segment is ACTIVE but unregistered for exactly this window, and
     * nothing can be asking for it yet: no index entry names it until
     * the writer has actually written into it. */
    if (pool->registry &&
        large_wal_registry_register(pool->registry, claimed_segment_no,
                                     claimed_fd, /*owns_fd=*/0) != MYDB_OK)
        return MYDB_ERR;

    return MYDB_OK;
}

int large_wal_segment_pool_claim_next(LargeWalSegmentPool *pool, uint32_t *out_slot_index)
{
    if (!pool || !out_slot_index) return MYDB_ERR;

    /* pool->lock covers everything through the header commit, then is
     * released BEFORE registering — see the header's doc comment for
     * why those two must not nest. */
    uint64_t claimed_segment_no = 0;
    int      claimed_fd         = -1;

    pthread_mutex_lock(&pool->lock);
    int rc = claim_next_locked(pool, out_slot_index, &claimed_segment_no, &claimed_fd);
    pthread_mutex_unlock(&pool->lock);

    /* Collapsed back to plain MYDB_ERR: callers and tests have checked
     * for exactly that since Phase 2, and "no free slot" vs "write
     * failed" is a distinction only claim_next_wait needs. */
    if (rc != MYDB_OK) return MYDB_ERR;

    return register_claimed(pool, claimed_segment_no, claimed_fd);
}

/* now + ms, as an absolute CLOCK_REALTIME deadline —
 * pthread_cond_timedwait wants absolute, not a duration, precisely so a
 * spurious wakeup can't restart the clock. */
static void deadline_from_now(struct timespec *ts, uint32_t ms)
{
    clock_gettime(CLOCK_REALTIME, ts);
    ts->tv_sec  += (time_t)(ms / 1000u);
    ts->tv_nsec += (long)(ms % 1000u) * 1000000L;
    if (ts->tv_nsec >= 1000000000L) { ts->tv_sec++; ts->tv_nsec -= 1000000000L; }
}

int large_wal_segment_pool_claim_next_wait(LargeWalSegmentPool *pool,
                                            uint32_t timeout_ms,
                                            uint32_t *out_slot_index)
{
    if (!pool || !out_slot_index) return MYDB_ERR;

    struct timespec deadline;
    deadline_from_now(&deadline, timeout_ms);

    uint64_t claimed_segment_no = 0;
    int      claimed_fd         = -1;
    int      rc;

    pthread_mutex_lock(&pool->lock);
    for (;;) {
        if (pool->shutting_down) {
            pthread_mutex_unlock(&pool->lock);
            return MYDB_ERR;
        }

        rc = claim_next_locked(pool, out_slot_index, &claimed_segment_no, &claimed_fd);
        if (rc != MYDB_ERR_FULL) break;   /* claimed it, or failed for real */

        /* Every slot is still in use. Sleep until free_slot broadcasts,
         * then go round and try again — the broadcast only means "some
         * slot changed", not "the one slot this pool wants next", so a
         * retry is required rather than assuming success.
         *
         * cond_timedwait releases pool->lock while it sleeps, which is
         * what lets the archiver take it inside free_slot. And this
         * thread holds no registry lock at all, so copy_out's
         * reg -> node -> pool path stays clear. */
        if (pthread_cond_timedwait(&pool->slot_free_cv, &pool->lock, &deadline) == ETIMEDOUT) {
            pthread_mutex_unlock(&pool->lock);
            return MYDB_ERR;
        }
    }
    pthread_mutex_unlock(&pool->lock);

    if (rc != MYDB_OK) return MYDB_ERR;

    return register_claimed(pool, claimed_segment_no, claimed_fd);
}

int large_wal_segment_pool_mark_done(LargeWalSegmentPool *pool, WalWorker *worker,
                                     uint32_t slot_index, uint64_t end_lsn, uint32_t data_pages,
                                     uint64_t *out_ticket)
{
    if (out_ticket) *out_ticket = 0;
    if (!pool || slot_index >= LARGE_WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;

    LargeWalSlot *slot = &pool->slots[slot_index];

    /* pool->lock only — this rewrites the segment *header* (page 0),
     * which no reader ever touches: large_wal_get resolves an fd through
     * the registry and preads content pages by page_no. So there is
     * nothing here for a node lock to protect against. */
    pthread_mutex_lock(&pool->lock);

    if (slot->header.state != LSEG_ACTIVE) {
        pthread_mutex_unlock(&pool->lock);
        return MYDB_ERR;
    }

    /* Local copy, committed to slot->header only after the write and
     * sync both succeed — see claim_next's own note on why mutating
     * slot->header up front would leave memory and disk disagreeing on
     * any failure path. */
    LargeWalSegmentHeader next = slot->header;
    next.end_lsn    = end_lsn;
    next.data_pages = data_pages;
    next.state      = LSEG_DONE;

    uint8_t page_buf[PAGE_SIZE];
    memset(page_buf, 0, PAGE_SIZE);
    large_wal_segment_header_serialize(&next, page_buf);

    int rc = MYDB_OK;
    if (pwrite_all(slot->fd, page_buf, PAGE_SIZE, 0) != MYDB_OK) {
        rc = MYDB_ERR;
    } else if (worker) {
        /* out_ticket is written by async_fdatasync itself, in the same
         * call that posts -- so it is set before any later step here can
         * fail. That ordering is the point: a mark_done that posts and
         * then returns an error would otherwise leave its caller with no
         * ticket to drain with, and an undrained mailbox blocks the next
         * post forever, for the whole partition. */
        if (wal_worker_async_fdatasync(worker, slot->fd, out_ticket) != MYDB_OK) rc = MYDB_ERR;
    } else {
        if (fdatasync(slot->fd) < 0) rc = MYDB_ERR;
    }

    if (rc == MYDB_OK) {
        large_wal_segment_header_deserialize(page_buf, &slot->header);

        /* A slot just became LSEG_DONE, which is the only thing the
         * archiver ever waits for. Broadcast rather than signal: it
         * costs nothing with one waiter, and stays correct if a second
         * archiver-side consumer is ever added. Done while still
         * holding the lock so the state change and the wake-up can't be
         * observed out of order. */
        pthread_cond_broadcast(&pool->slot_done_cv);
    }

    pthread_mutex_unlock(&pool->lock);
    return rc;
}

/* Overwrites this slot's content pages (1..PAGES_PER_FILE-1) with
 * zeros. The file is posix_fallocate'd once at creation and never
 * truncated, so without this the previous segment's pages stay
 * physically present — and they carry valid magic/version/file_type/CRC,
 * having been legitimately written once. tail_scan, which walks pages
 * until one fails to deserialize, would sail straight past the next
 * generation's tail into them. Zeroing is what makes an unwritten page
 * actually *look* unwritten.
 *
 * Chunked rather than one 2MB buffer: this runs once per segment on the
 * archiver's path, so ~32 pwrites of 64KB costs nothing that matters. */
static int zero_content_pages(int fd)
{
    enum { CHUNK = 64 * 1024 };
    static const uint8_t zeros[CHUNK];   /* .bss — never written to */

    off_t  pos = PAGE_SIZE;              /* page 0 is the header; keep it */
    off_t  end = LARGE_WAL_SEGMENT_FILE_SIZE;

    while (pos < end) {
        size_t n = (size_t)((end - pos) < CHUNK ? (end - pos) : CHUNK);
        if (pwrite_all(fd, zeros, n, pos) != MYDB_OK) return MYDB_ERR;
        pos += (off_t)n;
    }
    return MYDB_OK;
}

int large_wal_segment_pool_free_slot(LargeWalSegmentPool *pool, uint32_t slot_index)
{
    if (!pool || slot_index >= LARGE_WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;

    LargeWalSlot *slot = &pool->slots[slot_index];

    /* pool->lock guards slot->header, as everywhere else here. The
     * segment *content* zeroed below is guarded by this segment's
     * registry node lock, which the CALLER holds — see the header's
     * LOCKING CONTRACT. copy_out is already inside that node lock so
     * that freeing this slot and repointing the registry at the
     * holding-area copy are one atomic step to a reader; re-taking it
     * here would self-deadlock. */
    pthread_mutex_lock(&pool->lock);

    if (slot->header.state != LSEG_DONE) {
        pthread_mutex_unlock(&pool->lock);
        return MYDB_ERR;
    }

    /* Zero the content, and make that durable, BEFORE the header says
     * LSEG_FREE. The two writes must not share one flush: they could
     * then reach disk in either order, and page 0 landing first would
     * publish the slot as reusable while the old segment's pages are
     * still there — reviving exactly the bug this zeroing exists to
     * kill. Two fsyncs is affordable here precisely because freeing is
     * the archiver's cold path, once per segment.
     *
     * A crash anywhere before step 2 completes leaves the header still
     * LSEG_DONE, so claim_next (which demands LSEG_FREE) can't hand the
     * slot out and nothing ever scans it — and the content is already
     * durable in the holding area, since copy_out fsyncs that before it
     * calls us. */
    if (zero_content_pages(slot->fd) != MYDB_OK || fdatasync(slot->fd) < 0) {
        pthread_mutex_unlock(&pool->lock);
        return MYDB_ERR;
    }

    /* Local copy, committed only on success — see claim_next's note. */
    LargeWalSegmentHeader next = slot->header;
    next.segment_no = 0;
    next.start_lsn  = 0;
    next.end_lsn    = 0;
    next.data_pages = 0;
    next.state      = LSEG_FREE;

    uint8_t page_buf[PAGE_SIZE];
    memset(page_buf, 0, PAGE_SIZE);
    large_wal_segment_header_serialize(&next, page_buf);

    int rc = MYDB_OK;
    if (pwrite_all(slot->fd, page_buf, PAGE_SIZE, 0) != MYDB_OK) rc = MYDB_ERR;
    else if (fdatasync(slot->fd) < 0)                            rc = MYDB_ERR;

    if (rc == MYDB_OK) {
        large_wal_segment_header_deserialize(page_buf, &slot->header);

        /* A slot just became LSEG_FREE — the other half of the handoff.
         * This is what wakes a writer parked in claim_next_wait. */
        pthread_cond_broadcast(&pool->slot_free_cv);
    }

    pthread_mutex_unlock(&pool->lock);
    return rc;
}

int large_wal_segment_pool_slot_info(LargeWalSegmentPool *pool, uint32_t slot_index,
                                      uint64_t *out_segment_no, uint8_t *out_state)
{
    if (!pool || slot_index >= LARGE_WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;

    pthread_mutex_lock(&pool->lock);
    if (out_segment_no) *out_segment_no = pool->slots[slot_index].header.segment_no;
    if (out_state)      *out_state      = pool->slots[slot_index].header.state;
    pthread_mutex_unlock(&pool->lock);

    return MYDB_OK;
}

/* ------------------------------------------------------------------
 * Raw page I/O
 * ------------------------------------------------------------------ */

int large_wal_segment_pool_write_page(LargeWalSegmentPool *pool, uint32_t slot_index,
                                       uint32_t page_no, const uint8_t *buf)
{
    if (!pool || !buf || slot_index >= LARGE_WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;
    if (!page_no_valid(page_no)) return MYDB_ERR;

    off_t offset = (off_t)page_no * PAGE_SIZE;
    return locked_pwrite(pool, slot_index, buf, PAGE_SIZE, offset);
}

int large_wal_segment_pool_read_page(LargeWalSegmentPool *pool, uint32_t slot_index,
                                      uint32_t page_no, uint8_t *out_buf)
{
    if (!pool || !out_buf || slot_index >= LARGE_WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;
    if (!page_no_valid(page_no)) return MYDB_ERR;

    off_t offset = (off_t)page_no * PAGE_SIZE;
    return pread_all(pool->slots[slot_index].fd, out_buf, PAGE_SIZE, offset);
}

int large_wal_segment_pool_read_segment(LargeWalSegmentPool *pool, uint32_t slot_index,
                                         uint8_t *out_buf)
{
    if (!pool || !out_buf || slot_index >= LARGE_WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;
    return pread_all(pool->slots[slot_index].fd, out_buf, LARGE_WAL_SEGMENT_FILE_SIZE, 0);
}

/* ------------------------------------------------------------------
 * large_wal_segment_pool_write — the Writer-thread-facing abstraction.
 * A blind byte mover: never parses or constructs page-header
 * content (the caller's buf already carries whatever headers/LSNs it
 * needs). Only knows page/segment geometry. slot_index/page_no/offset
 * are the caller's own cursor, purely positional — never derived from
 * anything inside buf.
 * ------------------------------------------------------------------ */

int large_wal_segment_pool_write(LargeWalSegmentPool *pool, WalWorker *worker,
                                  uint32_t *slot_index, uint32_t *page_no, uint32_t *offset,
                                  const uint8_t *buf, size_t buf_len)
{
    if (!pool || !slot_index || !page_no || !offset || !buf) return MYDB_ERR;
    if (*slot_index >= LARGE_WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;
    if (!page_no_valid(*page_no)) return MYDB_ERR;
    if (*offset >= PAGE_SIZE) return MYDB_ERR;

    /* Single exit through `out:` from here on. Once mark_done() below has
     * handed a flush to worker, EVERY return path has to drain it:
     * wal_worker_wait() is the only thing that clears the mailbox's done
     * flag, so returning early without it wedges the next
     * wal_worker_async_fdatasync() forever — for the whole partition,
     * not just this call. */
    int rc = MYDB_OK;

    /* Names the fsync mark_done() handed to worker, if this call rolled
     * over at all. Stays 0 otherwise, and wal_worker_wait() treats 0 as
     * "I posted nothing" -- which is what lets the drain at `out:` stay
     * unconditional without ever collecting the OTHER caller thread's
     * result. Both WAL paths share one WalWorker (wal_manager.h). */
    uint64_t flush_ticket = 0;

    size_t written = 0;
    while (written < buf_len) {
        uint32_t room_left = PAGE_SIZE - *offset;
        size_t   remaining = buf_len - written;
        uint32_t chunk = (remaining < (size_t)room_left) ? (uint32_t)remaining : room_left;

        /* One locked_pwrite per chunk, and a chunk never spans pages —
         * so each 16KB page reaches disk under this segment's node lock
         * in a single call, which is what makes it atomic against a
         * concurrent large_wal_get. Acquiring per chunk rather than once
         * for the whole call is deliberate: this loop rolls segments
         * internally via claim_next, which needs the registry's WRITE
         * lock, and holding the read lock across that would self-
         * deadlock on a non-recursive rwlock. */
        off_t abs_offset = (off_t)(*page_no) * PAGE_SIZE + *offset;
        if (locked_pwrite(pool, *slot_index, buf + written, chunk, abs_offset) != MYDB_OK) {
            rc = MYDB_ERR;
            goto out;
        }

        written += chunk;
        *offset += chunk;

        if (*offset < PAGE_SIZE) {
            /* This page-slot isn't full — every remaining byte must have
             * fit, so the loop is about to exit with the cursor left
             * mid-page for a future call to resume appending into. */
            break;
        }

        /* This page-slot just filled exactly. */
        *offset = 0;

        if (*page_no + 1 >= LARGE_WAL_SEGMENT_PAGES_PER_FILE) {
            /* The segment is full too. The one non-"blind" step: read
             * the page we just filled back and copy its own end_lsn
             * field — the only reliable source for the segment's true
             * highest LSN, since a page can hold several records and
             * start_lsn names only the first. A field copy, not content
             * interpretation — identical to normal_wal's own rollover
             * path (wal_segment_pool.c), now that both share
             * WalPageHeader. */
            uint8_t  last_page[PAGE_SIZE];
            uint64_t seg_end_lsn = 0;
            if (large_wal_segment_pool_read_page(pool, *slot_index, *page_no, last_page) == MYDB_OK) {
                WalPageHeader hdr;
                if (large_wal_page_header_deserialize(last_page, &hdr) == MYDB_OK)
                    seg_end_lsn = hdr.end_lsn;
            }

            uint32_t finished_slot       = *slot_index;
            uint32_t finished_data_pages = *page_no;
            if (large_wal_segment_pool_mark_done(pool, worker, finished_slot, seg_end_lsn,
                                                 finished_data_pages, &flush_ticket) != MYDB_OK) {
                rc = MYDB_ERR;
                goto out;
            }
            /* mark_done may have just handed a flush to worker — from
             * here on, bailing out without draining it would wedge the
             * mailbox, hence goto rather than return. claim_next failing
             * is the reachable case: the pool runs out of free slots
             * whenever the archiver is behind. */
            /* The waiting variant, not the plain one: the pool being
             * momentarily full is a condition that clears itself within
             * milliseconds once the archiver copies a DONE slot out.
             * Failing here instead would turn that into a failed user
             * write. On timeout it returns exactly what plain
             * claim_next returns, so this is never worse. */
            if (large_wal_segment_pool_claim_next_wait(pool, LARGE_WAL_CLAIM_WAIT_MS,
                                                        slot_index) != MYDB_OK) {
                rc = MYDB_ERR;
                goto out;
            }
            *page_no = 1;
        } else {
            (*page_no)++;
        }
    }

    /* TEMPORARY: fdatasync every call — large_wal_writer (large_wal_writer.h)
     * now exists, but each submit() is still a fully synchronous,
     * single-record handoff (design doc §11's group-commit batching isn't
     * built yet), so there's still no batching to fold this flush into.
     * fdatasync, not fsync: segment files are posix_fallocate'd to their
     * full fixed size once at creation and never resized again, so
     * skipping metadata (timestamps etc.) not needed for correct data
     * retrieval is strictly cheaper here with no durability difference —
     * avoids dragging the filesystem journal into every flush on
     * filesystems like ext4. Remove this one line once submit() batches
     * multiple pending records behind one flush instead. */
    if (fdatasync(pool->slots[*slot_index].fd) < 0) rc = MYDB_ERR;

out:
    /* If this call rolled over, mark_done() may have handed its fsync to
     * worker instead of waiting for it (see mark_done's own doc comment)
     * — confirm it's actually durable before telling our own caller this
     * call succeeded. Cheap no-op if it didn't. Runs unconditionally,
     * including on the error paths above: draining is what keeps the
     * shared mailbox usable, and the worker's own result still counts
     * toward what we report. */
    if (worker && wal_worker_wait(worker, flush_ticket) != MYDB_OK) rc = MYDB_ERR;

    return rc;
}

/* ------------------------------------------------------------------
 * Crash-reload tail-scan
 * ------------------------------------------------------------------ */

int large_wal_segment_pool_tail_scan(LargeWalSegmentPool *pool, uint32_t slot_index,
                                      uint32_t *out_data_pages)
{
    if (!pool || !out_data_pages || slot_index >= LARGE_WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;

    int      fd    = pool->slots[slot_index].fd;
    uint32_t count = 0;
    uint8_t  buf[PAGE_SIZE];

    for (uint32_t page_no = 1; page_no < LARGE_WAL_SEGMENT_PAGES_PER_FILE; page_no++) {
        off_t offset = (off_t)page_no * PAGE_SIZE;
        if (pread_all(fd, buf, PAGE_SIZE, offset) != MYDB_OK) break;

        WalPageHeader hdr;
        /* An unwritten (still-zero, sparse) page fails file_header_
         * check_id's magic check inside deserialize — that failure is
         * exactly the stop condition, not an error to report upward. */
        if (large_wal_page_header_deserialize(buf, &hdr) != MYDB_OK) break;

        count++;
    }

    *out_data_pages = count;
    return MYDB_OK;
}
