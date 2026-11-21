add_library(wal STATIC
    src/wal_mailbox.c
    src/wal_worker.c
    src/wal_page.c
    src/wal_types.c
    src/wal_segment.c
    src/wal_segment_pool.c
    src/wal_ring_buffer.c
    src/wal_flusher.c
    src/wal_manager.c
)

# large_wal (Phases 1-5: page/segment format, the 4-slot rotation pool,
# the archive section, the staging buffer, the writer thread + state file)
# and the old top-level wal_manager (owner of the worker + both subsystems'
# managers) were moved out of the tree to ../large_wal_stash once records
# were allowed to span pages and segments: one log, no overflow log.
# What stays: wal_mailbox (the shared background-thread + request/response
# queue) + wal_worker (the fsync-offload thread built on it) +
# wal_manager, which was normal_wal_manager and now also owns the worker
# (MYDB_WAL_IMPLEMENTATION.md §10).
target_include_directories(wal PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# common_utils only — not storage_engine. WAL manages its own files and
# doesn't need BufferPool/BTree/DiskManager/RelationDef; pulling in all of
# storage_engine just for common.h/file_header.h/checksum.h would be an
# artificial coupling the common_utils extraction exists to avoid.
#
# Threads::Threads — wal_ring_buffer.c's lock/flush_cond (and, from this
# point on, the Flusher thread itself) are this project's first real
# pthread usage beyond PartitionBuffer's data-protection latches.
target_link_libraries(wal PUBLIC
    common_utils
    Threads::Threads
)

target_compile_options(wal PRIVATE -Wall -Wextra -Wpedantic)
