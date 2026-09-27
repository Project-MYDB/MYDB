add_library(wal STATIC
    src/wal_mailbox.c
    src/wal_worker.c
    src/wal_manager.c
    src/wal_page.c
    src/normal_wal/wal_types.c
    src/normal_wal/wal_segment.c
    src/normal_wal/wal_segment_pool.c
    src/normal_wal/wal_ring_buffer.c
    src/normal_wal/wal_flusher.c
    src/normal_wal/normal_wal_manager.c
    src/large_wal/large_wal_page.c
    src/large_wal/large_wal_segment.c
    src/large_wal/large_wal_segment_pool.c
    src/large_wal/large_wal_index.c
    src/large_wal/large_wal_archiver.c
    src/large_wal/large_wal_buffer.c
    src/large_wal/large_wal_state.c
    src/large_wal/large_wal_writer.c
    src/large_wal/large_wal_registry.c
    src/large_wal/large_wal_manager.c
    src/large_wal/large_wal_api.c
)

# large_wal Phase 1: on-disk struct layer (page/segment header format +
# checksum). Phase 2: the 4-slot rotation pool. Phase 3: the archive
# section (holding area + index + archiver). Phase 4: the static staging
# buffer. Phase 5: the large_wal_writer thread + large_wal_state.mydb.
# Then: wal_mailbox (the shared background-thread + request/response
# queue both mailboxes were hand-written copies of) + wal_worker (the
# fsync-offload thread built on it) +
# normal_wal_manager + wal_manager (top-level, owns the worker + both
# subsystems' managers) for overlapping a segment rollover's two fsyncs.
# All mirror normal_wal's own equivalent phases where one exists
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
