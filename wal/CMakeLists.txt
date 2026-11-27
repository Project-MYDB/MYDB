add_library(wal STATIC
    src/wal_worker.c
    src/wal_page.c
    src/wal_types.c
    src/wal_segment.c
    src/wal_segment_pool.c
    src/wal_manager.c
    src/wal_insert.c
    src/wal_flush.c
    src/wal_group_commit_lock.c
)

target_include_directories(wal PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# common_utils for common.h, file_header and CRC-32C; the WAL needs
# nothing else from the storage engine. Threads for its locks and threads.
target_link_libraries(wal PUBLIC
    common_utils
    Threads::Threads
)

target_compile_options(wal PRIVATE -Wall -Wextra -Wpedantic)
