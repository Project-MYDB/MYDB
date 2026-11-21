# common_utils modules (common.h, file_header, checksum) — extracted out
# of storage_engine in WAL Phase 1 since they're system-wide, not
# storage-specific.
set(COMMON_UTILS_TEST_MODULES file_header checksum)

foreach(mod ${COMMON_UTILS_TEST_MODULES})
    add_executable(test_${mod} common_utils/test_${mod}.c)
    target_link_libraries(test_${mod} PRIVATE common_utils)
    add_test(NAME test_${mod} COMMAND test_${mod})
endforeach()

# Pure storage_engine modules (stateless I/O layer).
set(STORAGE_ONLY_TEST_MODULES
    disk_manager page buffer_pool btree relation_def)

foreach(mod ${STORAGE_ONLY_TEST_MODULES})
    add_executable(test_${mod} storage_engine/test_${mod}.c)
    target_link_libraries(test_${mod} PRIVATE storage_engine)
    add_test(NAME test_${mod} COMMAND test_${mod})
endforeach()

# WAL — the on-disk struct layer (record/page/segment header
# format + checksum). Skeleton only this phase: no file I/O, no ring
# buffer, no threads (see MYDB_WAL_IMPLEMENTATION.md §8).
add_executable(test_wal_types wal/test_wal_types.c)
target_link_libraries(test_wal_types PRIVATE wal)
add_test(NAME test_wal_types COMMAND test_wal_types)

# The large_wal tests (Phases 1-5, the registry, the manager and the
# concurrency test) and the old test_wal_manager moved out with their code to
# ../large_wal_stash (tests_CMakeLists_fragment.txt holds their blocks).

# WAL Phase 2 — the pool of 10 physical wal_<N>.mydb segment files:
# pre-allocation, round-robin claiming, raw page I/O, crash-reload
# tail-scan (MYDB_WAL_IMPLEMENTATION.md §8.4). Real file I/O under /tmp.
add_executable(test_wal_segment_pool wal/test_wal_segment_pool.c)
target_link_libraries(test_wal_segment_pool PRIVATE wal)
add_test(NAME test_wal_segment_pool COMMAND test_wal_segment_pool)

# WAL Phase 3 — the 16MB ring buffer + txn_append_record LSN assignment
# (MYDB_WAL_IMPLEMENTATION.md §8.6). No ATT/DPT, no Flusher thread yet.
add_executable(test_wal_ring_buffer wal/test_wal_ring_buffer.c)
target_link_libraries(test_wal_ring_buffer PRIVATE wal)
add_test(NAME test_wal_ring_buffer COMMAND test_wal_ring_buffer)

# WAL Phase 4 — the real Flusher thread (MYDB_WAL_DESIGN.md §7). The
# first genuinely concurrent test in this codebase.
add_executable(test_wal_flusher wal/test_wal_flusher.c)
target_link_libraries(test_wal_flusher PRIVATE wal)
add_test(NAME test_wal_flusher COMMAND test_wal_flusher)

# wal_mailbox — the shared background-thread + request/response queue
# that wal_worker and LargeWalWriter's submit() (large_wal, now moved out)
# were both hand-written copies of. Tests the four rules its header now owns: the depth slot
# comes back on collect (not on handler completion), a queued request is
# still served during shutdown, waiting on an unposted node is a no-op,
# and two callers in flight each collect their own result.
add_executable(test_wal_mailbox wal/test_wal_mailbox.c)
target_link_libraries(test_wal_mailbox PRIVATE wal)
add_test(NAME test_wal_mailbox COMMAND test_wal_mailbox)

# wal_worker — the shared persistent background thread that overlaps a
# segment rollover's two distinct-file fsyncs (old segment's mark_done,
# new segment's trailing write) instead of paying for them sequentially.
add_executable(test_wal_worker wal/test_wal_worker.c)
target_link_libraries(test_wal_worker PRIVATE wal)
add_test(NAME test_wal_worker COMMAND test_wal_worker)

# wal_manager (lifecycle/ownership; was normal_wal_manager, which mirrored
# LargeWalManager, merged with the old top-level wal_manager): owns
# WalWorker + WalRingBuffer + WalSegmentPool + WalFlusher, one init() call
# brings all four up together.
add_executable(test_wal_manager wal/test_wal_manager.c)
target_link_libraries(test_wal_manager PRIVATE wal)
add_test(NAME test_wal_manager COMMAND test_wal_manager)

# Session-state modules that moved to partition_manager in v3.
# Their test files still live under tests/storage_engine/ (moved with git mv
# would need an update here — for now keep the path, link partition_manager).
set(PARTITION_MGR_TEST_MODULES
    transaction partition schema_file pm_api partition_buffer)

foreach(mod ${PARTITION_MGR_TEST_MODULES})
    add_executable(test_${mod} storage_engine/test_${mod}.c)
    target_link_libraries(test_${mod} PRIVATE partition_manager)
    add_test(NAME test_${mod} COMMAND test_${mod})
endforeach()

# system_schema tests link the system_schema library (transitively
# pulls storage_engine).
add_executable(test_system_schema system_schema/test_system_schema.c)
target_link_libraries(test_system_schema PRIVATE system_schema)
add_test(NAME test_system_schema COMMAND test_system_schema)

# engine tests link `engine` (transitively pulls storage_engine +
# system_schema).
#
# v3 NOTE: test_engine, test_storage, test_integration and test_exec_engine
# were written against the pre-v3 stateful storage / EngineState layout and
# were removed when storage became stateless and session state moved into
# Connection / PartitionCtx (2026-06-01).  They are pending rewrite against
# the new pm_* / ExecContext / pctx_* API (see project-progress memory).
# test_storage in particular must be re-conceived — the old stateful storage
# API it exercised no longer exists.
set(ENGINE_TEST_MODULES database_file crypto)
foreach(mod ${ENGINE_TEST_MODULES})
    add_executable(test_${mod} engine/test_${mod}.c)
    target_link_libraries(test_${mod} PRIVATE engine)
    add_test(NAME test_${mod} COMMAND test_${mod})
endforeach()

# query_parser tests — links query_parser (headers exposed via its PUBLIC
# include dirs, so #include "Parser.hpp" / "Lexer.hpp" resolve correctly).
add_executable(test_parser query_parser/test_parser.cpp)
target_link_libraries(test_parser PRIVATE query_parser)
add_test(NAME test_parser COMMAND test_parser)

# planner unit tests — exercise planner_plan_join_order and
# planner_choose_join_algo with in-memory fixtures (no engine bootstrap).
add_executable(test_join_order planner/test_join_order.c)
target_link_libraries(test_join_order PRIVATE planner)
add_test(NAME test_join_order COMMAND test_join_order)

# execution_engine functional tests — drive the full engine via
# engine_execute_sql().  Link `engine`, which transitively pulls the
# execution_engine, planner, partition_manager, query_parser and storage
# libraries needed to run SQL end-to-end.
add_executable(test_joins execution_engine/test_joins.cpp)
target_link_libraries(test_joins PRIVATE engine)
add_test(NAME test_joins COMMAND test_joins)
