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

# WAL: record, page and segment header formats and checksums.
add_executable(test_wal_types wal/test_wal_types.c)
target_link_libraries(test_wal_types PRIVATE wal)
add_test(NAME test_wal_types COMMAND test_wal_types)

# wal_segment_pool: the segment files, claiming, I/O, finding and cutting
# the end of the log. Real files under /tmp.
add_executable(test_wal_segment_pool wal/test_wal_segment_pool.c)
target_link_libraries(test_wal_segment_pool PRIVATE wal)
add_test(NAME test_wal_segment_pool COMMAND test_wal_segment_pool)

# wal_worker: the background fdatasync of finished segments and its
# publishing of flushed_lsn.
add_executable(test_wal_worker wal/test_wal_worker.c)
target_link_libraries(test_wal_worker PRIVATE wal)
add_test(NAME test_wal_worker COMMAND test_wal_worker)

# wal_insert: block counters, planning and copying pieces, wal_insert,
# the space check where it passes, and the completeness scan.
add_executable(test_wal_insert wal/test_wal_insert.c)
target_link_libraries(test_wal_insert PRIVATE wal)
add_test(NAME test_wal_insert COMMAND test_wal_insert)

# wal_flush: the write step on real files, the hole wait, the space
# waiter, log_flush, and concurrent sessions. TIMEOUT: a lost
# wake-up or a stuck hole would hang.
add_executable(test_wal_flush wal/test_wal_flush.c)
target_link_libraries(test_wal_flush PRIVATE wal)
add_test(NAME test_wal_flush COMMAND test_wal_flush)
set_tests_properties(test_wal_flush PROPERTIES TIMEOUT 180)

# wal_group_commit_lock (after MariaDB's group_commit_lock). TIMEOUT: a
# lost wake-up would hang.
add_executable(test_wal_group_commit_lock wal/test_wal_group_commit_lock.c)
target_link_libraries(test_wal_group_commit_lock PRIVATE wal)
add_test(NAME test_wal_group_commit_lock COMMAND test_wal_group_commit_lock)
set_tests_properties(test_wal_group_commit_lock PROPERTIES TIMEOUT 60)

# wal_manager: init, shutdown, and resuming at the end of the log.
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
