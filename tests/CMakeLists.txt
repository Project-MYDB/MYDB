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

# WAL — normal_wal's on-disk struct layer (record/page/segment header
# format + checksum). Skeleton only this phase: no file I/O, no ring
# buffer, no threads (see MYDB_WAL_IMPLEMENTATION.md §8).
add_executable(test_wal_types wal/test_wal_types.c)
target_link_libraries(test_wal_types PRIVATE wal)
add_test(NAME test_wal_types COMMAND test_wal_types)

# large_wal Phase 1 — on-disk struct layer only (page/segment header
# format + checksum), mirroring normal_wal's own Phase 1 exactly
# (MYDB_WAL_IMPLEMENTATION.md §10).
add_executable(test_large_wal_types wal/test_large_wal_types.c)
target_link_libraries(test_large_wal_types PRIVATE wal)
add_test(NAME test_large_wal_types COMMAND test_large_wal_types)

# large_wal Phase 2 — the 4-slot rotation pool of large_wal_<N>.mydb
# segment files (MYDB_WAL_IMPLEMENTATION.md §10.1). Real file I/O under
# /tmp; no copy-out/holding-area yet — "archive section" phase next.
add_executable(test_large_wal_segment_pool wal/test_large_wal_segment_pool.c)
target_link_libraries(test_large_wal_segment_pool PRIVATE wal)
add_test(NAME test_large_wal_segment_pool COMMAND test_large_wal_segment_pool)

# large_wal Phase 3 — the archive section: large_wal_index.mydb (content_lsn
# -> segment location) and the archiver (rotation-pool copy-out to the
# holding area, large_wal_get resolution, Gate A/B freeing). Real file I/O
# under /tmp. (MYDB_WAL_IMPLEMENTATION.md §10.1/§10.6).
add_executable(test_large_wal_index wal/test_large_wal_index.c)
target_link_libraries(test_large_wal_index PRIVATE wal)
add_test(NAME test_large_wal_index COMMAND test_large_wal_index)

add_executable(test_large_wal_archiver wal/test_large_wal_archiver.c)
target_link_libraries(test_large_wal_archiver PRIVATE wal)
add_test(NAME test_large_wal_archiver COMMAND test_large_wal_archiver)

# large_wal Phase 4 — the static staging buffer (MYDB_WAL_IMPLEMENTATION.md
# §10.10): static-vs-heap sizing only, no page framing (deferred to the
# writer-thread phase — see the plan).
add_executable(test_large_wal_buffer wal/test_large_wal_buffer.c)
target_link_libraries(test_large_wal_buffer PRIVATE wal)
add_test(NAME test_large_wal_buffer COMMAND test_large_wal_buffer)

# large_wal Phase 5 — large_wal_state.mydb (flush_lsn watermark, §10.9)
# and the large_wal_writer thread (Appendix A): segment-fit check,
# header stamping, one byte-for-byte segment write, index insert, and
# widening the archiver's registry to cover claim_next() onward.
add_executable(test_large_wal_state wal/test_large_wal_state.c)
target_link_libraries(test_large_wal_state PRIVATE wal)
add_test(NAME test_large_wal_state COMMAND test_large_wal_state)

add_executable(test_large_wal_writer wal/test_large_wal_writer.c)
target_link_libraries(test_large_wal_writer PRIVATE wal)
add_test(NAME test_large_wal_writer COMMAND test_large_wal_writer)

# (segment_no -> fd) table extracted out of large_wal_archiver once
# large_wal_writer started registering into it too -- shared
# infrastructure, not archiver-owned.
add_executable(test_large_wal_registry wal/test_large_wal_registry.c)
target_link_libraries(test_large_wal_registry PRIVATE wal)
add_test(NAME test_large_wal_registry COMMAND test_large_wal_registry)

# large_wal_manager (lifecycle/ownership, mirrors PartitionCtx) +
# large_wal_api (external contract + large_wal_get's real logic, mirrors
# pm_api.h) -- exercised together since large_wal_get/write only make
# sense against a fully brought-up LargeWalManager.
add_executable(test_large_wal_manager wal/test_large_wal_manager.c)
target_link_libraries(test_large_wal_manager PRIVATE wal)
add_test(NAME test_large_wal_manager COMMAND test_large_wal_manager)

# large_wal segment locking -- the only test that runs more than one
# thread against large_wal. Readers race the writer, readers race the
# archiver relocating and deleting segments underneath them, and the
# registry/index are hammered directly. Passing here is necessary but not
# sufficient: the real verification is a clean run under
# -DMYDB_SANITIZE_THREAD=ON (see the file's own header comment).
#
# Extended when the archiver thread landed: the hand-rolled driver this
# file used to run was replaced by the real large_wal_archiver thread,
# and it now also covers claim_next_wait's two outcomes (wakes on a
# free / times out), the Gate A+B provider actually driving try_free,
# and a restart re-registering holding-area files (including deleting a
# truncated one so copy_out's O_EXCL can create it again).
add_executable(test_large_wal_concurrency wal/test_large_wal_concurrency.c)
target_link_libraries(test_large_wal_concurrency PRIVATE wal Threads::Threads)
add_test(NAME test_large_wal_concurrency COMMAND test_large_wal_concurrency)

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
# that wal_worker and LargeWalWriter's submit() were both hand-written
# copies of. Tests the four rules its header now owns: the depth slot
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

# normal_wal_manager (lifecycle/ownership, mirrors LargeWalManager): owns
# WalRingBuffer + WalSegmentPool + WalFlusher, one init() call brings all
# three up together.
add_executable(test_normal_wal_manager wal/test_normal_wal_manager.c)
target_link_libraries(test_normal_wal_manager PRIVATE wal)
add_test(NAME test_normal_wal_manager COMMAND test_normal_wal_manager)

# wal_manager — top-level, sibling to normal_wal/ and large_wal/: owns
# the one shared WalWorker + both subsystems' own managers. NOT the
# TxnManager->wal_manager routing/dispatch layer (still deferred) —
# purely "owns the shared worker + both sub-managers."
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
