cmake_minimum_required(VERSION 3.20)
project(MyDB LANGUAGES C CXX)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
endif()

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Export compile commands so clangd/IDE can find include paths
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# pthread — first real threading primitive in the project, needed for
# PartitionBuffer's per-outer-slot latch (PARTITION_BUFFER_DESIGN.md §6/§7).
find_package(Threads REQUIRED)

# Opt-in ThreadSanitizer build: -DMYDB_SANITIZE_THREAD=ON.
#
# This was the real verification for large_wal's segment locking (now
# moved out to ../large_wal_stash) and still is for the WAL's own
# threads: the Flusher, the worker and the ring buffer's locks. A green
# run of a threaded test proves very little on its own — a data race
# usually produces correct output until the day it doesn't — so the
# locking phase is only actually verified by running the WAL tests under
# TSan. Off by default because it slows execution ~10x and cannot be
# combined with ASan/valgrind.
option(MYDB_SANITIZE_THREAD "Build with ThreadSanitizer (-fsanitize=thread)" OFF)
if(MYDB_SANITIZE_THREAD)
    add_compile_options(-fsanitize=thread -g -fno-omit-frame-pointer)
    add_link_options(-fsanitize=thread)
endif()

# Common utilities — common.h, file_header.h/.c, checksum.h/.c. Genuinely
# system-wide (every module depends on them), so this is the first
# subdirectory built, ahead of storage_engine which used to own them.
add_subdirectory(common_utils)
# Storage engine — stateless I/O layer (v3): BufferPool, BTree, Page,
# DiskManager, RelationDef only.  No session state.
add_subdirectory(storage_engine)
# WAL — wal record/page/segment format, ring buffer, segment pool and
# Flusher: one log. (A second, LARGE_WAL overflow log for records above
# 4 KB used to sit beside it; it was moved out to ../large_wal_stash once
# records were allowed to span pages and segments.) Depends only on
# common_utils (not storage_engine) — WAL manages its own files and
# doesn't need BufferPool/BTree/DiskManager/RelationDef. Not yet wired
# into partition_manager's TransactionManager (see
# MYDB_WAL_IMPLEMENTATION.md §11, deferred).
add_subdirectory(wal)
# crypto — SHA-256 + salt helpers, shared by engine (login/bootstrap) and the
# network client (challenge-response). Lifted out of engine/ when the client
# became a second consumer. Built before engine + client.
add_subdirectory(crypto)
# System schema (users + privileges) — engine-level metadata, lives outside
# any partition. Depends on storage_engine for common.h / file_header / checksum.
add_subdirectory(system_schema)
# Partition manager — owns session state (Catalog, SchemaFile, TxnManager,
# StatsFile, PartitionBuffer, PartitionCtx).  Wraps storage_engine and
# provides the pm_* API consumed by the execution engine.
# Depends on storage_engine + system_schema.
add_subdirectory(partition_manager)
# Query parser — Lexer/Parser plus parser_api C bridge. Built before
# engine so engine can link it.
add_subdirectory(query_parser)
# Planner — cost-based access path selection.  Depends on partition_manager
# (for stats.h which moved there in v3).  Built before execution_engine.
add_subdirectory(planner)
# Execution engine — AST walker. Built after query_parser, before engine.
add_subdirectory(execution_engine)
# Engine orchestrator — owns __database.mydb, EngineState, login/use_schema
# flow, and the bootstrap path. Pulls in crypto helpers (SHA-256 + salt).
# Engine links query_parser + execution_engine to drive engine_execute_sql.
add_subdirectory(engine)
# Server module — listener/protocol/session/auth/dispatch/event-loop library
# (mydb_server), linked into the mydb-server binary. Built after engine.
add_subdirectory(server)
# Client module — socket + REPL library (mydb_client), no engine inside.
# Built after server (it reuses server/protocol.c) and crypto.
add_subdirectory(client)
# `mydb-server` + `mydb-client` executables (thin mains over the libs above).
add_subdirectory(bin)

# Tests
enable_testing()
add_subdirectory(tests)
