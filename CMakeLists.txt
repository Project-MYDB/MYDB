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

# common_utils — system-wide headers and helpers every module uses:
# common.h (error codes, Value, Row, FILETYPE_*), file_header, checksum,
# and crypto (SHA-256 + salt helpers, shared by the engine and the network
# client; lifted out of engine/ when the client became a second consumer,
# later folded into common_utils).
# Built first; storage_engine links it PUBLIC.
add_subdirectory(common_utils)
# Storage engine — stateless I/O layer (v3): BufferPool, BTree, Page,
# DiskManager, RelationDef only.  No session state.
add_subdirectory(storage_engine)
# System schema (users + privileges) — engine-level metadata, lives outside
# any partition. Depends on common_utils (via storage_engine) for common.h /
# file_header / checksum.
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
# Built after server (it reuses server/protocol.c); crypto comes from
# common_utils.
add_subdirectory(client)
# `mydb-server` + `mydb-client` executables (thin mains over the libs above).
add_subdirectory(bin)

# Tests
enable_testing()
add_subdirectory(tests)
