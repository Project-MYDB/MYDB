add_library(engine STATIC
    src/database_file.c
    src/engine.c
    src/stats_buffer.c
)

target_include_directories(engine PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    # engine.h includes partition_ctx.h (from partition_manager) for the
    # PartitionCtx* active_partition field in EngineState.
    ${CMAKE_SOURCE_DIR}/partition_manager/include
)

# Engine orchestrates partition_manager (session state: Catalog, SchemaFile,
# TxnManager, PartitionCtx) and system_schema (users + privileges). PUBLIC
# so test binaries that link `engine` inherit those include paths transitively.
#
# query_parser + execution_engine are needed by engine_execute_sql,
# which routes raw SQL through parse → execute. PUBLIC so bin (and any
# test binary linking engine) gets the parser_api.h / exec_engine_api.h
# include paths transitively.
target_link_libraries(engine PUBLIC
        storage_engine system_schema partition_manager
        query_parser execution_engine common_utils
)

target_compile_options(engine PRIVATE -Wall -Wextra -Wpedantic)
