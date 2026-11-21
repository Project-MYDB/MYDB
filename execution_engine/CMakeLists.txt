# execution_engine — AST walker that sits between the parser and
# partition_manager.
#
# Public API: one C-callable function
#   int exec_engine_execute(ExecContext *, ParserAST *, char *, size_t)
# declared in include/exec_engine_api.h.
#
# Link topology note
# ──────────────────
# engine → execution_engine (engine is the sole caller of exec_engine_execute).
# execution_engine → partition_manager (pm_* DDL/DML/DQL/TCL wrappers).
# execution_engine → planner           (cost-based access path selection).
# execution_engine → query_parser      (AST types, parser_ast_node).
#
# execution_engine does NOT link against `engine` to avoid a circular
# CMake target dependency.  It only needs engine/include for the
# EngineState / ExecContext / Connection types and engine_* signatures;
# the actual engine_* symbols are resolved at final-binary link time
# because every executable that uses the executor also links `engine`.
#
# engine/include, system_schema/include and partition_manager/include are
# added so implementation files can #include "engine.h" / "exec_context.h" /
# "pm_api.h" etc.  partition_manager is linked (it owns the pm_* symbols).

add_library(execution_engine STATIC
    src/exec_engine_api.cpp
    src/dispatch.cpp
    src/tcl.cpp
    src/ddl.cpp
    src/dml.cpp
    src/dql.cpp
    src/value_cast.cpp
    src/expr_eval.cpp
    src/result_fmt.cpp
)

target_include_directories(execution_engine PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# engine.h and system_schema.h are needed by implementation files but
# must not be exposed as part of this library's public interface.
# engine.h includes crypto.h (SALT_LEN / SHA256_DIGEST_LEN /
# MYDB_NONCE_LEN for the network-login signatures). crypto.h now lives in
# common_utils, whose include path arrives through partition_manager ->
# storage_engine's PUBLIC link, so it needs no path of its own here.
target_include_directories(execution_engine PRIVATE
    ${CMAKE_SOURCE_DIR}/engine/include
    ${CMAKE_SOURCE_DIR}/system_schema/include
    ${CMAKE_SOURCE_DIR}/partition_manager/include
)

target_link_libraries(execution_engine
    PUBLIC query_parser
    PUBLIC partition_manager
    PUBLIC planner
)

target_compile_features(execution_engine PUBLIC cxx_std_17)
target_compile_options(execution_engine PRIVATE -Wall -Wextra -Wpedantic)
