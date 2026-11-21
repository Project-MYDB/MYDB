# mydb_client — client-side library (socket connection + auth handshake +
# terminal REPL).  Deliberately ENGINE-FREE: it links only the shared wire
# protocol (mydb_proto) and the shared crypto in common_utils (challenge
# response).  This is what keeps the distributed client binary small and free of engine internals.
add_library(mydb_client STATIC
    src/client_conn.c
    src/client_repl.c
)

target_include_directories(mydb_client PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(mydb_client PUBLIC mydb_proto common_utils)

target_compile_options(mydb_client PRIVATE -Wall -Wextra -Wpedantic)
