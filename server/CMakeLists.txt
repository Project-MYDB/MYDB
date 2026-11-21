# mydb_proto — the wire protocol, the ONE source shared by both binaries.
# Dependency-free (just sockets + byte order) so the light client can link it
# without dragging in the engine.  Both mydb_server and mydb_client link this.
add_library(mydb_proto STATIC
    src/protocol.c
)
target_include_directories(mydb_proto PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_compile_options(mydb_proto PRIVATE -Wall -Wextra -Wpedantic)

# mydb_server — server-side library (listener, session, auth, dispatch, event
# loop).  Linked into the mydb-server binary alongside the engine.
add_library(mydb_server STATIC
    src/listener.c
    src/tcp_listener.c
    src/session.c
    src/auth.c
    src/dispatch.c
    src/server.c
)
target_include_directories(mydb_server PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)
# The server calls engine_* (auth, dispatch, logout) and crypto_* (nonce
# generation), and frames packets via mydb_proto.
target_link_libraries(mydb_server PUBLIC engine common_utils mydb_proto)

target_compile_options(mydb_server PRIVATE -Wall -Wextra -Wpedantic)
