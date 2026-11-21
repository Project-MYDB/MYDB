add_library(common_utils STATIC
    src/file_header.c
    src/checksum.c
    src/crypto.c
)

# common.h / file_header.h / checksum.h are genuinely system-wide (every
# module in the repo depends on them) rather than storage-specific, so they
# live here instead of under storage_engine/. Every consumer still reaches
# these headers via a bare #include "common.h" resolved through this
# target's PUBLIC include dir, transitively, exactly as before the move.
target_include_directories(common_utils PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# crypto.{h,c} (SHA-256 + salt helpers) moved here from their own crypto/
# module: they are shared by the engine (login/bootstrap) and by the network
# client, which links no engine, so like the files above they are
# system-wide rather than owned by one module.

target_compile_options(common_utils PRIVATE -Wall -Wextra -Wpedantic)
