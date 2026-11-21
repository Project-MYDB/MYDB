#include "checksum.h"

uint32_t fnv1a(const void *data, size_t len)
{
    uint32_t h = 0x811c9dc5u;
    const uint8_t *p = data;
    for (size_t i = 0; i < len; i++) {
        h ^= p[i];
        h *= 0x01000193u;
    }
    return h;
}
