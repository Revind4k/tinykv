/* SipHash-1-3 (Aumasson & Bernstein), the keyed hash Redis and Rust use for hash tables.
 * A per-process random key means clients can't pick keys that all collide in one bucket
 * (hash-flooding DoS), which an unkeyed hash like FNV would allow. */
#include "siphash.h"

#include <string.h>

#define ROTL(x, b) (uint64_t)(((x) << (b)) | ((x) >> (64 - (b))))

static uint64_t load64(const uint8_t *p) {
    uint64_t v;
    memcpy(&v, p, 8); /* little-endian hosts only (x86-64, arm64) */
    return v;
}

#define SIPROUND                                                                                  \
    do {                                                                                          \
        v0 += v1; v1 = ROTL(v1, 13); v1 ^= v0; v0 = ROTL(v0, 32);                                  \
        v2 += v3; v3 = ROTL(v3, 16); v3 ^= v2;                                                     \
        v0 += v3; v3 = ROTL(v3, 21); v3 ^= v0;                                                     \
        v2 += v1; v1 = ROTL(v1, 17); v1 ^= v2; v2 = ROTL(v2, 32);                                  \
    } while (0)

uint64_t siphash13(const void *data, size_t len, const uint8_t key[16]) {
    const uint8_t *in = data;
    uint64_t k0 = load64(key), k1 = load64(key + 8);
    uint64_t v0 = 0x736f6d6570736575ULL ^ k0, v1 = 0x646f72616e646f6dULL ^ k1;
    uint64_t v2 = 0x6c7967656e657261ULL ^ k0, v3 = 0x7465646279746573ULL ^ k1;
    const uint8_t *end = in + len - (len % 8);
    for (; in != end; in += 8) {
        uint64_t m = load64(in);
        v3 ^= m;
        SIPROUND;
        v0 ^= m;
    }
    uint64_t b = (uint64_t)len << 56;
    switch (len & 7) {
    case 7: b |= (uint64_t)in[6] << 48; /* fallthrough */
    case 6: b |= (uint64_t)in[5] << 40; /* fallthrough */
    case 5: b |= (uint64_t)in[4] << 32; /* fallthrough */
    case 4: b |= (uint64_t)in[3] << 24; /* fallthrough */
    case 3: b |= (uint64_t)in[2] << 16; /* fallthrough */
    case 2: b |= (uint64_t)in[1] << 8;  /* fallthrough */
    case 1: b |= (uint64_t)in[0];       /* fallthrough */
    case 0: break;
    }
    v3 ^= b;
    SIPROUND;
    v0 ^= b;
    v2 ^= 0xff;
    SIPROUND;
    SIPROUND;
    SIPROUND;
    return v0 ^ v1 ^ v2 ^ v3;
}
