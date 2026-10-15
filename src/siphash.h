#ifndef TINYKV_SIPHASH_H
#define TINYKV_SIPHASH_H

#include <stddef.h>
#include <stdint.h>

uint64_t siphash13(const void *data, size_t len, const uint8_t key[16]);

#endif
