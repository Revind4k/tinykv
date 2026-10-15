/* libFuzzer target: the RESP parser must never crash, read out of bounds, or consume bytes
 * it didn't parse, whatever arrives on the socket. Built with ASan + UBSan. */
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "resp.h"

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    /* Copy into an exact-size heap buffer so ASan catches any read past the end. */
    char *buf = malloc(size ? size : 1);
    if (!buf) return 0;
    memcpy(buf, data, size);
    argvec a = {0};
    size_t pos = 0;
    while (pos < size) {
        size_t used;
        const char *err;
        int r = resp_parse(buf + pos, size - pos, &used, &a, &err);
        if (r != RESP_OK) break;
        if (used == 0 || used > size - pos) abort(); /* must make progress, within bounds */
        for (size_t i = 0; i < a.n; i++) {
            const char *p = a.v[i].p;
            if (p < buf + pos || p + a.v[i].len > buf + pos + used) abort(); /* args lie inside the frame */
        }
        pos += used;
    }
    argvec_free(&a);
    free(buf);
    return 0;
}
