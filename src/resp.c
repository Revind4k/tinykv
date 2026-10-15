#include "resp.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int push(argvec *a, const char *p, size_t len) {
    if (a->n == a->cap) {
        size_t cap = a->cap ? a->cap * 2 : 8;
        arg *v = realloc(a->v, cap * sizeof(*v));
        if (!v) return -1;
        a->v = v;
        a->cap = cap;
    }
    a->v[a->n].p = p;
    a->v[a->n].len = len;
    a->n++;
    return 0;
}

void argvec_free(argvec *a) {
    free(a->v);
    memset(a, 0, sizeof(*a));
}

/* Parses a decimal integer terminated by \r\n starting at p. Returns a pointer past the
 * \r\n, NULL if the line isn't complete yet, or sets *bad on a malformed line. */
static const char *read_int_line(const char *p, const char *end, long long *out, int *bad) {
    const char *cr = memchr(p, '\r', (size_t)(end - p));
    if (!cr || cr + 1 >= end) {
        if ((size_t)(end - p) > 32) *bad = 1; /* no sane length field is this long */
        return NULL;
    }
    if (cr[1] != '\n' || cr == p) {
        *bad = 1;
        return NULL;
    }
    int neg = 0;
    const char *q = p;
    if (*q == '-') {
        neg = 1;
        q++;
    }
    if (q == cr || cr - q > 18) {
        *bad = 1;
        return NULL;
    }
    long long v = 0;
    for (; q < cr; q++) {
        if (*q < '0' || *q > '9') {
            *bad = 1;
            return NULL;
        }
        v = v * 10 + (*q - '0');
    }
    *out = neg ? -v : v;
    return cr + 2;
}

static int parse_inline(const char *buf, size_t len, size_t *consumed, argvec *out, const char **err) {
    const char *nl = memchr(buf, '\n', len);
    if (!nl) {
        if (len > RESP_MAX_INLINE) {
            *err = "too big inline request";
            return RESP_ERROR;
        }
        return RESP_INCOMPLETE;
    }
    const char *end = (nl > buf && nl[-1] == '\r') ? nl - 1 : nl;
    for (const char *p = buf; p < end;) {
        while (p < end && (*p == ' ' || *p == '\t')) p++;
        const char *s = p;
        while (p < end && *p != ' ' && *p != '\t') p++;
        if (p > s && push(out, s, (size_t)(p - s)) != 0) {
            *err = "out of memory";
            return RESP_ERROR;
        }
    }
    *consumed = (size_t)(nl - buf) + 1;
    return RESP_OK;
}

int resp_parse(const char *buf, size_t len, size_t *consumed, argvec *out, const char **err) {
    out->n = 0;
    *consumed = 0;
    if (len == 0) return RESP_INCOMPLETE;
    if (buf[0] != '*') return parse_inline(buf, len, consumed, out, err);

    const char *p = buf + 1, *end = buf + len;
    long long count;
    int bad = 0;
    p = read_int_line(p, end, &count, &bad);
    if (bad || (p && (count > RESP_MAX_ARGS))) {
        *err = "invalid multibulk length";
        return RESP_ERROR;
    }
    if (!p) return RESP_INCOMPLETE;
    for (long long i = 0; i < count; i++) {
        if (p >= end) return RESP_INCOMPLETE;
        if (*p != '$') {
            *err = "expected '$'";
            return RESP_ERROR;
        }
        long long blen;
        const char *q = read_int_line(p + 1, end, &blen, &bad);
        if (bad || (q && (blen < 0 || blen > RESP_MAX_BULK))) {
            *err = "invalid bulk length";
            return RESP_ERROR;
        }
        if (!q) return RESP_INCOMPLETE;
        if ((size_t)(end - q) < (size_t)blen + 2) return RESP_INCOMPLETE;
        if (q[blen] != '\r' || q[blen + 1] != '\n') {
            *err = "bulk string not terminated by CRLF";
            return RESP_ERROR;
        }
        if (push(out, q, (size_t)blen) != 0) {
            *err = "out of memory";
            return RESP_ERROR;
        }
        p = q + blen + 2;
    }
    *consumed = (size_t)(p - buf); /* "*0\r\n" and "*-1\r\n" consume and yield no args */
    return RESP_OK;
}

/* ---- replies ---- */

static int reserve(outbuf *o, size_t n) {
    if (o->oom) return -1;
    if (o->len + n <= o->cap) return 0;
    size_t cap = o->cap ? o->cap : 256;
    while (cap < o->len + n) cap *= 2;
    char *b = realloc(o->buf, cap);
    if (!b) {
        o->oom = 1;
        return -1;
    }
    o->buf = b;
    o->cap = cap;
    return 0;
}

void out_raw(outbuf *o, const char *s, size_t n) {
    if (reserve(o, n) == 0) {
        memcpy(o->buf + o->len, s, n);
        o->len += n;
    }
}

static void out_line(outbuf *o, char type, const char *s, size_t n) {
    if (reserve(o, n + 3) != 0) return;
    o->buf[o->len++] = type;
    memcpy(o->buf + o->len, s, n);
    o->len += n;
    o->buf[o->len++] = '\r';
    o->buf[o->len++] = '\n';
}

void out_simple(outbuf *o, const char *s) { out_line(o, '+', s, strlen(s)); }
void out_error(outbuf *o, const char *s) { out_line(o, '-', s, strlen(s)); }

void out_errorf(outbuf *o, const char *fmt, ...) {
    char b[512];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(b, sizeof(b), fmt, ap);
    va_end(ap);
    if (n < 0) n = 0;
    if ((size_t)n >= sizeof(b)) n = sizeof(b) - 1;
    for (int i = 0; i < n; i++)
        if (b[i] == '\r' || b[i] == '\n') b[i] = ' '; /* an error line must stay one line */
    out_line(o, '-', b, (size_t)n);
}

static void out_num(outbuf *o, char type, long long v) {
    char b[24];
    int n = snprintf(b, sizeof(b), "%lld", v);
    out_line(o, type, b, (size_t)n);
}

void out_int(outbuf *o, long long v) { out_num(o, ':', v); }
void out_array(outbuf *o, long long n) { out_num(o, '*', n); }
void out_null(outbuf *o) { out_raw(o, "$-1\r\n", 5); }

void out_bulk(outbuf *o, const char *s, size_t n) {
    out_num(o, '$', (long long)n);
    if (reserve(o, n + 2) != 0) return;
    memcpy(o->buf + o->len, s, n);
    o->len += n;
    o->buf[o->len++] = '\r';
    o->buf[o->len++] = '\n';
}

void out_free(outbuf *o) {
    free(o->buf);
    memset(o, 0, sizeof(*o));
}
