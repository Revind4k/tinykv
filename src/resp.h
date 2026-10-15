/* RESP2 request parsing (multibulk and inline) and reply encoding. */
#ifndef TINYKV_RESP_H
#define TINYKV_RESP_H

#include <stddef.h>

#define RESP_MAX_ARGS (1024 * 1024)
#define RESP_MAX_BULK (512L * 1024 * 1024)
#define RESP_MAX_INLINE (64 * 1024)

typedef struct {
    const char *p;
    size_t len;
} arg;

typedef struct {
    arg *v;
    size_t n, cap;
} argvec;

enum { RESP_INCOMPLETE = 0, RESP_OK = 1, RESP_ERROR = -1 };

/* Parses one request from buf[0..len). On RESP_OK, out holds the arguments (pointing into
 * buf) and *consumed the bytes used; a blank inline line yields zero arguments. On
 * RESP_INCOMPLETE nothing is consumed. On RESP_ERROR, *err names the problem and the
 * connection should be closed: the stream can no longer be framed. */
int resp_parse(const char *buf, size_t len, size_t *consumed, argvec *out, const char **err);
void argvec_free(argvec *a);

/* Growable output buffer for replies. Encoding never fails mid-reply: on OOM the buffer
 * is marked failed and the connection is dropped by the caller. */
typedef struct {
    char *buf;
    size_t len, cap, sent;
    int oom;
} outbuf;

void out_raw(outbuf *o, const char *s, size_t n);
void out_simple(outbuf *o, const char *s);          /* +s */
void out_error(outbuf *o, const char *s);           /* -s */
void out_errorf(outbuf *o, const char *fmt, ...);
void out_int(outbuf *o, long long v);                /* :v */
void out_bulk(outbuf *o, const char *s, size_t n);   /* $n s */
void out_null(outbuf *o);                            /* $-1 */
void out_array(outbuf *o, long long n);              /* *n */
void out_free(outbuf *o);

#endif
