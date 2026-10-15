/* Unit tests for the hash table and RESP parser. Built with ASan + UBSan (make unit). */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "dict.h"
#include "resp.h"
#include "siphash.h"

static int failures, checks;
#define CHECK(cond)                                                              \
    do {                                                                         \
        checks++;                                                                \
        if (!(cond)) {                                                           \
            failures++;                                                          \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
        }                                                                        \
    } while (0)

static const uint8_t SEED[16] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

static void test_siphash(void) {
    uint8_t other[16];
    memcpy(other, SEED, 16);
    other[0] ^= 1;
    CHECK(siphash13("abc", 3, SEED) == siphash13("abc", 3, SEED));
    CHECK(siphash13("abc", 3, SEED) != siphash13("abd", 3, SEED));
    CHECK(siphash13("abc", 3, SEED) != siphash13("abc", 3, other)); /* the key matters */
    CHECK(siphash13("", 0, SEED) != siphash13("\0", 1, SEED));      /* length is mixed in */
    char buf[64];
    for (size_t n = 0; n < sizeof(buf); n++) buf[n] = (char)n; /* every tail length 0..7 */
    for (size_t n = 1; n < sizeof(buf); n++) CHECK(siphash13(buf, n, SEED) != siphash13(buf, n - 1, SEED));
}

static void test_dict_grow_and_rehash(void) {
    dict d;
    dict_init(&d, SEED);
    char k[32], v[32];
    const int N = 100000;
    for (int i = 0; i < N; i++) {
        int n = snprintf(k, sizeof(k), "key:%d", i), m = snprintf(v, sizeof(v), "v%d", i);
        int created = -1;
        CHECK(dict_set(&d, k, (size_t)n, v, (size_t)m, &created) != NULL);
        CHECK(created == 1);
        /* Every key inserted so far must stay reachable mid-rehash: spot-check a few. */
        if (i % 9973 == 0)
            for (int j = 0; j <= i; j += 997) {
                int kn = snprintf(k, sizeof(k), "key:%d", j);
                CHECK(dict_find(&d, k, (size_t)kn) != NULL);
            }
    }
    CHECK(dict_size(&d) == (size_t)N);
    for (int i = 0; i < N; i++) {
        int n = snprintf(k, sizeof(k), "key:%d", i), m = snprintf(v, sizeof(v), "v%d", i);
        entry *e = dict_find(&d, k, (size_t)n);
        CHECK(e && e->vlen == (size_t)m && memcmp(e->val, v, (size_t)m) == 0);
    }
    /* Overwrite keeps the count and replaces the value. */
    int created = -1;
    entry *e = dict_set(&d, "key:7", 5, "new", 3, &created);
    CHECK(created == 0 && dict_size(&d) == (size_t)N && e->vlen == 3);
    /* Delete everything; the table can then shrink back down. */
    for (int i = 0; i < N; i++) {
        int n = snprintf(k, sizeof(k), "key:%d", i);
        CHECK(dict_delete(&d, k, (size_t)n) == 1);
    }
    CHECK(dict_size(&d) == 0);
    CHECK(dict_delete(&d, "key:1", 5) == 0);
    while (dict_rehash(&d, 1000)) {
    }
    size_t before = d.ht[0].size;
    dict_maybe_shrink(&d);
    while (dict_rehash(&d, 1000)) {
    }
    CHECK(d.ht[0].size < before);
    dict_free(&d);
}

static void test_dict_binary_keys(void) {
    dict d;
    dict_init(&d, SEED);
    CHECK(dict_set(&d, "a\0b", 3, "x", 1, NULL));
    CHECK(dict_set(&d, "a\0c", 3, "y", 1, NULL));
    CHECK(dict_set(&d, "", 0, "empty", 5, NULL)); /* the empty key is a valid key */
    CHECK(dict_size(&d) == 3);
    entry *e = dict_find(&d, "a\0c", 3);
    CHECK(e && e->val[0] == 'y');
    CHECK(dict_find(&d, "a", 1) == NULL);
    CHECK(dict_find(&d, "", 0) != NULL);
    dict_free(&d);
}

static void test_dict_ttl_index(void) {
    dict d;
    dict_init(&d, SEED);
    char k[16];
    for (int i = 0; i < 100; i++) {
        int n = snprintf(k, sizeof(k), "t%d", i);
        entry *e = dict_set(&d, k, (size_t)n, "v", 1, NULL);
        if (i % 2 == 0) dict_set_expire(&d, e, 1000 + i);
    }
    CHECK(d.ttl_len == 50);
    /* Removing TTLs and deleting keys keeps the index dense and consistent. */
    for (int i = 0; i < 100; i += 4) {
        int n = snprintf(k, sizeof(k), "t%d", i);
        dict_delete(&d, k, (size_t)n);
    }
    CHECK(d.ttl_len == 25);
    entry *e = dict_find(&d, "t2", 2);
    dict_set_expire(&d, e, NO_EXPIRE);
    CHECK(d.ttl_len == 24);
    for (size_t i = 0; i < d.ttl_len; i++) CHECK(d.ttl[i]->ttl_idx == (ssize_t)i && d.ttl[i]->expire_at != NO_EXPIRE);
    for (int i = 0; i < 1000; i++) CHECK(dict_random_ttl(&d)->expire_at != NO_EXPIRE);
    /* SET on an existing key clears its TTL, as in Redis. */
    e = dict_find(&d, "t6", 2);
    CHECK(e->expire_at != NO_EXPIRE);
    dict_set(&d, "t6", 2, "w", 1, NULL);
    CHECK(dict_find(&d, "t6", 2)->expire_at == NO_EXPIRE && d.ttl_len == 23);
    dict_free(&d);
}

static int parse(const char *s, size_t n, size_t *used, argvec *a) {
    const char *err;
    return resp_parse(s, n, used, a, &err);
}

static void test_resp_multibulk_every_split(void) {
    const char *req = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$11\r\nhello\r\nworl\r\n";
    size_t len = strlen(req), used;
    argvec a = {0};
    for (size_t cut = 0; cut < len; cut++) /* any shorter prefix is incomplete, never an error */
        CHECK(parse(req, cut, &used, &a) == RESP_INCOMPLETE && used == 0);
    CHECK(parse(req, len, &used, &a) == RESP_OK);
    CHECK(used == len && a.n == 3);
    CHECK(a.v[2].len == 11 && memcmp(a.v[2].p, "hello\r\nworl", 11) == 0); /* binary-safe */
    argvec_free(&a);
}

static void test_resp_pipeline(void) {
    const char *req = "*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\nPING\r\n";
    size_t len = strlen(req), pos = 0, used;
    argvec a = {0};
    const char *names[] = {"PING", "GET", "PING"};
    for (int i = 0; i < 3; i++) {
        CHECK(parse(req + pos, len - pos, &used, &a) == RESP_OK);
        CHECK(a.v[0].len == 4 || a.v[0].len == 3);
        CHECK(memcmp(a.v[0].p, names[i], a.v[0].len) == 0);
        pos += used;
    }
    CHECK(pos == len);
    argvec_free(&a);
}

static void test_resp_inline(void) {
    size_t used;
    argvec a = {0};
    CHECK(parse("  set  k   v\r\n", 14, &used, &a) == RESP_OK && a.n == 3 && used == 14);
    CHECK(parse("ping\n", 5, &used, &a) == RESP_OK && a.n == 1);
    CHECK(parse("\r\n", 2, &used, &a) == RESP_OK && a.n == 0 && used == 2); /* blank line */
    CHECK(parse("get k", 5, &used, &a) == RESP_INCOMPLETE);
    char *big = malloc(RESP_MAX_INLINE + 2);
    memset(big, 'a', RESP_MAX_INLINE + 1);
    CHECK(parse(big, RESP_MAX_INLINE + 1, &used, &a) == RESP_ERROR);
    free(big);
    argvec_free(&a);
}

static void test_resp_rejects_malformed(void) {
    const char *bad[] = {
        "*x\r\n",                 /* non-numeric count */
        "*2\r\n+OK\r\n",          /* element isn't a bulk string */
        "*1\r\n$-5\r\n",          /* negative bulk length */
        "*1\r\n$3\r\nabcde\r\n",  /* bulk not followed by CRLF */
        "*1\r\n$99999999999\r\n", /* over the 512 MB bulk limit */
        "*9999999\r\n",           /* over the argument-count limit */
        "*1\nx\r\n",              /* LF inside the count: bad once the CRLF arrives */
        "*\r\n",                  /* empty count */
        "*12345678901234567890123456789012345", /* length field that never ends */
    };
    for (size_t i = 0; i < sizeof(bad) / sizeof(*bad); i++) {
        size_t used;
        argvec a = {0};
        int r = parse(bad[i], strlen(bad[i]), &used, &a);
        if (r != RESP_ERROR) fprintf(stderr, "  accepted malformed input #%zu\n", i);
        CHECK(r == RESP_ERROR);
        argvec_free(&a);
    }
    size_t used;
    argvec a = {0};
    CHECK(parse("*0\r\n", 4, &used, &a) == RESP_OK && a.n == 0 && used == 4);
    CHECK(parse("*1\r\n$0\r\n\r\n", 10, &used, &a) == RESP_OK && a.n == 1 && a.v[0].len == 0);
    argvec_free(&a);
}

static void test_out(void) {
    outbuf o = {0};
    out_simple(&o, "OK");
    out_int(&o, -42);
    out_bulk(&o, "a\r\nb", 4);
    out_null(&o);
    out_array(&o, 2);
    out_errorf(&o, "ERR bad\r\ninjected");
    const char *want = "+OK\r\n:-42\r\n$4\r\na\r\nb\r\n$-1\r\n*2\r\n-ERR bad  injected\r\n";
    CHECK(o.len == strlen(want) && memcmp(o.buf, want, o.len) == 0);
    out_free(&o);
}

int main(void) {
    test_siphash();
    test_dict_grow_and_rehash();
    test_dict_binary_keys();
    test_dict_ttl_index();
    test_resp_multibulk_every_split();
    test_resp_pipeline();
    test_resp_inline();
    test_resp_rejects_malformed();
    test_out();
    printf("%d checks, %d failures\n", checks, failures);
    return failures != 0;
}
