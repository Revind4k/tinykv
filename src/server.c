/* tinykv: a single-threaded, Redis-compatible (RESP2) key-value server.
 *
 * One event loop owns all state, so commands need no locks and run one at a time. Replies
 * are not written while a command runs; they are queued, and before the loop sleeps it
 * first flushes the append-only file and only then writes replies. With
 * --appendfsync always, a client never sees "OK" for a write that isn't on disk. */
#define _GNU_SOURCE
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "dict.h"
#include "event.h"
#include "resp.h"

#define TINYKV_VERSION "0.1.0"
#define READ_CHUNK (16 * 1024)
#define MAX_QUERY_BUF (1024L * 1024 * 1024) /* same default as Redis' client-query-buffer-limit */
/* Backpressure: once a client has this much unsent output, stop executing its pipeline and
 * stop reading its socket until the replies drain. A client that pipelines 200 GETs of a
 * 1 MB value and never reads would otherwise make the loop build 200 MB of replies in one
 * pass, stalling every other client and growing memory without bound. */
#define OUT_SOFT_LIMIT (1L * 1024 * 1024)
/* Hard cap for a single enormous reply (a 512 MB value, KEYS over a huge keyspace). */
#define MAX_PENDING_OUT (1024L * 1024 * 1024)
#define CRON_MS 100
#define AUTO_REWRITE_MIN (64L * 1024 * 1024)

enum { FSYNC_NO, FSYNC_EVERYSEC, FSYNC_ALWAYS };

typedef struct client {
    int fd;
    int mask;
    char *qbuf;
    size_t qlen, qcap;
    outbuf out;
    argvec argv;
    int close_after_reply;
    int paused;           /* input held back until output drains (see OUT_SOFT_LIMIT) */
    int pending;
    struct client *next_pending;
} client;

static struct {
    dict db;
    int listen_fd, ep;
    int port;
    const char *bind;
    client **byfd;
    int byfd_cap;
    long connected;
    long max_clients;
    client *pending;

    const char *aof_path; /* NULL: persistence off */
    int aof_fd;
    int fsync_policy;
    outbuf aofbuf;
    long long aof_size, aof_base_size;
    long long last_fsync_ms;
    int aof_failed;
    pid_t rewrite_pid;
    outbuf rewrite_buf;
    char rewrite_tmp[PATH_MAX];
    long long rewrites_done;

    int loading;
    long long start_ms;
    long long stat_cmds, stat_conns, stat_expired, stat_rejected;
    volatile sig_atomic_t shutdown;
} S;

static long long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (long long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void logf_(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static void logf_(const char *fmt, ...) {
    char ts[32];
    time_t t = time(NULL);
    strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", localtime(&t));
    fprintf(stderr, "%s tinykv[%d] ", ts, (int)getpid());
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fputc('\n', stderr);
}

/* ---------------------------------------------------------------- strings & ints */

/* Strict integer parse matching Redis: optional '-', no '+', no spaces, no leading zeros. */
static int parse_ll(const char *s, size_t n, long long *out) {
    if (n == 0 || n > 20) return -1;
    size_t i = 0;
    int neg = 0;
    if (s[0] == '-') {
        neg = 1;
        if (++i == n) return -1;
    }
    if (s[i] == '0' && n - i > 1) return -1;
    unsigned long long v = 0;
    for (; i < n; i++) {
        if (s[i] < '0' || s[i] > '9') return -1;
        unsigned long long d = (unsigned long long)(s[i] - '0');
        if (v > (ULLONG_MAX - d) / 10) return -1;
        v = v * 10 + d;
    }
    if (neg) {
        if (v > (unsigned long long)LLONG_MAX + 1) return -1;
        *out = (long long)(0 - v);
    } else {
        if (v > (unsigned long long)LLONG_MAX) return -1;
        *out = (long long)v;
    }
    return 0;
}

static int arg_is(const arg *a, const char *s) {
    size_t n = strlen(s);
    return a->len == n && strncasecmp(a->p, s, n) == 0;
}

/* Glob match (*, ?, [abc], [a-z], [^x], \x) for KEYS. Iterative with one backtrack point,
 * so a pattern like "*a*a*a*b" can't go exponential. */
static int glob_match(const char *p, size_t pl, const char *s, size_t sl) {
    size_t pi = 0, si = 0, star_p = SIZE_MAX, star_s = 0;
    while (si < sl) {
        if (pi < pl && p[pi] == '*') {
            star_p = pi++;
            star_s = si;
            continue;
        }
        if (pi < pl) {
            int ok = 0;
            size_t adv = 1;
            if (p[pi] == '?') {
                ok = 1;
            } else if (p[pi] == '[') {
                size_t j = pi + 1;
                int neg = j < pl && p[j] == '^';
                if (neg) j++;
                int hit = 0;
                for (; j < pl && p[j] != ']'; j++) {
                    if (p[j] == '\\' && j + 1 < pl) {
                        j++;
                        if (p[j] == s[si]) hit = 1;
                    } else if (j + 2 < pl && p[j + 1] == '-' && p[j + 2] != ']') {
                        unsigned char lo = (unsigned char)p[j], hi = (unsigned char)p[j + 2];
                        if (lo > hi) { unsigned char t = lo; lo = hi; hi = t; }
                        if ((unsigned char)s[si] >= lo && (unsigned char)s[si] <= hi) hit = 1;
                        j += 2;
                    } else if (p[j] == s[si]) {
                        hit = 1;
                    }
                }
                ok = neg ? !hit : hit;
                adv = (j < pl ? j + 1 : j) - pi;
            } else if (p[pi] == '\\' && pi + 1 < pl) {
                ok = p[pi + 1] == s[si];
                adv = 2;
            } else {
                ok = p[pi] == s[si];
            }
            if (ok) {
                pi += adv;
                si++;
                continue;
            }
        }
        if (star_p == SIZE_MAX) return 0;
        pi = star_p + 1;
        si = ++star_s;
    }
    while (pi < pl && p[pi] == '*') pi++;
    return pi == pl;
}

/* ---------------------------------------------------------------- persistence */

static void aof_append_cmd(outbuf *o, int argc, const arg *argv) {
    out_array(o, argc);
    for (int i = 0; i < argc; i++) out_bulk(o, argv[i].p, argv[i].len);
}

/* Records a write in the AOF (and the rewrite buffer, while a rewrite child runs). */
static void propagate(int argc, const arg *argv) {
    if (!S.aof_path || S.loading) return;
    aof_append_cmd(&S.aofbuf, argc, argv);
    if (S.rewrite_pid > 0) aof_append_cmd(&S.rewrite_buf, argc, argv);
}

static void propagate_set(const entry *e) {
    char ms[24];
    arg v[5] = {{"SET", 3}, {e->key, e->klen}, {e->val, e->vlen}, {"PXAT", 4}, {ms, 0}};
    int n = 3;
    if (e->expire_at != NO_EXPIRE) {
        v[4].len = (size_t)snprintf(ms, sizeof(ms), "%lld", (long long)e->expire_at);
        n = 5;
    }
    propagate(n, v);
}

static int write_all(int fd, const char *p, size_t n) {
    while (n) {
        ssize_t w = write(fd, p, n);
        if (w < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        p += w;
        n -= (size_t)w;
    }
    return 0;
}

static void aof_flush(int force_fsync) {
    if (!S.aof_path || S.aof_fd < 0) return;
    if (S.aofbuf.oom) {
        logf_("AOF buffer out of memory; refusing further writes");
        S.aof_failed = 1;
        out_free(&S.aofbuf);
    }
    if (S.aofbuf.len) {
        if (write_all(S.aof_fd, S.aofbuf.buf, S.aofbuf.len) != 0) {
            logf_("AOF write failed: %s; refusing further writes", strerror(errno));
            S.aof_failed = 1;
        } else {
            S.aof_size += (long long)S.aofbuf.len;
        }
        S.aofbuf.len = 0;
        if (S.fsync_policy == FSYNC_ALWAYS) force_fsync = 1;
    }
    if (force_fsync) {
        fsync(S.aof_fd);
        S.last_fsync_ms = now_ms();
    }
}

typedef struct {
    outbuf o;
    int fd;
} snapshot;

static void snapshot_entry(entry *e, void *ctx) {
    snapshot *sn = ctx;
    outbuf *o = &sn->o;
    if (e->expire_at != NO_EXPIRE && e->expire_at <= now_ms()) return;
    char ms[24];
    arg v[5] = {{"SET", 3}, {e->key, e->klen}, {e->val, e->vlen}, {"PXAT", 4}, {ms, 0}};
    int n = 3;
    if (e->expire_at != NO_EXPIRE) {
        v[4].len = (size_t)snprintf(ms, sizeof(ms), "%lld", (long long)e->expire_at);
        n = 5;
    }
    aof_append_cmd(o, n, v);
    if (o->len > (1 << 20)) { /* stream to disk in 1 MB pieces */
        if (write_all(sn->fd, o->buf, o->len) != 0) _exit(1);
        o->len = 0;
    }
}

/* Compacts the AOF in a forked child. The child writes one SET per live key from its
 * copy-on-write view of memory while the parent keeps serving; writes that arrive meanwhile
 * are buffered and appended to the new file before it atomically replaces the old one. */
static int aof_rewrite_start(void) {
    if (!S.aof_path || S.rewrite_pid > 0) return -1;
    snprintf(S.rewrite_tmp, sizeof(S.rewrite_tmp), "%s.rewrite.%d", S.aof_path, (int)getpid());
    aof_flush(0);
    pid_t pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) {
        int fd = open(S.rewrite_tmp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) _exit(1);
        snapshot w = {{0}, fd};
        dict_foreach(&S.db, snapshot_entry, &w);
        if (w.o.oom || write_all(fd, w.o.buf, w.o.len) != 0 || fsync(fd) != 0) _exit(1);
        _exit(0);
    }
    S.rewrite_pid = pid;
    S.rewrite_buf.len = 0;
    logf_("background AOF rewrite started by pid %d", (int)pid);
    return 0;
}

static void aof_rewrite_done(int status) {
    pid_t pid = S.rewrite_pid;
    S.rewrite_pid = -1;
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        logf_("AOF rewrite child %d failed; keeping the old file", (int)pid);
        unlink(S.rewrite_tmp);
        return;
    }
    /* Writes still in aofbuf are also in rewrite_buf: send them to the old file now, so they
     * aren't written to the new file twice after the swap. */
    aof_flush(0);
    int fd = open(S.rewrite_tmp, O_WRONLY | O_APPEND);
    if (fd < 0 || S.rewrite_buf.oom || write_all(fd, S.rewrite_buf.buf, S.rewrite_buf.len) != 0 || fsync(fd) != 0) {
        logf_("AOF rewrite: appending buffered writes failed; keeping the old file");
        if (fd >= 0) close(fd);
        unlink(S.rewrite_tmp);
        return;
    }
    if (rename(S.rewrite_tmp, S.aof_path) != 0) {
        logf_("AOF rewrite: rename failed: %s", strerror(errno));
        close(fd);
        unlink(S.rewrite_tmp);
        return;
    }
    close(S.aof_fd);
    S.aof_fd = fd;
    struct stat st;
    S.aof_size = S.aof_base_size = fstat(fd, &st) == 0 ? (long long)st.st_size : 0;
    out_free(&S.rewrite_buf);
    S.rewrites_done++;
    logf_("background AOF rewrite finished: %lld bytes", S.aof_size);
}

/* ---------------------------------------------------------------- keyspace */

static int expired(const entry *e) {
    /* While loading the AOF nothing expires: a later command in the log may depend on the
     * key (INCR on a key that was live when it ran). Expiry resumes once loading ends. */
    return !S.loading && e->expire_at != NO_EXPIRE && e->expire_at <= now_ms();
}

static entry *lookup(const arg *k) {
    entry *e = dict_find(&S.db, k->p, k->len);
    if (e && expired(e)) {
        dict_delete(&S.db, k->p, k->len);
        S.stat_expired++;
        return NULL;
    }
    return e;
}

/* Samples keys with a TTL and deletes expired ones; repeats while more than a quarter of
 * a sample was expired, within a time budget, so memory held by keys nobody reads again
 * is still reclaimed without stalling the loop. */
static void active_expire(void) {
    long long deadline = now_ms() + 1, t = now_ms();
    for (;;) {
        int sampled = 0, gone = 0;
        for (int i = 0; i < 20 && S.db.ttl_len; i++) {
            entry *e = dict_random_ttl(&S.db);
            sampled++;
            if (e->expire_at <= t) {
                dict_delete(&S.db, e->key, e->klen);
                S.stat_expired++;
                gone++;
            }
        }
        if (sampled == 0 || gone * 4 <= sampled) return;
        if ((t = now_ms()) >= deadline) return;
    }
}

/* ---------------------------------------------------------------- commands */

typedef void (*cmd_fn)(client *c, int argc, arg *argv);
typedef struct {
    const char *name;
    int arity; /* exact if positive, minimum if negative (counting the name) */
    int write;
    cmd_fn fn;
} command;

#define O (&c->out)

static void cmd_ping(client *c, int argc, arg *argv) {
    if (argc == 2) out_bulk(O, argv[1].p, argv[1].len);
    else out_simple(O, "PONG");
}

static void cmd_echo(client *c, int argc, arg *argv) {
    (void)argc;
    out_bulk(O, argv[1].p, argv[1].len);
}

static void cmd_quit(client *c, int argc, arg *argv) {
    (void)argc;
    (void)argv;
    out_simple(O, "OK");
    c->close_after_reply = 1;
}

static void cmd_get(client *c, int argc, arg *argv) {
    (void)argc;
    entry *e = lookup(&argv[1]);
    if (e) out_bulk(O, e->val, e->vlen);
    else out_null(O);
}

/* SET key value [NX|XX] [GET] [EX s|PX ms|EXAT s|PXAT ms|KEEPTTL] */
static void cmd_set(client *c, int argc, arg *argv) {
    int nx = 0, xx = 0, get = 0, keepttl = 0, has_exp = 0;
    long long expire_at = NO_EXPIRE;
    for (int i = 3; i < argc; i++) {
        arg *a = &argv[i];
        if (arg_is(a, "NX")) nx = 1;
        else if (arg_is(a, "XX")) xx = 1;
        else if (arg_is(a, "GET")) get = 1;
        else if (arg_is(a, "KEEPTTL")) keepttl = 1;
        else if ((arg_is(a, "EX") || arg_is(a, "PX") || arg_is(a, "EXAT") || arg_is(a, "PXAT")) && i + 1 < argc && !has_exp) {
            long long v;
            if (parse_ll(argv[i + 1].p, argv[i + 1].len, &v) != 0) {
                out_error(O, "ERR value is not an integer or out of range");
                return;
            }
            int secs = arg_is(a, "EX") || arg_is(a, "EXAT");
            int rel = arg_is(a, "EX") || arg_is(a, "PX");
            if (v <= 0 || (secs && v > LLONG_MAX / 1000)) {
                out_errorf(O, "ERR invalid expire time in 'set' command");
                return;
            }
            long long ms = secs ? v * 1000 : v;
            if (rel && ms > LLONG_MAX - now_ms()) {
                out_errorf(O, "ERR invalid expire time in 'set' command");
                return;
            }
            expire_at = rel ? now_ms() + ms : ms;
            has_exp = 1;
            i++;
        } else {
            out_error(O, "ERR syntax error");
            return;
        }
    }
    if ((nx && xx) || (keepttl && has_exp)) {
        out_error(O, "ERR syntax error");
        return;
    }
    entry *old = lookup(&argv[1]);
    if (get) {
        if (old) out_bulk(O, old->val, old->vlen);
        else out_null(O);
    }
    if ((nx && old) || (xx && !old)) {
        if (!get) out_null(O);
        return;
    }
    long long keep = (keepttl && old) ? old->expire_at : NO_EXPIRE;
    entry *e = dict_set(&S.db, argv[1].p, argv[1].len, argv[2].p, argv[2].len, NULL);
    if (!e) {
        out_error(O, "OOM command not allowed when used memory > 'maxmemory'.");
        return;
    }
    if (has_exp) dict_set_expire(&S.db, e, expire_at);
    else if (keep != NO_EXPIRE) dict_set_expire(&S.db, e, keep);
    propagate_set(e); /* relative TTLs are logged as absolute PXAT, so replay is exact */
    if (!get) out_simple(O, "OK");
}

static void cmd_setnx(client *c, int argc, arg *argv) {
    (void)argc;
    if (lookup(&argv[1])) {
        out_int(O, 0);
        return;
    }
    entry *e = dict_set(&S.db, argv[1].p, argv[1].len, argv[2].p, argv[2].len, NULL);
    if (!e) {
        out_error(O, "OOM command not allowed when used memory > 'maxmemory'.");
        return;
    }
    propagate_set(e);
    out_int(O, 1);
}

static void cmd_getdel(client *c, int argc, arg *argv) {
    (void)argc;
    entry *e = lookup(&argv[1]);
    if (!e) {
        out_null(O);
        return;
    }
    out_bulk(O, e->val, e->vlen);
    dict_delete(&S.db, argv[1].p, argv[1].len);
    arg v[2] = {{"DEL", 3}, argv[1]};
    propagate(2, v);
}

static void cmd_mget(client *c, int argc, arg *argv) {
    out_array(O, argc - 1);
    for (int i = 1; i < argc; i++) {
        entry *e = lookup(&argv[i]);
        if (e) out_bulk(O, e->val, e->vlen);
        else out_null(O);
    }
}

static void cmd_mset(client *c, int argc, arg *argv) {
    if (argc % 2 == 0) {
        out_error(O, "ERR wrong number of arguments for 'mset' command");
        return;
    }
    for (int i = 1; i < argc; i += 2)
        if (!dict_set(&S.db, argv[i].p, argv[i].len, argv[i + 1].p, argv[i + 1].len, NULL)) {
            out_error(O, "OOM command not allowed when used memory > 'maxmemory'.");
            return;
        }
    argv[0].p = "MSET";
    argv[0].len = 4;
    propagate(argc, argv);
    out_simple(O, "OK");
}

static void cmd_del(client *c, int argc, arg *argv) {
    long long n = 0;
    for (int i = 1; i < argc; i++)
        if (lookup(&argv[i]) && dict_delete(&S.db, argv[i].p, argv[i].len)) {
            arg v[2] = {{"DEL", 3}, argv[i]};
            propagate(2, v);
            n++;
        }
    out_int(O, n);
}

static void cmd_exists(client *c, int argc, arg *argv) {
    long long n = 0;
    for (int i = 1; i < argc; i++) n += lookup(&argv[i]) != NULL; /* repeats count, as in Redis */
    out_int(O, n);
}

static void incr_by(client *c, arg *key, long long by) {
    entry *e = lookup(key);
    long long cur = 0;
    if (e && parse_ll(e->val, e->vlen, &cur) != 0) {
        out_error(O, "ERR value is not an integer or out of range");
        return;
    }
    if ((by > 0 && cur > LLONG_MAX - by) || (by < 0 && cur < LLONG_MIN - by)) {
        out_error(O, "ERR increment or decrement would overflow");
        return;
    }
    cur += by;
    char b[24];
    size_t n = (size_t)snprintf(b, sizeof(b), "%lld", cur);
    if (e) {
        if (dict_set_value(e, b, n) != 0) { /* keeps the TTL, like Redis */
            out_error(O, "OOM command not allowed when used memory > 'maxmemory'.");
            return;
        }
    } else if (!(e = dict_set(&S.db, key->p, key->len, b, n, NULL))) {
        out_error(O, "OOM command not allowed when used memory > 'maxmemory'.");
        return;
    }
    propagate_set(e);
    out_int(O, cur);
}

static void cmd_incr(client *c, int argc, arg *argv) { (void)argc; incr_by(c, &argv[1], 1); }
static void cmd_decr(client *c, int argc, arg *argv) { (void)argc; incr_by(c, &argv[1], -1); }

static void cmd_incrby(client *c, int argc, arg *argv) {
    (void)argc;
    long long by;
    if (parse_ll(argv[2].p, argv[2].len, &by) != 0) {
        out_error(O, "ERR value is not an integer or out of range");
        return;
    }
    incr_by(c, &argv[1], by);
}

static void cmd_decrby(client *c, int argc, arg *argv) {
    (void)argc;
    long long by;
    if (parse_ll(argv[2].p, argv[2].len, &by) != 0) {
        out_error(O, "ERR value is not an integer or out of range");
        return;
    }
    if (by == LLONG_MIN) { /* -LLONG_MIN doesn't fit; Redis words this one differently */
        out_error(O, "ERR decrement would overflow");
        return;
    }
    incr_by(c, &argv[1], -by);
}

static void cmd_append(client *c, int argc, arg *argv) {
    (void)argc;
    entry *e = lookup(&argv[1]);
    if (!e) {
        e = dict_set(&S.db, argv[1].p, argv[1].len, argv[2].p, argv[2].len, NULL);
    } else {
        char *v = realloc(e->val, e->vlen + argv[2].len + 1);
        if (v) {
            memcpy(v + e->vlen, argv[2].p, argv[2].len);
            e->val = v;
            e->vlen += argv[2].len;
        } else {
            e = NULL;
        }
    }
    if (!e) {
        out_error(O, "OOM command not allowed when used memory > 'maxmemory'.");
        return;
    }
    propagate_set(e);
    out_int(O, (long long)e->vlen);
}

static void cmd_strlen(client *c, int argc, arg *argv) {
    (void)argc;
    entry *e = lookup(&argv[1]);
    out_int(O, e ? (long long)e->vlen : 0);
}

/* EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT key n [NX|XX|GT|LT] */
static void generic_expire(client *c, int argc, arg *argv, int secs, int rel) {
    long long v;
    if (parse_ll(argv[2].p, argv[2].len, &v) != 0) {
        out_error(O, "ERR value is not an integer or out of range");
        return;
    }
    int nx = 0, xx = 0, gt = 0, lt = 0;
    for (int i = 3; i < argc; i++) {
        if (arg_is(&argv[i], "NX")) nx = 1;
        else if (arg_is(&argv[i], "XX")) xx = 1;
        else if (arg_is(&argv[i], "GT")) gt = 1;
        else if (arg_is(&argv[i], "LT")) lt = 1;
        else {
            out_errorf(O, "ERR Unsupported option %.*s", (int)argv[i].len, argv[i].p);
            return;
        }
    }
    if (nx + xx + gt + lt > 1 && !(gt + lt == 1 && xx && !nx)) {
        out_error(O, "ERR NX and XX, GT or LT options at the same time are not compatible");
        return;
    }
    if (secs && (v > LLONG_MAX / 1000 || v < LLONG_MIN / 1000)) {
        out_error(O, "ERR invalid expire time in 'expire' command");
        return;
    }
    long long ms = secs ? v * 1000 : v;
    long long base = rel ? now_ms() : 0;
    if (rel && ms > LLONG_MAX - base) {
        out_error(O, "ERR invalid expire time in 'expire' command");
        return;
    }
    long long at = base + ms;
    entry *e = lookup(&argv[1]);
    if (!e) {
        out_int(O, 0);
        return;
    }
    int has = e->expire_at != NO_EXPIRE;
    if ((nx && has) || (xx && !has) || (gt && (!has || at <= e->expire_at)) || (lt && has && at >= e->expire_at)) {
        out_int(O, 0);
        return;
    }
    if (at <= now_ms() && !S.loading) { /* a time in the past deletes the key */
        dict_delete(&S.db, argv[1].p, argv[1].len);
        arg d[2] = {{"DEL", 3}, argv[1]};
        propagate(2, d);
    } else {
        dict_set_expire(&S.db, e, at);
        char b[24];
        arg p[3] = {{"PEXPIREAT", 9}, argv[1], {b, (size_t)snprintf(b, sizeof(b), "%lld", at)}};
        propagate(3, p);
    }
    out_int(O, 1);
}

static void cmd_expire(client *c, int argc, arg *argv) { generic_expire(c, argc, argv, 1, 1); }
static void cmd_pexpire(client *c, int argc, arg *argv) { generic_expire(c, argc, argv, 0, 1); }
static void cmd_expireat(client *c, int argc, arg *argv) { generic_expire(c, argc, argv, 1, 0); }
static void cmd_pexpireat(client *c, int argc, arg *argv) { generic_expire(c, argc, argv, 0, 0); }

static void generic_ttl(client *c, arg *key, int ms) {
    entry *e = lookup(key);
    if (!e) out_int(O, -2);
    else if (e->expire_at == NO_EXPIRE) out_int(O, -1);
    else {
        long long left = e->expire_at - now_ms();
        if (left < 0) left = 0;
        out_int(O, ms ? left : (left + 500) / 1000); /* Redis rounds TTL to the nearest second */
    }
}

static void cmd_ttl(client *c, int argc, arg *argv) { (void)argc; generic_ttl(c, &argv[1], 0); }
static void cmd_pttl(client *c, int argc, arg *argv) { (void)argc; generic_ttl(c, &argv[1], 1); }

static void cmd_persist(client *c, int argc, arg *argv) {
    (void)argc;
    entry *e = lookup(&argv[1]);
    if (!e || e->expire_at == NO_EXPIRE) {
        out_int(O, 0);
        return;
    }
    dict_set_expire(&S.db, e, NO_EXPIRE);
    arg p[2] = {{"PERSIST", 7}, argv[1]};
    propagate(2, p);
    out_int(O, 1);
}

static void cmd_type(client *c, int argc, arg *argv) {
    (void)argc;
    out_simple(O, lookup(&argv[1]) ? "string" : "none");
}

typedef struct {
    outbuf *o;
    const arg *pat;
    long long n;
    outbuf tmp;
} keys_ctx;

static void keys_visit(entry *e, void *arg_) {
    keys_ctx *k = arg_;
    if (e->expire_at != NO_EXPIRE && e->expire_at <= now_ms()) return;
    if (glob_match(k->pat->p, k->pat->len, e->key, e->klen)) {
        out_bulk(&k->tmp, e->key, e->klen);
        k->n++;
    }
}

static void cmd_keys(client *c, int argc, arg *argv) {
    (void)argc;
    keys_ctx k = {O, &argv[1], 0, {0}};
    dict_foreach(&S.db, keys_visit, &k);
    out_array(O, k.n);
    if (k.tmp.oom) O->oom = 1;
    else out_raw(O, k.tmp.buf, k.tmp.len);
    out_free(&k.tmp);
}

static void cmd_dbsize(client *c, int argc, arg *argv) {
    (void)argc;
    (void)argv;
    out_int(O, (long long)dict_size(&S.db));
}

static void cmd_flushall(client *c, int argc, arg *argv) {
    if (argc == 2 && !arg_is(&argv[1], "SYNC") && !arg_is(&argv[1], "ASYNC")) {
        out_error(O, "ERR syntax error");
        return;
    }
    dict_free(&S.db);
    dict_init(&S.db, S.db.seed);
    arg p[1] = {{"FLUSHALL", 8}};
    propagate(1, p);
    out_simple(O, "OK");
}

static void cmd_select(client *c, int argc, arg *argv) {
    (void)argc;
    if (argv[1].len == 1 && argv[1].p[0] == '0') out_simple(O, "OK");
    else out_error(O, "ERR DB index is out of range");
}

static void cmd_bgrewriteaof(client *c, int argc, arg *argv) {
    (void)argc;
    (void)argv;
    if (!S.aof_path) out_error(O, "ERR AOF is disabled (start with --appendonly <file>)");
    else if (S.rewrite_pid > 0) out_error(O, "ERR Background append only file rewriting already in progress");
    else if (aof_rewrite_start() != 0) out_errorf(O, "ERR Can't start rewrite: %s", strerror(errno));
    else out_simple(O, "Background append only file rewriting started");
}

static void cmd_info(client *c, int argc, arg *argv) {
    (void)argc;
    (void)argv;
    char b[2048];
    int n = snprintf(b, sizeof(b),
        "# Server\r\ntinykv_version:%s\r\nredis_version:7.0.0-compat\r\nmultiplexing_api:%s\r\n"
        "process_id:%d\r\ntcp_port:%d\r\nuptime_in_seconds:%lld\r\n\r\n"
        "# Clients\r\nconnected_clients:%ld\r\n\r\n"
        "# Persistence\r\naof_enabled:%d\r\naof_rewrite_in_progress:%d\r\naof_rewrites:%lld\r\n"
        "aof_current_size:%lld\r\naof_last_write_status:%s\r\n\r\n"
        "# Stats\r\ntotal_connections_received:%lld\r\ntotal_commands_processed:%lld\r\n"
        "expired_keys:%lld\r\nrejected_connections:%lld\r\n\r\n"
        "# Keyspace\r\ndb0:keys=%zu,expires=%zu\r\n",
        TINYKV_VERSION, ev_backend(), (int)getpid(), S.port, (now_ms() - S.start_ms) / 1000,
        S.connected, S.aof_path != NULL, S.rewrite_pid > 0, S.rewrites_done, S.aof_size,
        S.aof_failed ? "err" : "ok", S.stat_conns, S.stat_cmds, S.stat_expired, S.stat_rejected,
        dict_size(&S.db), S.db.ttl_len);
    out_bulk(O, b, (size_t)n);
}

/* SHUTDOWN [NOSAVE|SAVE]: stop after this loop iteration; the AOF is flushed and fsynced
 * on the way out. Like Redis, no reply is sent: the connection just closes. */
static void cmd_shutdown(client *c, int argc, arg *argv) {
    if (argc == 2 && !arg_is(&argv[1], "NOSAVE") && !arg_is(&argv[1], "SAVE")) {
        out_error(&c->out, "ERR syntax error");
        return;
    }
    S.shutdown = 1;
    c->close_after_reply = 1;
}

/* Clients probe these on connect (redis-cli, redis-benchmark); answer harmlessly. */
static void cmd_empty_array(client *c, int argc, arg *argv) {
    (void)argc;
    (void)argv;
    out_array(O, 0);
}

static void cmd_client(client *c, int argc, arg *argv) {
    (void)argc;
    if (arg_is(&argv[1], "SETNAME") || arg_is(&argv[1], "SETINFO")) out_simple(O, "OK");
    else out_errorf(O, "ERR unknown subcommand '%.*s'", (int)argv[1].len, argv[1].p);
}

static const command commands[] = {
    {"ping", -1, 0, cmd_ping},        {"echo", 2, 0, cmd_echo},          {"quit", 1, 0, cmd_quit},
    {"get", 2, 0, cmd_get},           {"set", -3, 1, cmd_set},           {"setnx", 3, 1, cmd_setnx},
    {"getdel", 2, 1, cmd_getdel},     {"mget", -2, 0, cmd_mget},         {"mset", -3, 1, cmd_mset},
    {"del", -2, 1, cmd_del},          {"unlink", -2, 1, cmd_del},        {"exists", -2, 0, cmd_exists},
    {"incr", 2, 1, cmd_incr},         {"decr", 2, 1, cmd_decr},          {"incrby", 3, 1, cmd_incrby},
    {"decrby", 3, 1, cmd_decrby},     {"append", 3, 1, cmd_append},      {"strlen", 2, 0, cmd_strlen},
    {"expire", -3, 1, cmd_expire},    {"pexpire", -3, 1, cmd_pexpire},   {"expireat", -3, 1, cmd_expireat},
    {"pexpireat", -3, 1, cmd_pexpireat}, {"ttl", 2, 0, cmd_ttl},         {"pttl", 2, 0, cmd_pttl},
    {"persist", 2, 1, cmd_persist},   {"type", 2, 0, cmd_type},          {"keys", 2, 0, cmd_keys},
    {"dbsize", 1, 0, cmd_dbsize},     {"flushall", -1, 1, cmd_flushall}, {"flushdb", -1, 1, cmd_flushall},
    {"select", 2, 0, cmd_select},     {"info", -1, 0, cmd_info},         {"bgrewriteaof", 1, 0, cmd_bgrewriteaof},
    {"shutdown", -1, 0, cmd_shutdown},
    {"command", -1, 0, cmd_empty_array}, {"config", -2, 0, cmd_empty_array}, {"client", -2, 0, cmd_client},
};

static const command *find_command(const arg *name) {
    for (size_t i = 0; i < sizeof(commands) / sizeof(*commands); i++)
        if (arg_is(name, commands[i].name)) return &commands[i];
    return NULL;
}

static void execute(client *c, int argc, arg *argv) {
    const command *cmd = find_command(&argv[0]);
    S.stat_cmds++;
    if (!cmd) {
        /* Same text as Redis: the name, then each argument quoted, within 128 bytes. */
        char b[400];
        int n = snprintf(b, sizeof(b), "ERR unknown command '%.*s', with args beginning with: ",
                         (int)(argv[0].len > 128 ? 128 : argv[0].len), argv[0].p);
        size_t room = 128;
        for (int i = 1; i < argc && room > 0 && n > 0 && (size_t)n < sizeof(b) - 4; i++) {
            size_t len = argv[i].len < room ? argv[i].len : room;
            room -= len;
            n += snprintf(b + n, sizeof(b) - (size_t)n, "'%.*s' ", (int)len, argv[i].p);
        }
        out_error(O, b);
        return;
    }
    if ((cmd->arity > 0 && argc != cmd->arity) || (cmd->arity < 0 && argc < -cmd->arity)) {
        out_errorf(O, "ERR wrong number of arguments for '%s' command", cmd->name);
        return;
    }
    if (cmd->write && S.aof_failed && !S.loading) {
        out_error(O, "MISCONF Errors writing to the AOF file; writes are refused until it is fixed");
        return;
    }
    cmd->fn(c, argc, argv);
}

#undef O

/* ---------------------------------------------------------------- clients */

static void client_free(client *c) {
    if (c->mask) ev_set(S.ep, c->fd, c->mask, 0);
    close(c->fd);
    S.byfd[c->fd] = NULL;
    if (c->pending) { /* unlink from the pending-write list */
        for (client **p = &S.pending; *p; p = &(*p)->next_pending)
            if (*p == c) {
                *p = c->next_pending;
                break;
            }
    }
    free(c->qbuf);
    out_free(&c->out);
    argvec_free(&c->argv);
    free(c);
    S.connected--;
}

static void set_mask(client *c, int mask) {
    if (mask != c->mask && ev_set(S.ep, c->fd, c->mask, mask) == 0) c->mask = mask;
}

static void queue_reply(client *c) {
    if (!c->pending && c->out.len > c->out.sent) {
        c->pending = 1;
        c->next_pending = S.pending;
        S.pending = c;
    }
}

static void process_input(client *c) {
    size_t pos = 0;
    while (pos < c->qlen && !c->close_after_reply) {
        if (c->out.len - c->out.sent > (size_t)OUT_SOFT_LIMIT) {
            c->paused = 1;
            break;
        }
        size_t used;
        const char *err;
        int r = resp_parse(c->qbuf + pos, c->qlen - pos, &used, &c->argv, &err);
        if (r == RESP_INCOMPLETE) break;
        if (r == RESP_ERROR) {
            out_errorf(&c->out, "ERR Protocol error: %s", err);
            c->close_after_reply = 1;
            break;
        }
        pos += used;
        if (c->argv.n) execute(c, (int)c->argv.n, c->argv.v);
    }
    if (pos) {
        memmove(c->qbuf, c->qbuf + pos, c->qlen - pos);
        c->qlen -= pos;
    }
}

static void on_readable(client *c) {
    if (c->paused) return; /* a stale event from this batch; reading resumes after drain */
    if (c->qcap - c->qlen < READ_CHUNK) {
        size_t cap = c->qcap ? c->qcap * 2 : READ_CHUNK * 2;
        while (cap - c->qlen < READ_CHUNK) cap *= 2;
        if (cap > MAX_QUERY_BUF) {
            logf_("closing client fd=%d: query buffer over %ld bytes", c->fd, MAX_QUERY_BUF);
            client_free(c);
            return;
        }
        char *b = realloc(c->qbuf, cap);
        if (!b) {
            client_free(c);
            return;
        }
        c->qbuf = b;
        c->qcap = cap;
    }
    ssize_t n = read(c->fd, c->qbuf + c->qlen, c->qcap - c->qlen);
    if (n == 0 || (n < 0 && errno != EAGAIN && errno != EINTR)) {
        client_free(c);
        return;
    }
    if (n < 0) return;
    c->qlen += (size_t)n;
    process_input(c);
    if (c->out.oom) {
        client_free(c);
        return;
    }
    queue_reply(c);
}

/* Writes as much queued output as the socket takes. When everything is out, a paused
 * client's buffered input is executed again. Returns 0 if the client is still open. */
static int flush_client(client *c) {
    for (;;) {
        while (c->out.sent < c->out.len) {
            ssize_t n = write(c->fd, c->out.buf + c->out.sent, c->out.len - c->out.sent);
            if (n < 0) {
                if (errno == EINTR) continue;
                if (errno == EAGAIN) break;
                client_free(c);
                return -1;
            }
            c->out.sent += (size_t)n;
        }
        if (c->out.sent < c->out.len) break; /* socket full: wait for writability */
        c->out.len = c->out.sent = 0;
        if (c->out.cap > 1 << 20) out_free(&c->out); /* don't hold a big buffer forever */
        if (c->close_after_reply) {
            client_free(c);
            return -1;
        }
        if (!c->paused) {
            set_mask(c, EV_READ);
            return 0;
        }
        c->paused = 0; /* drained: run the commands that were waiting */
        process_input(c);
        if (c->out.oom) {
            client_free(c);
            return -1;
        }
    }
    if (c->out.len - c->out.sent > (size_t)MAX_PENDING_OUT) {
        logf_("closing client fd=%d: over %ld bytes of unread replies", c->fd, MAX_PENDING_OUT);
        client_free(c);
        return -1;
    }
    set_mask(c, c->paused ? EV_WRITE : EV_READ | EV_WRITE);
    return 0;
}

static void on_accept(void) {
    for (;;) {
        struct sockaddr_storage sa;
        socklen_t sl = sizeof(sa);
        int fd = accept(S.listen_fd, (struct sockaddr *)&sa, &sl);
        if (fd < 0) {
            if (errno != EAGAIN && errno != EINTR && errno != ECONNABORTED) logf_("accept: %s", strerror(errno));
            return;
        }
        S.stat_conns++;
        if (S.connected >= S.max_clients) {
            const char *msg = "-ERR max number of clients reached\r\n";
            (void)!write(fd, msg, strlen(msg));
            close(fd);
            S.stat_rejected++;
            continue;
        }
        fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);
        fcntl(fd, F_SETFD, FD_CLOEXEC);
        int one = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        if (fd >= S.byfd_cap) {
            int cap = S.byfd_cap ? S.byfd_cap : 1024;
            while (cap <= fd) cap *= 2;
            client **b = realloc(S.byfd, (size_t)cap * sizeof(*b));
            if (!b) {
                close(fd);
                continue;
            }
            memset(b + S.byfd_cap, 0, (size_t)(cap - S.byfd_cap) * sizeof(*b));
            S.byfd = b;
            S.byfd_cap = cap;
        }
        client *c = calloc(1, sizeof(*c));
        if (!c) {
            close(fd);
            continue;
        }
        c->fd = fd;
        S.byfd[fd] = c;
        S.connected++;
        set_mask(c, EV_READ);
        if (c->mask == 0) client_free(c);
    }
}

/* ---------------------------------------------------------------- startup */

static int aof_load(void) {
    int fd = open(S.aof_path, O_RDONLY);
    if (fd < 0) return errno == ENOENT ? 0 : -1;
    struct stat st;
    if (fstat(fd, &st) != 0) {
        close(fd);
        return -1;
    }
    size_t size = (size_t)st.st_size;
    char *buf = malloc(size ? size : 1);
    if (!buf) {
        close(fd);
        return -1;
    }
    size_t got = 0;
    while (got < size) {
        ssize_t n = read(fd, buf + got, size - got);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            break;
        }
        got += (size_t)n;
    }
    close(fd);
    client fake = {.fd = -1};
    size_t pos = 0;
    long long cmds = 0;
    long long t0 = now_ms();
    S.loading = 1;
    while (pos < got) {
        size_t used;
        const char *err;
        int r = resp_parse(buf + pos, got - pos, &used, &fake.argv, &err);
        if (r == RESP_ERROR || (r == RESP_OK && fake.argv.n == 0 && buf[pos] != '*')) {
            logf_("AOF %s is corrupt at byte %zu (%s); refusing to start", S.aof_path, pos, r == RESP_ERROR ? err : "inline data");
            exit(1);
        }
        if (r == RESP_INCOMPLETE) {
            /* A crash mid-write leaves a partial last command. Drop it, like Redis' default
             * aof-load-truncated yes; everything before it was acknowledged intact. */
            logf_("AOF %s ends with a truncated command (%zu bytes); truncating to %zu", S.aof_path, got - pos, pos);
            if (truncate(S.aof_path, (off_t)pos) != 0) logf_("truncate failed: %s", strerror(errno));
            break;
        }
        if (fake.argv.n) {
            execute(&fake, (int)fake.argv.n, fake.argv.v);
            cmds++;
        }
        fake.out.len = 0; /* replies to the log are discarded */
        pos += used;
    }
    S.loading = 0;
    S.stat_cmds = 0;
    argvec_free(&fake.argv);
    out_free(&fake.out);
    free(buf);
    logf_("loaded %lld commands from %s in %lld ms: %zu keys", cmds, S.aof_path, now_ms() - t0, dict_size(&S.db));
    S.aof_size = S.aof_base_size = (long long)pos;
    return 0;
}

static int listen_tcp(const char *host, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;
    sa.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &sa.sin_addr) != 1 || bind(fd, (struct sockaddr *)&sa, sizeof(sa)) != 0 || listen(fd, 511) != 0) {
        close(fd);
        return -1;
    }
    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);
    fcntl(fd, F_SETFD, FD_CLOEXEC);
    return fd;
}

static void on_signal(int sig) {
    (void)sig;
    S.shutdown = 1;
}

static void random_seed(uint8_t seed[16]) {
    int fd = open("/dev/urandom", O_RDONLY);
    if (fd < 0 || read(fd, seed, 16) != 16) {
        long long t = now_ms() ^ (long long)getpid();
        memcpy(seed, &t, 8);
        memcpy(seed + 8, &t, 8);
    }
    if (fd >= 0) close(fd);
}

static void usage(void) {
    fprintf(stderr,
        "usage: tinykv [--port 6379] [--bind 127.0.0.1] [--appendonly FILE]\n"
        "              [--appendfsync always|everysec|no] [--maxclients 10000]\n");
    exit(2);
}

static void cron(void) {
    active_expire();
    long long deadline = now_ms() + 1;
    while (dict_rehash(&S.db, 100) && now_ms() < deadline) {
    }
    dict_maybe_shrink(&S.db);
    if (S.aof_path) {
        if (S.fsync_policy == FSYNC_EVERYSEC && now_ms() - S.last_fsync_ms >= 1000) aof_flush(1);
        if (S.rewrite_pid > 0) {
            int status;
            if (waitpid(S.rewrite_pid, &status, WNOHANG) == S.rewrite_pid) aof_rewrite_done(status);
        } else if (S.aof_size > AUTO_REWRITE_MIN && S.aof_size > 2 * S.aof_base_size) {
            aof_rewrite_start();
        }
    }
}

int main(int argc, char **argv) {
    S.port = 6379;
    S.bind = "127.0.0.1"; /* never listen on every interface by default */
    S.fsync_policy = FSYNC_EVERYSEC;
    S.max_clients = 10000;
    S.aof_fd = -1;
    S.rewrite_pid = -1;
    for (int i = 1; i < argc; i++) {
        const char *a = argv[i], *v = i + 1 < argc ? argv[i + 1] : NULL;
        if (!strcmp(a, "--port") && v) S.port = atoi(argv[++i]);
        else if (!strcmp(a, "--bind") && v) S.bind = argv[++i];
        else if (!strcmp(a, "--appendonly") && v) S.aof_path = argv[++i];
        else if (!strcmp(a, "--maxclients") && v) S.max_clients = atol(argv[++i]);
        else if (!strcmp(a, "--appendfsync") && v) {
            i++;
            if (!strcmp(v, "always")) S.fsync_policy = FSYNC_ALWAYS;
            else if (!strcmp(v, "everysec")) S.fsync_policy = FSYNC_EVERYSEC;
            else if (!strcmp(v, "no")) S.fsync_policy = FSYNC_NO;
            else usage();
        } else if (!strcmp(a, "--version")) {
            printf("tinykv %s (%s)\n", TINYKV_VERSION, ev_backend());
            return 0;
        } else usage();
    }
    if (S.port <= 0 || S.port > 65535 || S.max_clients <= 0) usage();

    signal(SIGPIPE, SIG_IGN);
    struct sigaction sa = {0};
    sa.sa_handler = on_signal;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    uint8_t seed[16];
    random_seed(seed);
    dict_init(&S.db, seed);
    S.start_ms = now_ms();

    if (S.aof_path) {
        if (aof_load() != 0) {
            logf_("can't read %s: %s", S.aof_path, strerror(errno));
            return 1;
        }
        S.aof_fd = open(S.aof_path, O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC, 0644);
        if (S.aof_fd < 0) {
            logf_("can't open %s: %s", S.aof_path, strerror(errno));
            return 1;
        }
        S.last_fsync_ms = now_ms();
    }

    S.listen_fd = listen_tcp(S.bind, S.port);
    if (S.listen_fd < 0) {
        logf_("can't listen on %s:%d: %s", S.bind, S.port, strerror(errno));
        return 1;
    }
    S.ep = ev_create();
    if (S.ep < 0 || ev_set(S.ep, S.listen_fd, 0, EV_READ) != 0) {
        logf_("event loop init failed: %s", strerror(errno));
        return 1;
    }
    logf_("tinykv %s listening on %s:%d (%s, aof %s)", TINYKV_VERSION, S.bind, S.port, ev_backend(), S.aof_path ? S.aof_path : "off");

    ev_fired fired[256];
    long long next_cron = now_ms() + CRON_MS;
    while (!S.shutdown) {
        /* before sleeping: persist first, then answer (see top of file) */
        aof_flush(0);
        for (client *c = S.pending, *next; c; c = next) {
            next = c->next_pending;
            c->pending = 0;
            c->next_pending = NULL;
            flush_client(c);
        }
        S.pending = NULL;

        long long wait = next_cron - now_ms();
        int n = ev_wait(S.ep, fired, 256, wait < 0 ? 0 : (int)wait);
        if (n < 0) {
            logf_("event wait: %s", strerror(errno));
            break;
        }
        for (int i = 0; i < n; i++) {
            int fd = fired[i].fd;
            if (fd == S.listen_fd) {
                on_accept();
                continue;
            }
            client *c = fd < S.byfd_cap ? S.byfd[fd] : NULL;
            if (!c) continue; /* closed earlier in this batch */
            if ((fired[i].mask & EV_WRITE) && flush_client(c) != 0) continue;
            if (fired[i].mask & EV_READ) on_readable(c);
        }
        if (now_ms() >= next_cron) {
            cron();
            next_cron = now_ms() + CRON_MS;
        }
    }

    logf_("shutting down");
    aof_flush(1);
    if (S.rewrite_pid > 0) {
        kill(S.rewrite_pid, SIGKILL);
        waitpid(S.rewrite_pid, NULL, 0);
        unlink(S.rewrite_tmp);
    }
    for (int fd = 0; fd < S.byfd_cap; fd++)
        if (S.byfd[fd]) client_free(S.byfd[fd]);
    free(S.byfd);
    dict_free(&S.db);
    out_free(&S.aofbuf);
    out_free(&S.rewrite_buf);
    if (S.aof_fd >= 0) close(S.aof_fd);
    close(S.listen_fd);
    return 0;
}
