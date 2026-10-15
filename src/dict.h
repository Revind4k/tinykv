/* Binary-safe string -> string hash table with incremental rehashing and O(1) random
 * sampling of keys that carry a TTL (for active expiry). */
#ifndef TINYKV_DICT_H
#define TINYKV_DICT_H

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#define NO_EXPIRE INT64_MIN

typedef struct entry {
    struct entry *next;   /* bucket chain */
    uint64_t hash;
    int64_t expire_at;    /* unix ms, or NO_EXPIRE */
    ssize_t ttl_idx;      /* position in dict.ttl, or -1 */
    size_t klen, vlen;
    char *val;
    char key[];           /* klen bytes, not NUL-terminated */
} entry;

typedef struct {
    entry **buckets;
    size_t size;          /* power of two, or 0 */
    size_t used;
} table;

typedef struct {
    table ht[2];          /* ht[1] is the target while a rehash is in progress */
    ssize_t rehashidx;    /* next ht[0] bucket to move, or -1 when not rehashing */
    entry **ttl;          /* every entry with a TTL, for random sampling */
    size_t ttl_len, ttl_cap;
    uint8_t seed[16];
} dict;

void dict_init(dict *d, const uint8_t seed[16]);
void dict_free(dict *d);
size_t dict_size(const dict *d);

entry *dict_find(dict *d, const char *key, size_t klen);
/* Sets key to val (copied). Returns the entry; *created says whether it is new. Clears any TTL. */
entry *dict_set(dict *d, const char *key, size_t klen, const char *val, size_t vlen, int *created);
/* Replaces an entry's value in place (copied), keeping its TTL. Returns 0, or -1 on OOM. */
int dict_set_value(entry *e, const char *val, size_t vlen);
int dict_delete(dict *d, const char *key, size_t klen);
void dict_set_expire(dict *d, entry *e, int64_t expire_at);

/* A uniformly random entry with a TTL, or NULL if none. */
entry *dict_random_ttl(dict *d);
/* Moves up to n buckets from ht[0] to ht[1]. Returns 1 if more rehashing remains. */
int dict_rehash(dict *d, int n);
/* Shrinks the table when it is mostly empty (called from the periodic cron). */
void dict_maybe_shrink(dict *d);

/* Visits every entry. The callback must not modify the dict. */
void dict_foreach(dict *d, void (*fn)(entry *, void *), void *arg);

#endif
