/* Chained hash table with incremental rehashing (the scheme Redis uses).
 *
 * Growing a table by rehashing every entry at once stalls the server for as long as that
 * takes: tens of milliseconds at a few million keys. Instead, a resize allocates the new
 * table as ht[1] and every later operation moves one bucket across, plus a time-boxed
 * batch in the cron. Lookups check both tables until ht[0] is empty. */
#include "dict.h"

#include <stdlib.h>
#include <string.h>

#include "siphash.h"

#define INITIAL_SIZE 16

static uint64_t rng_state = 0x9E3779B97F4A7C15ULL;

static uint64_t rng(void) { /* xorshift64*; only used for sampling, not security */
    rng_state ^= rng_state >> 12;
    rng_state ^= rng_state << 25;
    rng_state ^= rng_state >> 27;
    return rng_state * 2685821657736338717ULL;
}

void dict_init(dict *d, const uint8_t seed[16]) {
    memset(d, 0, sizeof(*d));
    d->rehashidx = -1;
    memcpy(d->seed, seed, 16);
    memcpy(&rng_state, seed, 8);
    rng_state |= 1;
}

static void free_table(table *t) {
    for (size_t i = 0; i < t->size; i++) {
        entry *e = t->buckets[i];
        while (e) {
            entry *next = e->next;
            free(e->val);
            free(e);
            e = next;
        }
    }
    free(t->buckets);
    memset(t, 0, sizeof(*t));
}

void dict_free(dict *d) {
    free_table(&d->ht[0]);
    free_table(&d->ht[1]);
    free(d->ttl);
    d->ttl = NULL;
    d->ttl_len = d->ttl_cap = 0;
    d->rehashidx = -1;
}

size_t dict_size(const dict *d) { return d->ht[0].used + d->ht[1].used; }

static int rehashing(const dict *d) { return d->rehashidx != -1; }

static int start_resize(dict *d, size_t size) {
    entry **b = calloc(size, sizeof(*b));
    if (!b) return -1;
    if (d->ht[0].size == 0) { /* first allocation: nothing to move */
        d->ht[0].buckets = b;
        d->ht[0].size = size;
        return 0;
    }
    d->ht[1].buckets = b;
    d->ht[1].size = size;
    d->ht[1].used = 0;
    d->rehashidx = 0;
    return 0;
}

int dict_rehash(dict *d, int n) {
    if (!rehashing(d)) return 0;
    int empty_visits = n * 10; /* bound the work when crossing long runs of empty buckets */
    while (n-- && d->ht[0].used) {
        while (d->ht[0].buckets[d->rehashidx] == NULL) {
            d->rehashidx++;
            if (--empty_visits == 0) return 1;
        }
        entry *e = d->ht[0].buckets[d->rehashidx];
        while (e) {
            entry *next = e->next;
            size_t i = e->hash & (d->ht[1].size - 1);
            e->next = d->ht[1].buckets[i];
            d->ht[1].buckets[i] = e;
            d->ht[0].used--;
            d->ht[1].used++;
            e = next;
        }
        d->ht[0].buckets[d->rehashidx++] = NULL;
    }
    if (d->ht[0].used == 0) {
        free(d->ht[0].buckets);
        d->ht[0] = d->ht[1];
        memset(&d->ht[1], 0, sizeof(d->ht[1]));
        d->rehashidx = -1;
        return 0;
    }
    return 1;
}

static void maybe_grow(dict *d) {
    if (rehashing(d)) return;
    if (d->ht[0].size == 0) start_resize(d, INITIAL_SIZE);
    else if (d->ht[0].used >= d->ht[0].size) start_resize(d, d->ht[0].size * 2);
}

void dict_maybe_shrink(dict *d) {
    if (rehashing(d) || d->ht[0].size <= INITIAL_SIZE) return;
    if (d->ht[0].used * 8 >= d->ht[0].size) return; /* below 12.5% full */
    size_t size = INITIAL_SIZE;
    while (size < d->ht[0].used * 2) size <<= 1;
    start_resize(d, size);
}

static entry **find_slot(dict *d, const char *key, size_t klen, uint64_t h) {
    for (int t = 0; t <= 1; t++) {
        table *tb = &d->ht[t];
        if (tb->size == 0) continue;
        entry **p = &tb->buckets[h & (tb->size - 1)];
        for (; *p; p = &(*p)->next)
            if ((*p)->hash == h && (*p)->klen == klen && memcmp((*p)->key, key, klen) == 0) return p;
        if (!rehashing(d)) break;
    }
    return NULL;
}

entry *dict_find(dict *d, const char *key, size_t klen) {
    if (dict_size(d) == 0) return NULL;
    dict_rehash(d, 1);
    entry **p = find_slot(d, key, klen, siphash13(key, klen, d->seed));
    return p ? *p : NULL;
}

static void ttl_remove(dict *d, entry *e) {
    if (e->ttl_idx < 0) return;
    entry *last = d->ttl[--d->ttl_len];
    d->ttl[e->ttl_idx] = last; /* swap-remove keeps the array dense for O(1) sampling */
    last->ttl_idx = e->ttl_idx;
    e->ttl_idx = -1;
}

static int ttl_add(dict *d, entry *e) {
    if (d->ttl_len == d->ttl_cap) {
        size_t cap = d->ttl_cap ? d->ttl_cap * 2 : 64;
        entry **n = realloc(d->ttl, cap * sizeof(*n));
        if (!n) return -1;
        d->ttl = n;
        d->ttl_cap = cap;
    }
    e->ttl_idx = (ssize_t)d->ttl_len;
    d->ttl[d->ttl_len++] = e;
    return 0;
}

void dict_set_expire(dict *d, entry *e, int64_t expire_at) {
    if (expire_at == NO_EXPIRE) {
        ttl_remove(d, e);
    } else if (e->ttl_idx < 0 && ttl_add(d, e) != 0) {
        return; /* OOM: the key simply keeps no TTL */
    }
    e->expire_at = expire_at;
}

int dict_set_value(entry *e, const char *val, size_t vlen) {
    char *v = malloc(vlen ? vlen : 1);
    if (!v) return -1;
    memcpy(v, val, vlen);
    free(e->val);
    e->val = v;
    e->vlen = vlen;
    return 0;
}

entry *dict_set(dict *d, const char *key, size_t klen, const char *val, size_t vlen, int *created) {
    dict_rehash(d, 1);
    uint64_t h = siphash13(key, klen, d->seed);
    entry **p = d->ht[0].size ? find_slot(d, key, klen, h) : NULL;
    if (p) {
        if (dict_set_value(*p, val, vlen) != 0) return NULL;
        dict_set_expire(d, *p, NO_EXPIRE); /* SET discards a previous TTL, as in Redis */
        if (created) *created = 0;
        return *p;
    }
    maybe_grow(d);
    entry *e = malloc(sizeof(*e) + klen);
    if (!e) return NULL;
    e->val = NULL;
    if (dict_set_value(e, val, vlen) != 0) {
        free(e);
        return NULL;
    }
    memcpy(e->key, key, klen);
    e->klen = klen;
    e->hash = h;
    e->expire_at = NO_EXPIRE;
    e->ttl_idx = -1;
    table *t = rehashing(d) ? &d->ht[1] : &d->ht[0]; /* new keys go to the target table */
    size_t i = h & (t->size - 1);
    e->next = t->buckets[i];
    t->buckets[i] = e;
    t->used++;
    if (created) *created = 1;
    return e;
}

int dict_delete(dict *d, const char *key, size_t klen) {
    if (dict_size(d) == 0) return 0;
    dict_rehash(d, 1);
    uint64_t h = siphash13(key, klen, d->seed);
    for (int t = 0; t <= 1; t++) {
        table *tb = &d->ht[t];
        if (tb->size == 0) continue;
        for (entry **p = &tb->buckets[h & (tb->size - 1)]; *p; p = &(*p)->next) {
            entry *e = *p;
            if (e->hash == h && e->klen == klen && memcmp(e->key, key, klen) == 0) {
                *p = e->next;
                tb->used--;
                ttl_remove(d, e);
                free(e->val);
                free(e);
                return 1;
            }
        }
        if (!rehashing(d)) break;
    }
    return 0;
}

entry *dict_random_ttl(dict *d) {
    return d->ttl_len ? d->ttl[rng() % d->ttl_len] : NULL;
}

void dict_foreach(dict *d, void (*fn)(entry *, void *), void *arg) {
    for (int t = 0; t <= 1; t++)
        for (size_t i = 0; i < d->ht[t].size; i++)
            for (entry *e = d->ht[t].buckets[i]; e; e = e->next) fn(e, arg);
}
