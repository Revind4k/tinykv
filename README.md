# tinykv

A Redis-compatible key-value server in about 2,000 lines of C, with no dependencies. It speaks RESP2, so `redis-cli`, `redis-benchmark` and any Redis client library work against it unchanged. It persists to an append-only file that survives `kill -9`, compacts that file in a forked child while still serving, and is tested by sending the same commands to tinykv and to real Redis and requiring identical bytes back.

```
$ make && ./tinykv --appendonly data.aof
tinykv 0.1.0 listening on 127.0.0.1:6379 (kqueue, aof data.aof)

$ redis-cli set visits 41 EX 3600 && redis-cli incr visits && redis-cli ttl visits
OK
(integer) 42
(integer) 3600
```

## Performance

`bench/bench.sh` runs `redis-benchmark` against tinykv and against redis-server 8.10 on the same machine (Apple M4, loopback, persistence off on both, 50 clients, 100k random keys, 64-byte values), taking the median of 3 runs:

| Command | Pipeline | tinykv req/s | Redis req/s | tinykv / Redis |
|---|---|---|---|---|
| SET | 1 | 240,732 | 251,130 | 0.96× |
| GET | 1 | 241,663 | 248,880 | 0.97× |
| INCR | 1 | 245,942 | 250,501 | 0.98× |
| SET | 16 | 2,717,391 | 1,766,784 | 1.54× |
| GET | 16 | 2,824,859 | 2,212,390 | 1.28× |
| INCR | 16 | 2,403,846 | 2,304,148 | 1.04× |

Without pipelining, both servers are bound by the per-request system calls and the benchmark client, and they land within 4% of each other. With pipelining, tinykv pulls ahead because it does much less per command. Redis tracks memory, enforces ACLs, fires keyspace notifications, supports client tracking, and has many more data types behind every lookup. **This shows the core is lean, not that it's a better Redis.** On the same machine the client competes with the server for CPU, so treat the absolute numbers as relative.

## Design

**One thread, one event loop.** All state lives on the loop thread, so commands need no locks and run atomically, as in Redis. `event.c` wraps kqueue (macOS/BSD) and epoll (Linux) behind one level-triggered interface; CI runs on both.

**Persist, then reply.** Replies aren't written while a command runs; they're queued. Before the loop sleeps, it flushes the append-only file first and only then writes the queued replies. With `--appendfsync always`, a client never sees `OK` for a write that isn't on disk. `everysec` (the default) fsyncs once a second, and `no` leaves it to the OS.

**Hash table** (`dict.c`):
- **SipHash-1-3 with a random per-process key.** Clients can't choose keys that all land in one bucket, the hash-flooding attack an unkeyed hash like FNV allows.
- **Incremental rehashing.** Growing the table never stops the world. A resize allocates the new table, every later operation moves one bucket across, and the cron moves batches within a 1 ms budget. Lookups check both tables until the move finishes.
- **Shrinking.** The table shrinks when it's under 12.5% full.

**Expiry, lazy and active.** A key past its TTL is deleted when it's next touched. A 100 ms cron also samples 20 keys with TTLs at random and repeats while more than a quarter of a sample was expired, within a 1 ms budget. That reclaims keys nobody reads again. Sampling is O(1) because keys with TTLs are kept in a dense array, where each entry stores its own index and is swap-removed when deleted.

**Append-only file:**
- **Exact replay.** Writes are logged as RESP. Relative times are logged as absolute (`SET … EX 100` becomes `SET … PXAT <ms>`, `EXPIRE` becomes `PEXPIREAT`), so replaying later gives the same state.
- **No expiry during replay.** Nothing expires while the log replays. A key that expires before `INCR` runs could otherwise come back as a TTL-less `1`, and a test covers that case.
- **Truncated tail.** A crash mid-write leaves a partial last command. It's truncated away at startup, with a warning, and the server starts. Corruption anywhere else refuses to start, with the byte offset.

**Compaction** (`BGREWRITEAOF`, and automatically past 64 MB once the file has doubled):
- The server `fork()`s. The child writes one `SET` per live key from its copy-on-write view of memory, while the parent keeps serving.
- Writes that arrive meanwhile go to a second buffer.
- When the child exits, the buffer is appended to the new file, fsynced, and `rename()`d over the old file atomically. A failed rewrite leaves the old file untouched.

**Defensive limits:**
- 1 GB query buffer, 512 MB bulk string, 1M arguments, and 64 KB inline commands.
- Malformed frames get `-ERR Protocol error` and the connection is closed.
- A client that stops reading is cut off at 256 MB of unread replies, and other clients are never blocked by it. The test suite checks this with a client that requests 200 MB and never reads.
- The server binds to 127.0.0.1 unless told otherwise.

## Commands

`GET SET` (with `NX XX GET EX PX EXAT PXAT KEEPTTL`), `SETNX GETDEL MGET MSET DEL UNLINK EXISTS INCR DECR INCRBY DECRBY APPEND STRLEN`, `EXPIRE PEXPIRE EXPIREAT PEXPIREAT` (with `NX XX GT LT`), `TTL PTTL PERSIST TYPE KEYS` (glob, linear-time matcher), `DBSIZE FLUSHALL FLUSHDB SELECT 0 PING ECHO INFO BGREWRITEAOF SHUTDOWN QUIT`, and inline commands (`telnet`-style).

## Tests

```bash
make test        # unit tests + integration suite, against an ASan + UBSan build
make bench
```

- **Differential tests against real Redis.** About 100 scripted commands (error paths, integer overflow at both ends, option conflicts, binary keys and values with `\0` and `\r\n`, inline protocol), then **5,000 randomized operations** over 25 colliding keys. Every reply must match Redis byte for byte, error messages included. This caught one difference before release: Redis words the `DECRBY key -9223372036854775808` overflow as `ERR decrement would overflow`.
- **Persistence:**
  - Kill -9 with `appendfsync always`, then restart.
  - A TTL survives `INCR` across restart.
  - Truncated-tail recovery.
  - A corrupt file refuses to start.
  - `BGREWRITEAOF` over 50,000 writes shrinks the file more than 10×, keeps 500 writes that raced the child, and keeps TTLs.
- **Behavior:**
  - 10,000 pipelined commands in one `write()`.
  - A 20 MB value.
  - A request dribbled one byte at a time.
  - A protocol error.
  - A slow reader.
  - 200 concurrent clients.
  - Active expiry clearing 5,000 keys nobody reads.
- **Unit tests** (C, 401k checks):
  - The parser is checked at every possible split point of a request, on pipelines, and on 9 classes of malformed input.
  - The hash table stays correct across 100k inserts with lookups mid-rehash, plus shrinking, binary keys and TTL-index consistency.
- **Fuzzing.** CI runs a libFuzzer target over the RESP parser for 60 s per push under ASan + UBSan. It asserts the parser always makes progress and that every argument lies inside its frame.

## Limits

- **Strings only.** No lists, hashes or sets; no transactions, pub/sub or replication. There's one database (`SELECT 1` is an error, where Redis has 16).
- **Inline commands** split on whitespace and don't support quoting.
- **`appendfsync everysec`** fsyncs on the loop thread; Redis does it on a background thread.
- **`KEYS` is O(n)**, as in Redis; there's no `SCAN`.
- **Little-endian only** (SipHash's loads), which covers x86-64 and arm64.

MIT License.
