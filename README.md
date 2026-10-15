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
| SET | 1 | 238,777 | 251,383 | 0.95× |
| GET | 1 | 243,191 | 249,377 | 0.98× |
| INCR | 1 | 245,459 | 250,376 | 0.98× |
| SET | 16 | 2,673,797 | 1,779,360 | 1.50× |
| GET | 16 | 2,840,909 | 2,222,222 | 1.28× |
| INCR | 16 | 2,358,490 | 2,293,578 | 1.03× |

Without pipelining, both servers are bound by the per-request system calls and the benchmark client, and they land within 5% of each other. With pipelining, tinykv pulls ahead because it does much less per command. Redis tracks memory, enforces ACLs, fires keyspace notifications, supports client tracking, and has many more data types behind every lookup. **This shows the core is lean, not that it's a better Redis.** On the same machine the client competes with the server for CPU, so treat the absolute numbers as relative.

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
- Output has backpressure. Once a client has more than 1 MB of unsent replies, tinykv stops executing its pipeline and stops reading its socket until the replies drain, then resumes exactly where it stopped. A client that pipelines 200 GETs of a 1 MB value and doesn't read can't make the loop build 200 MB of replies in one pass. That would stall everyone else (it took 1.2 s on a CI runner before this was added) and grow memory without bound. A single reply larger than 1 GB closes the connection.
- The server binds to 127.0.0.1 unless told otherwise.

## Commands

`GET SET` (with `NX XX GET EX PX EXAT PXAT KEEPTTL`), `SETNX GETDEL MGET MSET DEL UNLINK EXISTS INCR DECR INCRBY DECRBY APPEND STRLEN`, `EXPIRE PEXPIRE EXPIREAT PEXPIREAT` (with `NX XX GT LT`), `TTL PTTL PERSIST TYPE KEYS` (glob, linear-time matcher), `DBSIZE FLUSHALL FLUSHDB SELECT 0 PING ECHO INFO BGREWRITEAOF SHUTDOWN QUIT`, and inline commands (`telnet`-style).

## Tests

```bash
make test        # unit tests + integration suite, against an ASan + UBSan build
make bench
```

- **Differential tests against real Redis.** About 100 scripted commands (error paths, integer overflow at both ends, option conflicts, binary keys and values with `\0` and `\r\n`, inline protocol), then **5,000 randomized operations** over 25 colliding keys. Every reply must match Redis byte for byte, error messages included. Ubuntu CI compares against the distribution's redis-server, and macOS CI against the current Homebrew release.
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
  - A slow reader: other clients stay under 100 ms, and the reader still gets all 200 replies once it reads.
  - 200 concurrent clients.
  - Active expiry clearing 5,000 keys nobody reads.
- **Unit tests** (C, 401k checks):
  - The parser is checked at every possible split point of a request, on pipelines, and on 9 classes of malformed input.
  - The hash table stays correct across 100k inserts with lookups mid-rehash, plus shrinking, binary keys and TTL-index consistency.
- **Fuzzing.** CI runs a libFuzzer target over the RESP parser for 60 s per push under ASan + UBSan. It asserts the parser always makes progress and that every argument lies inside its frame.

## Bugs CI caught before release

- **Undefined behavior on Linux only.** `KEYS` with no matches appended an empty, never-allocated buffer with `memcpy(dst, NULL, 0)`. macOS tolerated it; glibc declares that argument non-null, and UBSan on the Linux runner aborted. Fixed, with a regression test.
- **A latency stall under a slow reader.** It showed up as a flaky timing test on the macOS runner and led to the backpressure design above. The test now also requires the paused client to receive all 200 replies intact and in order.
- **A wording difference from Redis.** `DECRBY key -9223372036854775808` overflows differently from other overflows; the differential suite caught it.

## Limits

- **Strings only.** No lists, hashes or sets; no transactions, pub/sub or replication. There's one database (`SELECT 1` is an error, where Redis has 16).
- **Inline commands** split on whitespace and don't support quoting.
- **`appendfsync everysec`** fsyncs on the loop thread; Redis does it on a background thread.
- **`KEYS` is O(n)**, as in Redis; there's no `SCAN`.
- **Little-endian only** (SipHash's loads), which covers x86-64 and arm64.

MIT License.
