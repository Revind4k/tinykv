"""Integration tests for tinykv over real sockets. Standard library only.

The core check is differential: the same commands go to tinykv and to a real redis-server,
and the raw RESP replies must match byte for byte. The rest covers what Redis can't be
compared against here: crash recovery, AOF truncation and compaction, and behavior under
pipelining, huge values, slow readers and many clients.

    TINYKV=./tinykv python3 tests/test_server.py      # REDIS_SERVER=... to pick a binary
"""

import os
import random
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TINYKV = os.environ.get("TINYKV", os.path.join(ROOT, "tinykv"))
REDIS = os.environ.get("REDIS_SERVER") or shutil.which("redis-server")


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Server:
    def __init__(self, argv, port):
        self.port = port
        # stderr goes to a file, not a pipe: a sanitizer report must survive the crash it describes.
        self.logf = tempfile.TemporaryFile()
        self.proc = subprocess.Popen(argv, stdout=subprocess.DEVNULL, stderr=self.logf)
        deadline = time.time() + 10
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"server exited early: {self.log()}")
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
                return
            except OSError:
                time.sleep(0.02)
        raise RuntimeError("server did not start")

    def log(self):
        self.logf.seek(0)
        return self.logf.read().decode(errors="replace")

    def stop(self, sig=signal.SIGTERM):
        if self.proc.poll() is None:
            self.proc.send_signal(sig)
            self.proc.wait(10)
        out = self.log()
        self.logf.close()
        return out


def tinykv(*args, port=None):
    port = port or free_port()
    return Server([TINYKV, "--port", str(port), *args], port)


def encode(*args):
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        a = a if isinstance(a, bytes) else str(a).encode()
        out.append(b"$%d\r\n%s\r\n" % (len(a), a))
    return b"".join(out)


class Conn:
    """Minimal RESP client that returns each reply as its raw bytes."""

    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=10)
        self.buf = b""

    def close(self):
        self.sock.close()

    def _fill(self):
        chunk = self.sock.recv(1 << 20)
        if not chunk:
            raise ConnectionError("closed by server")
        self.buf += chunk

    def _line(self):
        while b"\r\n" not in self.buf:
            self._fill()
        i = self.buf.index(b"\r\n")
        line, self.buf = self.buf[: i + 2], self.buf[i + 2 :]
        return line

    def _exact(self, n):
        while len(self.buf) < n:
            self._fill()
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def raw_reply(self):
        line = self._line()
        t, rest = line[:1], line[1:-2]
        if t == b"$" and int(rest) >= 0:
            return line + self._exact(int(rest) + 2)
        if t == b"*" and int(rest) > 0:
            return line + b"".join(self.raw_reply() for _ in range(int(rest)))
        return line

    def raw(self, *args):
        self.sock.sendall(encode(*args))
        return self.raw_reply()

    def value(self, *args):
        return decode(self.raw(*args))


def decode(raw):
    """Raw reply -> Python value (bytes, int, None, list, or Exception for errors)."""
    def parse(b, i):
        t, j = b[i : i + 1], b.index(b"\r\n", i)
        body = b[i + 1 : j]
        i = j + 2
        if t == b"+":
            return body.decode(), i
        if t == b"-":
            return Exception(body.decode()), i
        if t == b":":
            return int(body), i
        if t == b"$":
            n = int(body)
            return (None, i) if n < 0 else (b[i : i + n], i + n + 2)
        if t == b"*":
            n, out = int(body), []
            for _ in range(max(n, 0)):
                v, i = parse(b, i)
                out.append(v)
            return out, i
        raise ValueError(raw)
    return parse(raw, 0)[0]


def sorted_array(raw):
    """KEYS order is unspecified; compare it as a set."""
    parts = decode(raw)
    return sorted(parts) if isinstance(parts, list) else raw


@unittest.skipUnless(REDIS, "redis-server not found; set REDIS_SERVER")
class Differential(unittest.TestCase):
    """tinykv must answer exactly like Redis."""

    @classmethod
    def setUpClass(cls):
        cls.tk = tinykv()
        rport = free_port()
        cls.rs = Server([REDIS, "--port", str(rport), "--save", "", "--appendonly", "no", "--bind", "127.0.0.1"], rport)
        cls.a, cls.b = Conn(cls.tk.port), Conn(rport)

    @classmethod
    def tearDownClass(cls):
        cls.a.close()
        cls.b.close()
        cls.tk.stop()
        cls.rs.stop()

    def setUp(self):
        self.a.raw("FLUSHALL")
        self.b.raw("FLUSHALL")

    def same(self, *cmd):
        try:
            ra = self.a.raw(*cmd)
        except ConnectionError:
            self.fail(f"tinykv dropped the connection on {cmd!r}; server log:\n{self.tk.log()[-4000:]}")
        rb = self.b.raw(*cmd)
        if cmd[0].upper() == "KEYS":
            ra, rb = sorted_array(ra), sorted_array(rb)
        self.assertEqual(ra, rb, f"{cmd!r}\n tinykv: {ra!r}\n redis:  {rb!r}")

    def test_scripted_commands(self):
        script = [
            ("PING",), ("PING", "hi"), ("ECHO", "x"),
            ("SET", "k", "v"), ("GET", "k"), ("GET", "missing"), ("SET", "k", "v2", "NX"), ("SET", "k", "v3", "XX"),
            ("SET", "new", "v", "XX"), ("SET", "k", "v4", "GET"), ("SET", "nope", "v", "NX", "GET"), ("GET", "k"),
            ("SETNX", "k", "x"), ("SETNX", "fresh", "x"), ("GETDEL", "fresh"), ("GETDEL", "fresh"),
            ("MSET", "a", "1", "b", "2", "c", "3"), ("MGET", "a", "b", "zz", "c"), ("EXISTS", "a", "a", "zz"),
            ("DEL", "a", "zz", "b"), ("UNLINK", "c"), ("DBSIZE",),
            ("INCR", "n"), ("INCRBY", "n", "41"), ("DECR", "n"), ("DECRBY", "n", "-8"), ("INCRBY", "n", "-100"),
            ("SET", "big", "9223372036854775806"), ("INCR", "big"), ("INCR", "big"), ("GET", "big"),
            ("SET", "neg", "-9223372036854775808"), ("DECR", "neg"), ("DECRBY", "neg", "-9223372036854775808"),
            ("SET", "s", "abc"), ("INCR", "s"), ("SET", "lead", "007"), ("INCR", "lead"), ("SET", "sp", " 1"), ("INCR", "sp"),
            ("INCRBY", "n", "1.5"), ("INCRBY", "n", "99999999999999999999"),
            ("APPEND", "s", "def"), ("APPEND", "new2", "x"), ("STRLEN", "s"), ("STRLEN", "zz"),
            ("SET", "t", "v", "EX", "100"), ("TTL", "t"), ("PERSIST", "t"), ("TTL", "t"), ("TTL", "zz"), ("PERSIST", "t"),
            ("EXPIRE", "t", "50"), ("TTL", "t"), ("EXPIRE", "t", "60", "NX"), ("EXPIRE", "t", "60", "XX"), ("TTL", "t"),
            ("EXPIRE", "t", "10", "GT"), ("EXPIRE", "t", "10", "LT"), ("TTL", "t"), ("EXPIRE", "t", "5", "NX", "XX"),
            ("EXPIRE", "zz", "10"), ("EXPIRE", "t", "-1"), ("EXISTS", "t"), ("EXPIRE", "s", "abc"),
            ("SET", "kt", "v", "EX", "100"), ("SET", "kt", "v2", "KEEPTTL"), ("TTL", "kt"), ("SET", "kt", "v3"), ("TTL", "kt"),
            ("SET", "x", "v", "EX", "0"), ("SET", "x", "v", "EX", "-5"), ("SET", "x", "v", "PX", "abc"), ("SET", "x", "v", "EX", "10", "PX", "10"),
            ("SET", "x", "v", "NX", "XX"), ("SET", "x", "v", "BOGUS"), ("SET", "x", "v", "EX", "10", "KEEPTTL"),
            ("SET", "x", "v", "EX", "10", "EX", "20"), ("TTL", "x"), ("SET", "x", "v", "PX", "abc", "PX", "5000"), ("PTTL", "x"),
            ("SET", "x", "v", "EX", "abc", "BOGUS"), ("SET", "x", "v", "EX", "abc", "NX", "XX"), ("SET", "x", "v", "PXAT", "1"), ("EXISTS", "x"),
            ("SET", "g", "v"), ("GETEX", "g"), ("TTL", "g"), ("GETEX", "g", "EX", "100"), ("TTL", "g"), ("GETEX", "g"), ("TTL", "g"),
            ("GETEX", "g", "PX", "50000"), ("TTL", "g"), ("GETEX", "g", "PERSIST"), ("TTL", "g"), ("GETEX", "g", "PERSIST", "persist"),
            ("GETEX", "g", "ex", "10", "EX", "20"), ("TTL", "g"), ("GETEX", "g", "EXAT", "99999999999"), ("PERSIST", "g"),
            ("GETEX", "g", "EX", "10", "PX", "10"), ("GETEX", "g", "PERSIST", "EX", "5"), ("GETEX", "g", "EX", "5", "PERSIST"),
            ("GETEX", "g", "EX"), ("GETEX", "g", "KEEPTTL"), ("GETEX", "g", "EX", "abc"), ("GETEX", "g", "EX", "0"),
            ("GETEX", "g", "PX", "-1"), ("GETEX", "g", "EX", "9223372036854775807"), ("GETEX", "zz", "EX", "abc"), ("GETEX", "zz", "BOGUS"),
            ("GETEX",), ("GETEX", "g", "PXAT", "1"), ("EXISTS", "g"), ("GETEX", "g"),
            ("TYPE", "s"), ("TYPE", "zz"), ("SELECT", "0"),
            ("GET",), ("SET", "only"), ("MSET", "a"), ("MSET", "a", "1", "b"), ("NOSUCHCMD", "arg1", "arg2"),
            ("MGET", "s", "kt", "nope"), ("KEYS", "*"), ("KEYS", "k*"), ("KEYS", "?t"), ("KEYS", "[ks]*"), ("KEYS", "nomatch*"),
        ]
        for cmd in script:
            self.same(*cmd)

    def test_binary_safe(self):
        blob = bytes(range(256)) * 4
        for cmd in [("SET", b"k\x00\r\n", blob), ("GET", b"k\x00\r\n"), ("STRLEN", b"k\x00\r\n"),
                    ("APPEND", b"k\x00\r\n", b"\x00\x00"), ("GET", b"k\x00\r\n"), ("SET", b"", b""), ("GET", b""), ("EXISTS", b"")]:
            self.same(*cmd)

    def test_randomized(self):
        """5,000 random operations over a small key space so they collide constantly."""
        rng = random.Random(1234)
        keys = [f"key{i}" for i in range(25)]
        vals = ["0", "1", "-7", "42", "9223372036854775807", "abc", "", "x" * 300, "3.14", "007"]
        ops = [
            lambda k: ("SET", k, rng.choice(vals)),
            lambda k: ("SET", k, rng.choice(vals), rng.choice(["NX", "XX"])),
            lambda k: ("SET", k, rng.choice(vals), "EX", str(rng.randint(50, 500))),
            lambda k: ("GET", k), lambda k: ("DEL", k, rng.choice(keys)), lambda k: ("EXISTS", k),
            lambda k: ("INCR", k), lambda k: ("DECR", k), lambda k: ("INCRBY", k, str(rng.randint(-1000, 1000))),
            lambda k: ("APPEND", k, rng.choice(vals)), lambda k: ("STRLEN", k), lambda k: ("GETDEL", k),
            lambda k: ("SETNX", k, rng.choice(vals)), lambda k: ("MGET", k, rng.choice(keys), rng.choice(keys)),
            lambda k: ("MSET", k, rng.choice(vals), rng.choice(keys), rng.choice(vals)),
            lambda k: ("EXPIRE", k, str(rng.randint(100, 1000))), lambda k: ("PERSIST", k), lambda k: ("TTL", k),
            lambda k: ("GETEX", k, *rng.choice([(), ("EX", str(rng.randint(100, 1000))), ("PERSIST",)])),
            lambda k: ("TYPE", k), lambda k: ("DBSIZE",), lambda k: ("KEYS", "key1*"),
        ]
        for _ in range(5000):
            self.same(*rng.choice(ops)(rng.choice(keys)))

    def test_inline_protocol(self):
        for c in (self.a, self.b):
            c.sock.sendall(b"SET inl hello\r\nGET inl\r\n")
        for _ in range(2):
            self.assertEqual(self.a.raw_reply(), self.b.raw_reply())


class Behavior(unittest.TestCase):
    def setUp(self):
        self.srv = tinykv()
        self.c = Conn(self.srv.port)

    def tearDown(self):
        self.c.close()
        self.srv.stop()

    def test_pipeline_of_10k_commands_in_one_write(self):
        self.c.sock.sendall(b"".join(encode("INCR", "ctr") for _ in range(10_000)))
        replies = [self.c.raw_reply() for _ in range(10_000)]
        self.assertEqual(replies[-1], b":10000\r\n")
        self.assertEqual(self.c.value("GET", "ctr"), b"10000")

    def test_20mb_value_round_trip(self):
        blob = os.urandom(20 << 20)
        self.assertEqual(self.c.value("SET", "big", blob), "OK")
        self.assertEqual(self.c.value("GET", "big"), blob)

    def test_bytes_dribbled_one_at_a_time(self):
        for byte in encode("SET", "slow", "value"):
            self.c.sock.send(bytes([byte]))
            time.sleep(0.0005)
        self.assertEqual(self.c.raw_reply(), b"+OK\r\n")

    def test_protocol_error_replies_then_closes(self):
        self.c.sock.sendall(b"*1\r\n$abc\r\n")
        self.assertTrue(self.c.raw_reply().startswith(b"-ERR Protocol error"))
        with self.assertRaises((ConnectionError, OSError)):
            self.c.value("PING")

    def test_slow_reader_does_not_block_others(self):
        value = b"x" * (1 << 20)
        self.c.value("SET", "v", value)
        hog = Conn(self.srv.port)
        hog.sock.sendall(encode("GET", "v") * 200)  # ~200 MB of replies, not read yet
        time.sleep(0.3)
        for _ in range(5):  # other clients stay fast while the hog's replies back up
            start = time.time()
            self.assertEqual(self.c.value("PING"), "PONG")
            self.assertLess(time.time() - start, 0.1)
        # Backpressure must pause the hog, not drop work: once it reads, it gets every reply.
        for i in range(200):
            self.assertEqual(decode(hog.raw_reply()), value, f"reply {i}")
        self.assertEqual(hog.value("PING"), "PONG")
        hog.close()

    def test_200_concurrent_clients(self):
        errors = []

        def worker(i):
            try:
                c = Conn(self.srv.port)
                for j in range(50):
                    c.value("INCR", "shared")
                    assert c.value("SET", f"own{i}", str(j)) == "OK"
                c.close()
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(200)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        self.assertEqual(self.c.value("GET", "shared"), b"10000")
        self.assertEqual(self.c.value("DBSIZE"), 201)

    def test_active_expiry_reclaims_unread_keys(self):
        self.c.sock.sendall(b"".join(encode("SET", f"e{i}", "v", "PX", "100") for i in range(5000)))
        for _ in range(5000):
            self.c.raw_reply()
        self.c.value("SET", "keep", "v")
        time.sleep(1.5)  # never read the keys; the cron has to find them
        self.assertEqual(self.c.value("DBSIZE"), 1)

    def test_single_database(self):
        # A deliberate difference from Redis (16 databases): tinykv has one, and says so.
        self.assertEqual(self.c.value("SELECT", "0"), "OK")
        self.assertEqual(str(self.c.value("SELECT", "1")), "ERR DB index is out of range")

    def test_shutdown_command(self):
        self.c.sock.sendall(encode("SHUTDOWN"))
        self.assertEqual(self.c.sock.recv(16), b"")  # closes without replying, like Redis
        self.srv.proc.wait(5)
        self.assertEqual(self.srv.proc.returncode, 0)


class Persistence(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.aof = os.path.join(self.dir, "appendonly.aof")

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def start(self, *extra):
        return tinykv("--appendonly", self.aof, *extra)

    def test_survives_kill_9(self):
        srv = self.start("--appendfsync", "always")
        c = Conn(srv.port)
        c.value("SET", "a", "1")
        c.value("INCRBY", "a", "41")
        c.value("SET", "ttl", "v", "EX", "1000")
        c.value("MSET", "x", "1", "y", "2")
        c.value("DEL", "x")
        c.value("SET", "gone", "v", "PX", "50")
        c.close()
        srv.stop(signal.SIGKILL)  # no chance to flush anything at exit
        time.sleep(0.1)
        srv = self.start()
        c = Conn(srv.port)
        self.assertEqual(c.value("GET", "a"), b"42")
        self.assertGreater(c.value("TTL", "ttl"), 990)
        self.assertEqual(c.value("MGET", "x", "y"), [None, b"2"])
        self.assertIsNone(c.value("GET", "gone"))
        c.close()
        srv.stop()

    def test_replay_keeps_ttl_through_incr(self):
        """INCR on a key with a TTL keeps the TTL; the log must replay to the same state."""
        srv = self.start("--appendfsync", "always")
        c = Conn(srv.port)
        c.value("SET", "n", "1", "PX", "400")
        c.value("INCR", "n")
        c.close()
        srv.stop()
        time.sleep(0.6)  # the key has expired by now
        srv = self.start()
        c = Conn(srv.port)
        self.assertIsNone(c.value("GET", "n"))  # not resurrected as a TTL-less "1"
        self.assertEqual(c.value("DBSIZE"), 0)
        c.close()
        srv.stop()

    def test_getex_is_logged_as_what_it_did(self):
        """GETEX goes to the log as PEXPIREAT, PERSIST or DEL, so a relative PX replays as the same absolute time."""
        srv = self.start("--appendfsync", "always")
        c = Conn(srv.port)
        c.value("MSET", "rel", "1", "past", "3")
        c.value("SET", "kept", "2", "PX", "300")
        self.assertEqual(c.value("GETEX", "rel", "PX", "400"), b"1")
        self.assertEqual(c.value("GETEX", "kept", "PERSIST"), b"2")
        self.assertEqual(c.value("GETEX", "past", "PXAT", "1"), b"3")
        c.close()
        srv.stop()
        with open(self.aof, "rb") as f:
            self.assertNotIn(b"GETEX", f.read())
        time.sleep(0.6)  # past both TTLs; logged as-is, "rel" would get a fresh 400 ms on replay
        srv = self.start()
        c = Conn(srv.port)
        self.assertEqual(c.value("MGET", "rel", "kept", "past"), [None, b"2", None])
        self.assertEqual(c.value("TTL", "kept"), -1)
        c.close()
        srv.stop()

    def test_truncated_tail_is_dropped(self):
        with open(self.aof, "wb") as f:
            f.write(encode("SET", "ok", "1") + encode("SET", "ok2", "2") + encode("SET", "partial", "x")[:-5])
        srv = self.start()
        c = Conn(srv.port)
        self.assertEqual(c.value("MGET", "ok", "ok2", "partial"), [b"1", b"2", None])
        c.value("SET", "after", "3")  # appends cleanly after the truncation point
        c.close()
        log = srv.stop()
        self.assertIn("truncated command", log)
        srv = self.start()
        c = Conn(srv.port)
        self.assertEqual(c.value("MGET", "ok", "after"), [b"1", b"3"])
        c.close()
        srv.stop()

    def test_corrupt_file_refuses_to_start(self):
        with open(self.aof, "wb") as f:
            f.write(encode("SET", "a", "1") + b"*2\r\n$3\r\nGET\r\nXX\r\n" + encode("SET", "b", "2"))
        with self.assertRaises(RuntimeError) as cm:
            self.start()
        self.assertIn("corrupt", str(cm.exception))

    def test_bgrewriteaof_compacts_and_keeps_concurrent_writes(self):
        srv = self.start()
        c = Conn(srv.port)
        c.sock.sendall(b"".join(encode("INCR", f"k{i % 100}") for i in range(50_000)))
        for _ in range(50_000):
            c.raw_reply()
        c.value("SET", "ttl", "v", "EX", "1000")
        time.sleep(1.2)  # let everysec flush
        before = os.path.getsize(self.aof)
        self.assertEqual(c.value("BGREWRITEAOF"), "Background append only file rewriting started")
        for i in range(500):  # writes racing the child
            c.value("SET", f"during{i}", str(i))
        deadline = time.time() + 10
        while b"aof_rewrites:1" not in c.value("INFO") and time.time() < deadline:
            time.sleep(0.05)
        self.assertIn(b"aof_rewrites:1", c.value("INFO"))
        c.value("SET", "after", "x")
        c.close()
        srv.stop()
        after = os.path.getsize(self.aof)
        self.assertLess(after, before / 10, f"{before} -> {after} bytes")
        srv = self.start()
        c = Conn(srv.port)
        self.assertEqual(c.value("GET", "k7"), b"500")
        self.assertEqual(c.value("GET", "during499"), b"499")
        self.assertEqual(c.value("GET", "after"), b"x")
        self.assertGreater(c.value("TTL", "ttl"), 900)
        self.assertEqual(c.value("DBSIZE"), 100 + 1 + 500 + 1)
        c.close()
        srv.stop()


if __name__ == "__main__":
    unittest.main(verbosity=2)
