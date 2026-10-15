#!/usr/bin/env bash
# Throughput vs. redis-server on the same machine, persistence off on both.
# Reports the median of 3 runs per configuration (requests per second).
set -euo pipefail
cd "$(dirname "$0")/.."
N=${N:-500000}
TK_PORT=7801 RS_PORT=7802
./tinykv --port $TK_PORT 2>/dev/null & TK=$!
redis-server --port $RS_PORT --save "" --appendonly no --bind 127.0.0.1 >/dev/null & RS=$!
trap 'kill $TK $RS 2>/dev/null' EXIT
sleep 0.5

median() { sort -n | sed -n 2p; }
run() { # port, test, pipeline
  for _ in 1 2 3; do
    redis-benchmark -p "$1" -t "$2" -n "$N" -c 50 -P "$3" -r 100000 -d 64 --csv -q 2>/dev/null \
      | awk -F'"' 'NR==2 {print $4}'
  done | median
}

printf "%-6s %-9s %14s %14s %8s\n" test pipeline "tinykv req/s" "redis req/s" ratio
for p in 1 16; do
  for t in set get incr; do
    a=$(run $TK_PORT $t $p); b=$(run $RS_PORT $t $p)
    printf "%-6s %-9s %14.0f %14.0f %7.2fx\n" "$t" "$p" "$a" "$b" "$(echo "$a / $b" | bc -l)"
  done
done
