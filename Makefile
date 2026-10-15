CC      ?= cc
CFLAGS  ?= -O2 -g
WARN    := -std=c11 -Wall -Wextra -Wpedantic -Wshadow -Wstrict-prototypes -Werror -D_DEFAULT_SOURCE -D_DARWIN_C_SOURCE
# ASan + UBSan by default (CI). Where ASan can't start (some sandboxed macOS setups hang
# before main), run with SAN=undefined.
SAN     ?= address,undefined
SRC     := src/server.c src/dict.c src/resp.c src/event.c src/siphash.c
LIB     := src/dict.c src/resp.c src/siphash.c

.PHONY: all test unit asan fuzz bench clean

all: tinykv

tinykv: $(SRC) src/*.h
	$(CC) $(WARN) $(CFLAGS) -o $@ $(SRC)

# Address + undefined-behavior sanitizers; the integration suite runs against this build in CI.
tinykv-asan: $(SRC) src/*.h
	$(CC) $(WARN) -O1 -g -fsanitize=$(SAN) -fno-omit-frame-pointer -fno-sanitize-recover=all -o $@ $(SRC)

unit-tests: tests/unit.c $(LIB) src/*.h
	$(CC) $(WARN) -O1 -g -fsanitize=$(SAN) -fno-sanitize-recover=all -Isrc -o $@ tests/unit.c $(LIB)

unit: unit-tests
	./unit-tests

test: unit tinykv-asan
	TINYKV=./tinykv-asan python3 tests/test_server.py

# libFuzzer target for the RESP parser (needs LLVM clang; Apple clang ships without libFuzzer).
fuzz-resp: fuzz/fuzz_resp.c src/resp.c src/resp.h
	clang -g -O1 -fsanitize=fuzzer,address,undefined -Isrc -o $@ fuzz/fuzz_resp.c src/resp.c

bench: tinykv
	./bench/bench.sh

clean:
	rm -rf tinykv tinykv-asan unit-tests fuzz-resp *.dSYM
