#!/usr/bin/env bash
set -euo pipefail

echo "================================================================================"
echo " AUTOMATED CHAOS, REPLICATION & FAULT-INJECTION TEST HARNESS (POSIX)"
echo "================================================================================"

MASTER_PORT=6389
REPLICA_PORT=6390

cleanup() {
    echo "Cleaning up background server processes..."
    kill -9 "${MASTER_PID:-}" 2>/dev/null || true
    kill -9 "${REPLICA_PID:-}" 2>/dev/null || true
}
trap cleanup EXIT

echo "[1/5] Compiling Core Java 21 Redis Engine..."
mkdir -p bin
find src -name "*.java" > sources.txt
javac -d bin @sources.txt
rm -f sources.txt

echo "[2/5] Starting Master Node on port ${MASTER_PORT}..."
java -cp bin com.redisclone.server.RedisServer --port "${MASTER_PORT}" --aof false --rdb false &
MASTER_PID=$!
sleep 2

echo "[3/5] Starting Replica Node on port ${REPLICA_PORT}..."
java -cp bin com.redisclone.server.RedisServer --port "${REPLICA_PORT}" --replicaof 127.0.0.1 "${MASTER_PORT}" --aof false --rdb false &
REPLICA_PID=$!
sleep 2

echo "[4/5] Executing Core System & Chaos Test Suites..."
find test -name "*.java" > test_sources.txt
javac -d bin -cp "bin" @test_sources.txt
rm -f test_sources.txt
java -cp bin com.redisclone.RedisServerTest
java -cp bin com.redisclone.FailureAndEdgeCaseTest

echo "[5/5] Executing Low-Level Microbenchmark Suite..."
java -cp bin com.redisclone.benchmark.MicrobenchmarkSuite

echo "================================================================================"
echo " ✅ ALL CHAOS, REPLICATION & FAULT TESTS PASSED DETERMINISTICALLY!"
echo "================================================================================"
