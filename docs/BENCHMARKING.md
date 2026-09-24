# Empirical Benchmarking & Performance Analysis

## 1. Executive Summary

This document presents empirical performance measurements of the Core Java 21 Redis clone under diverse concurrency levels, pipelining depths, and payload characteristics. All figures presented herein were gathered using the built-in, nanosecond-resolution benchmarking suite (`com.redisclone.benchmark.RedisBenchmark`) and exported directly to machine-readable JSON and CSV logs without external smoothing or interpolation.

### Key Performance Highlights (Empirical)
- **Peak Pipelined Throughput:** **42,357.90 requests/second** (`SET`, 20 concurrent clients, pipeline depth 64).
- **Sub-Millisecond Median Latency:** **0.059 ms - 0.068 ms (p50)** for unpipelined operations under single-client loads.
- **Concurrent Scaling (10 clients):** **22,125.64 RPS** (`PING`), **20,712.06 RPS** (`INCR`), and **20,273.08 RPS** (`GET`).
- **Large Payload Resilience (1,024 Bytes):** **17,110.55 RPS** with median latency of **0.985 ms**, demonstrating low serialization overhead.
- **Zero Errors:** Across all 200,000+ benchmark requests executed during the test suite, **zero framing errors or dropped packets** were recorded.

---

## 2. Test Environment & Methodology

### 2.1 Hardware and Runtime Specification
All tests were executed on a dedicated physical machine under the following runtime constraints:
- **Operating System:** Windows 11 Enterprise (amd64, Kernel 10.0.26100)
- **Processor:** 11th Gen Intel(R) Core(TM) i5-1135G7 @ 2.40GHz (4 physical cores, 8 logical threads)
- **RAM:** 8.00 GB DDR4 (LPDDR4x @ 3200 MHz)
- **Java Virtual Machine:** Eclipse Temurin OpenJDK 64-Bit Server VM (build 21.0.12.1+10-LTS)
- **Networking:** Localhost loopback TCP (`127.0.0.1`), SO_REUSEADDR enabled, Nagle's algorithm disabled (`TCP_NODELAY = true`)

### 2.2 Measurement Methodology
1. **JIT Compiler Warmup:** Prior to gathering metrics, each benchmark phase initiates 5,000 warmup requests across multiple threads to trigger HotSpot Tier 4 C2 JIT compilation on hot byte-manipulation and framing routines.
2. **Nanosecond Resolution:** Individual request durations are captured using `System.nanoTime()` immediately before socket write and immediately following complete frame delimiter consumption (`\r\n`).
3. **Pipelining Framing:** In pipelined workloads, batches of $N$ commands are framed and transmitted in a single write operation, followed by synchronous consumption of $N$ discrete RESP replies.
4. **Isolated Reactor State:** Tests were conducted with background disk fsync disabled (`--aof false --rdb false`) to isolate network reactor, parser, and in-memory store throughput from underlying storage drive I/O barriers.

---

## 3. Concurrency Scaling Sweep (Pipeline Depth = 1, Payload = 16B)

The following data illustrates server performance across varying client concurrency levels (1, 10, 50, 100 simultaneous persistent TCP connections). Total volume: 10,000 requests per test.

| Workload | Concurrency | Throughput (RPS) | min (ms) | p50 (ms) | p90 (ms) | p95 (ms) | p99 (ms) | p99.9 (ms) | max (ms) | Heap Delta |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **PING** | 1 | 14,361.28 | 0.039 | 0.061 | 0.084 | 0.098 | 0.181 | 0.904 | 5.013 | +1,006 KB |
| **SET** | 1 | 13,360.15 | 0.042 | 0.067 | 0.089 | 0.105 | 0.222 | 0.673 | 4.174 | +3,338 KB |
| **GET** | 1 | 15,368.53 | 0.039 | 0.061 | 0.075 | 0.086 | 0.181 | 0.259 | 0.762 | +1,773 KB |
| **INCR** | 1 | 11,617.09 | 0.041 | 0.068 | 0.098 | 0.117 | 0.243 | 2.607 | 22.293 | +2,601 KB |
| **MIXED** | 1 | 14,627.61 | 0.039 | 0.059 | 0.081 | 0.102 | 0.213 | 0.811 | 13.889 | +1,784 KB |
| **PING** | 10 | 22,125.64 | 0.056 | 0.351 | 0.653 | 0.782 | 1.397 | 2.351 | 3.271 | +1,479 KB |
| **SET** | 10 | 17,698.78 | 0.056 | 0.425 | 0.846 | 1.082 | 2.225 | 4.880 | 4.926 | +2,643 KB |
| **GET** | 10 | 20,273.08 | 0.053 | 0.396 | 0.725 | 0.849 | 1.685 | 3.198 | 3.331 | +2,591 KB |
| **INCR** | 10 | 20,712.06 | 0.049 | 0.348 | 0.710 | 0.857 | 1.799 | 4.822 | 8.977 | +2,551 KB |
| **MIXED** | 10 | 18,872.57 | 0.072 | 0.381 | 0.807 | 1.001 | 1.982 | 3.694 | 4.028 | +2,257 KB |
| **PING** | 50 | 13,417.42 | 0.048 | 2.011 | 3.929 | 6.231 | 32.156 | 51.633 | 57.287 | +6,240 KB |
| **SET** | 50 | 17,126.09 | 0.096 | 2.263 | 3.772 | 4.229 | 5.491 | 20.684 | 35.067 | +5,585 KB |
| **GET** | 50 | 17,933.58 | 0.066 | 2.098 | 3.627 | 4.044 | 4.875 | 28.239 | 57.530 | +8,174 KB |
| **INCR** | 50 | 15,563.71 | 0.064 | 2.661 | 4.489 | 5.695 | 7.742 | 19.411 | 29.945 | +5,403 KB |
| **MIXED** | 50 | 17,484.51 | 0.060 | 2.315 | 3.651 | 3.971 | 5.838 | 21.190 | 32.891 | +8,182 KB |
| **PING** | 100 | 17,242.37 | 0.093 | 3.591 | 5.763 | 6.239 | 10.830 | 132.033 | 183.263 | +10,693 KB |
| **SET** | 100 | 16,793.40 | 0.071 | 4.239 | 6.220 | 6.571 | 11.258 | 57.301 | 97.643 | +13,893 KB |
| **GET** | 100 | 16,219.72 | 0.059 | 3.850 | 5.680 | 6.040 | 9.721 | 128.624 | 167.150 | +12,050 KB |
| **INCR** | 100 | 16,957.11 | 0.065 | 3.896 | 5.443 | 5.775 | 8.976 | 137.515 | 180.300 | +9,097 KB |
| **MIXED** | 100 | 13,250.71 | 0.054 | 4.094 | 6.460 | 7.437 | 11.138 | 131.344 | 138.581 | +11,858 KB |

---

## 4. Pipelining Depth Sweeps (20 Clients, 20,000 Requests)

Pipelining aggregates multiple commands into a single TCP write packet, removing the round-trip delay penalty and amortizing socket syscalls (`send`/`recv`).

| Workload | Concurrency | Pipeline Depth | Throughput (RPS) | min (ms) | p50 (ms) | p90 (ms) | p95 (ms) | p99 (ms) | max (ms) | Speedup vs Depth 1 |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **SET** | 20 | 1 | 20,196.79 | 0.053 | 0.822 | 1.479 | 1.721 | 2.428 | 5.519 | **1.00x** (Baseline) |
| **GET** | 20 | 1 | 18,140.48 | 0.063 | 0.883 | 1.611 | 1.808 | 3.376 | 10.487 | **1.00x** (Baseline) |
| **SET** | 20 | 4 | 24,592.34 | 0.037 | 0.656 | 1.104 | 1.272 | 1.501 | 3.093 | **1.22x** |
| **GET** | 20 | 4 | 36,236.12 | 0.051 | 0.494 | 0.752 | 0.885 | 1.310 | 5.115 | **2.00x** |
| **SET** | 20 | 16 | 35,240.58 | 0.055 | 0.502 | 0.663 | 0.794 | 1.038 | 5.778 | **1.74x** |
| **GET** | 20 | 16 | 35,755.33 | 0.046 | 0.485 | 0.737 | 0.842 | 1.105 | 5.583 | **1.97x** |
| **SET** | 20 | 64 | **42,357.90** | 0.018 | **0.296** | 0.408 | 0.563 | 3.000 | 4.085 | **2.10x** |
| **GET** | 20 | 64 | **37,425.41** | 0.040 | **0.312** | 0.527 | 0.661 | 3.318 | 4.322 | **2.06x** |

### Insights:
- Increasing pipelining depth to 64 boosts `SET` throughput from **20,196 RPS to 42,357 RPS** (a **109.7% increase**).
- Median latency drops dramatically from **0.822 ms down to 0.296 ms** (p50) because multiple RESP frames are decoded in one pass from the reactor's direct read buffer without intervening socket context switches.

---

## 5. Payload & Transaction Workloads

| Workload | Concurrency | Pipeline Depth | Payload Size | Throughput (RPS) | min (ms) | p50 (ms) | p90 (ms) | p99 (ms) | max (ms) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **LARGE_PAYLOAD** | 20 | 1 | 1,024 Bytes | 17,110.55 | 0.139 | 0.985 | 1.420 | 3.189 | 52.073 |
| **TRANSACTION** | 10 | 1 | 16 Bytes | 3,470.01 | 0.128 | 1.693 | 2.961 | 9.443 | 449.830 |

### Observations:
1. **Payload Degradation:** Increasing the payload size by **64x** (from 16B to 1,024B) incurs only a **15.2% drop in throughput** (from 20,196 to 17,110 RPS), confirming that the zero-copy buffer slicing in `RespParser` avoids heavy byte-array cloning.
2. **Transaction Cost:** A transactional sequence (`MULTI` -> `SET` -> `INCR` -> `EXEC`) executes 4 distinct protocol commands per atomic batch. At 3,470 transactions/sec, the server processes approximately **13,880 individual command executions per second**, reflecting the queue-and-batch processing overhead.

---

## 6. Comparison with Official Redis (C) & Other Systems

| Architecture Metric | Redis Clone (This Project) | Official Redis 7.2 (C) | Dragonfly / KeyDB |
| :--- | :--- | :--- | :--- |
| **Language & Runtime** | Core Java 21 (Temurin HotSpot JVM) | C (GCC / Clang) | C++20 |
| **Networking Model** | Java NIO `Selector` Event Loop | Single-threaded `epoll`/`kqueue` + I/O threads | Multi-threaded Shared-Nothing Reactor |
| **Thread Model** | Single Event Loop + Background DAEMONs | Single Main Thread + Background BIO threads | Multi-threaded per-core event loops |
| **Peak In-Memory RPS** | ~42,350 RPS (Pipeline 64, 4 cores) | ~120,000 - 150,000 RPS (Pipeline 64, bare-metal Linux) | ~500,000+ RPS (Multi-core) |
| **Memory Efficiency** | JVM Object Overhead (~32-48B per entry) | C Structs / SDS String (~16-24B per entry) | Dense memory layouts |
| **Garbage Collection** | Minor GC pauses (minimized via buffer reuse) | Manual `malloc`/`free` (jemalloc) | Custom memory allocators |
| **Portability** | Universal JVM (Windows, Linux, macOS) | POSIX-only native | POSIX-only native |
