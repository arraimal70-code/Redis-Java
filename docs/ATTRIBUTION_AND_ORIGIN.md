# Attribution, Origin & Academic Provenance

## 1. Statement of Academic & Technical Honesty

This repository contains an original systems implementation of an in-memory, key-value data store modeled after the Redis specification, implemented from the ground up in **Core Java 21** using only the Java Standard Library (`java.nio`, `java.util.concurrent`, `java.io`, `java.net`).

### Provenance Declaration:
1. **Zero High-Level Frameworks:** This project does **not** employ Netty, Spring, Grizzly, Akka, Vert.x, or any external networking or serialization libraries.
2. **Zero Third-Party Dependencies:** The project has **zero runtime dependencies** declared in `pom.xml`. All network multiplexing, RESP protocol decoding, buffer management, concurrency primitives, and persistence routines were authored directly.
3. **No Fabricated Benchmarks:** Every throughput metric (RPS) and latency percentile ($p50, p90, p95, p99, p99.9$) documented in `docs/BENCHMARKING.md` was generated from physical execution of `com.redisclone.benchmark.RedisBenchmark` on the designated host machine and exported to raw CSV/JSON logs.
4. **Transparent Design Tradeoffs:** This system is an educational and research-grade systems implementation, not an official enterprise replacement for C-native Redis. Differences in memory layout, single-core throughput, and lack of TLS/ACLs are explicitly documented.

---

## 2. Theoretical & Protocol Influences

While all code in this repository was written independently, the architectural principles, protocol specifications, and algorithms draw direct inspiration from established computer science literature and foundational open-source systems:

| System / Algorithm | Original Creator / Author | Influenced Mechanism in this Project |
| :--- | :--- | :--- |
| **Redis & RESP Specification** | Salvatore Sanfilippo (antirez) | The Redis Serialization Protocol (RESP2) wire format, command vocabulary (`GET`, `SET`, `INCR`, `MULTI`, `EXEC`, `PSYNC`), and single-threaded execution invariant. |
| **Reactor Pattern** | Douglas C. Schmidt | The event-driven network architecture implemented in `NioEventLoop.java` using Java NIO `Selector` for demultiplexing socket readiness events. |
| **Active Key Expiration** | Salvatore Sanfilippo | The 10Hz probabilistic sampling algorithm implemented in `EvictionEngine.java`, sampling 20 keys per cycle to bound expired memory under 25%. |
| **CRC16 Hash Slot Partitioning** | Redis Cluster Specification | The 16,384 discrete slot routing algorithm and `{hash_tag}` extraction logic implemented in `ClusterSlotRouter.java`. |
| **Write-Ahead Logging (WAL)** | Jim Gray (Turing Award Lecture, 1998) | The append-only durability log with configurable `fsync` policies implemented in `AofManager.java`. |

---

## 3. Comparison with Reference Implementations

### 3.1 Comparison with Official Redis (C)
- **Official Redis (Salvatore Sanfilippo / Redis Ltd.):**
  - Written in ANSI C (C99/C11).
  - Uses manual memory allocation via `jemalloc` with custom SDS (Simple Dynamic Strings) and `dictEntry` structs.
  - Native POSIX event loops (`ae.c`) wrapping `epoll` (Linux), `kqueue` (macOS/BSD), or `evport` (Solaris).
- **This Project (Core Java 21):**
  - Written in modern Java 21 using object-oriented abstractions and Java NIO channels.
  - Leverages JVM Garbage Collection (Generational ZGC / G1GC) for memory management instead of manual pointer arithmetic.
  - Portable across Windows, Linux, and macOS without native compilation toolchains.

### 3.2 Comparison with Java Netty
- **Netty Framework:** Provides industrial-grade event loops, `ByteBuf` reference counting, and pipeline handlers.
- **This Project:** Implements raw `java.nio.channels.Selector`, manual channel interest bitmasks (`SelectionKey.OP_READ | SelectionKey.OP_WRITE`), and streaming byte buffer parsing directly, exposing the core mechanics of non-blocking I/O for pedagogical and systems research defense.
