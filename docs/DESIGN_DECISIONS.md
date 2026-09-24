# Architecture Decision Records (ADRs)

This document formalizes the major architectural and systems engineering decisions made during the design and evolution of the Core Java 21 Redis clone. Each record outlines context, decision, alternatives considered, and consequences.

---

## ADR 001: Single-Threaded Java NIO Reactor vs Multi-Threaded Worker Pool

### Context
Redis achieves high single-core efficiency and deterministic atomicity by executing data mutations sequentially on a single thread. In Java, modern architectures often reach for thread-per-client or multi-threaded worker pools (`Executors.newVirtualThreadPerTaskExecutor()`). We needed to decide between an event-driven single-threaded reactor and a multi-threaded execution pipeline.

### Decision
Implement a **Single-Threaded Java NIO Reactor Loop** using `java.nio.channels.Selector`.

### Alternatives Considered
1. **Thread-Per-Client Blocking I/O (`ServerSocket`):** High OS thread stack overhead (1MB per thread), severe context switching degradation above 1,000 connections.
2. **Java 21 Virtual Threads (`newVirtualThreadPerTaskExecutor`):** Extremely lightweight for blocking I/O, but introduces concurrent lock contention across the in-memory store, necessitating fine-grained striping or CAS loops on every single read/write operation.
3. **Netty Framework:** Adds external third-party dependencies, abstracting away low-level NIO selector mechanics and byte buffer management.

### Consequences
- **Positive:** Guarantees absolute sequential consistency for command execution; completely eliminates internal locking, lock convoying, and race conditions during transaction blocks (`MULTI`/`EXEC`).
- **Negative:** Single-core throughput bound (~42,000 RPS on current host). A long-running command (e.g. `KEYS *`) blocks all concurrent clients until completed.

---

## ADR 002: In-Memory Storage Engine via `ConcurrentHashMap` vs Custom Hash Table

### Context
The keyspace requires sub-microsecond lookups, fast inserts, dynamic resizing, and thread safety for background persistence threads (AOF background sync and RDB serialization).

### Decision
Utilize `java.util.concurrent.ConcurrentHashMap` wrapped in a custom `DataStore` abstraction.

### Alternatives Considered
1. **Custom Open-Addressing Hash Table:** Can achieve higher cache locality by flattening structs into primitive arrays, but incurs massive complexity during dynamic re-hashing and concurrent snapshotting.
2. **Standard `HashMap` with `ReentrantReadWriteLock`:** Single global lock creates severe read-write contention under heavy concurrent client pools.

### Consequences
- **Positive:** Lock-free reads via volatile bucket pointer reads, segmented tree-bin expansion under high collision, and safe iterator traversal during RDB snapshotting without freezing the main event loop.
- **Negative:** Slightly higher memory footprint per key (~32-48 bytes per entry) compared to raw C structs (`dictEntry`) due to JVM object headers and reference padding.

---

## ADR 003: Probabilistic Active Expiration vs Global DelayQueue / TimerWheel

### Context
Keys with TTLs must be cleaned up to prevent unbounded memory growth. We needed an eviction mechanism that cleans abandoned keys without incurring $O(N)$ scanning or heavy insertion overhead on every `SET ... EX`.

### Decision
Implement **Probabilistic 10Hz Sampling (Active Expiration)** matching official Redis semantics.

### Alternatives Considered
1. **Global Priority Queue / `DelayQueue`:** $O(\log N)$ insertion and deletion overhead on every write operation, high lock contention between writer threads and timer thread.
2. **Hashed Timing Wheel:** Constant time $O(1)$ insertion, but requires complex bucket resizing and significant memory overhead for long TTLs (hours to days).
3. **Passive Expiration Only:** Zero overhead during execution, but abandoned keys that are never queried again remain in memory forever, leaking RAM.

### Consequences
- **Positive:** $O(k)$ bounded iterator sampling (20 keys per cycle) keeps latency deterministic under 100 microseconds. Memory overhead of expired keys is statistically bounded below 25%.
- **Negative:** Highly transient spikes in expired keys may take several consecutive cycles (several hundred milliseconds) to completely reclaim.

---

## ADR 004: Custom Zero-Allocation Streaming RESP Parser vs Regex / Tokenizer

### Context
Redis commands arrive across TCP as continuous streams of framed bytes. Frames may arrive fragmented or pipelined into large single chunks.

### Decision
Implement a custom **Streaming State-Machine RESP Parser (`RespParser.java`)** operating directly on `ByteBuffer` slices.

### Alternatives Considered
1. **`String.split("\r\n")` or `Scanner`:** Massive Young Gen GC allocation pressure due to intermediate `String` generation; breaks binary-safe payloads containing embedded `\r\n`.
2. **Regular Expressions (`java.util.regex`):** High CPU overhead and catastrophic backtracking risks on malformed inputs.

### Consequences
- **Positive:** Zero unnecessary allocations for numeric parsing; true binary safety for bulk strings; sub-millisecond execution times.
- **Negative:** Requires careful boundary tracking, pointer arithmetic, and explicit overflow checks (`parseAsciiLong`).

---

## ADR 005: Dual Persistence Engines (AOF Write-Ahead Log + RDB Point-in-Time Snapshots)

### Context
Distributed and standalone cache databases require configurable durability guarantees ranging from pure volatile caching to strict disaster recovery.

### Decision
Implement **both Append-Only File (AOF) with configurable fsync policies (`ALWAYS`, `EVERYSEC`, `NO`) and binary RDB snapshotting**.

### Alternatives Considered
1. **AOF Only:** Excellent durability and human-readable replay, but file sizes grow large without log compaction, slowing startup recovery times.
2. **RDB Only:** Fast loading on startup, but risks losing minutes of data if a crash occurs between snapshot intervals.

### Consequences
- **Positive:** Provides operators full control over the durability-latency trade-off curve (from in-memory caching to sub-second data loss tolerance).
- **Negative:** Increases disk I/O load when both are active; requires robust recovery handlers for both file formats.
