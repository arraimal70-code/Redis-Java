# Architecture Decision Records (ADRs)

This document formalizes the architectural and systems engineering decisions made in the Core Java 21 Redis clone. Each record outlines context, decision, alternatives considered, and consequences.

---

## ADR 001: Single-Threaded Java NIO Reactor vs Multi-Threaded Worker Pool

### Context
Redis achieves high single-core efficiency and deterministic atomicity by executing data mutations sequentially on a single thread. In Java, architectures often default to thread-per-connection or thread-pool models. We needed to choose between an event-driven single-threaded reactor and a multi-threaded execution pipeline.

### Decision
Implement a **Single-Threaded Java NIO Reactor Loop** using `java.nio.channels.Selector`.

### Alternatives Considered
1. **Thread-Per-Client Blocking I/O (`ServerSocket`):** High OS thread stack overhead (1MB per thread), severe context-switching degradation beyond 1,000 connections.
2. **Java 21 Virtual Threads (`newVirtualThreadPerTaskExecutor`):** Lightweight for blocking I/O, but introduces concurrent lock contention across the in-memory keyspace, requiring locks or CAS loops on every read and write.
3. **Netty Framework:** Adds external third-party dependencies, abstracting away low-level NIO selector mechanics and byte buffer management.

### Consequences
- **Positive:** Guarantees sequential consistency for command execution; eliminates internal locking, lock convoying, and race conditions during transaction blocks (`MULTI`/`EXEC`).
- **Negative:** Throughput is bounded by single-core CPU execution. Long-running operations block concurrent clients on the event loop until completed.

---

## ADR 002: In-Memory Storage Engine via `ConcurrentHashMap` vs Custom Hash Table

### Context
The keyspace requires sub-microsecond lookups, fast inserts, dynamic resizing, and safe concurrent access for background persistence threads (AOF background sync and RDB serialization).

### Decision
Utilize `java.util.concurrent.ConcurrentHashMap` wrapped in a custom `DataStore` abstraction.

### Alternatives Considered
1. **Custom Open-Addressing Hash Table:** Can achieve high cache locality with primitive arrays, but incurs substantial complexity during dynamic rehashing and concurrent snapshotting.
2. **Standard `HashMap` with `ReentrantReadWriteLock`:** A single global lock creates read-write contention under heavy concurrent client pools.

### Consequences
- **Positive:** Lock-free reads via volatile bucket pointer reads, segmented tree-bin expansion under hash collision, and safe iterator traversal during RDB snapshotting without freezing the main event loop.
- **Negative:** Higher memory footprint per key compared to raw C structs (`dictEntry`) due to JVM object headers and reference padding.

---

## ADR 003: Probabilistic Active Expiration vs Global DelayQueue / TimerWheel

### Context
Keys with TTLs must be cleaned up to prevent unbounded memory growth. We needed an eviction mechanism that cleans abandoned keys without incurring $O(N)$ scanning or heavy insertion overhead on every `SET ... EX`.

### Decision
Implement **Probabilistic 10Hz Sampling (Active Expiration)** matching Redis's `expire.c`.

### Alternatives Considered
1. **Global Priority Queue / `DelayQueue`:** $O(\log N)$ insertion and deletion overhead on every write operation; lock contention between writer threads and timer thread.
2. **Hashed Timing Wheel:** $O(1)$ insertion, but requires complex bucket resizing and significant memory overhead for long TTLs (hours to days).
3. **Passive Expiration Only:** Zero overhead during write execution, but abandoned keys that are never queried again remain in memory indefinitely.

### Consequences
- **Positive:** Bounded iterator sampling (20 keys per 100ms cycle) keeps latency bounded under 100 microseconds. The proportion of expired keys in memory is statistically kept below 25%.
- **Negative:** Large spikes of expired keys take several consecutive 100ms cycles to completely reclaim.

---

## ADR 004: Custom Streaming RESP Parser vs Regex / Tokenizer

### Context
Redis commands arrive over TCP as continuous byte streams. Frames may arrive fragmented across network packets or pipelined together in a single packet.

### Decision
Implement a custom **Streaming State-Machine RESP Parser (`RespParser.java`)** operating directly on `ByteBuffer` slices.

### Alternatives Considered
1. **`String.split("\r\n")` or `Scanner`:** High GC allocation pressure from intermediate `String` objects; breaks binary-safe payloads containing embedded `\r\n`.
2. **Regular Expressions (`java.util.regex`):** High CPU overhead and catastrophic backtracking risks on malformed inputs.

### Consequences
- **Positive:** Zero heap allocations for numeric parsing; binary safety for bulk strings; sub-microsecond parsing latency.
- **Negative:** Requires explicit buffer boundary tracking, rollback checkpoints (`startPos`), and manual overflow checks (`parseAsciiLong`).

---

## ADR 005: Dual Persistence Engines (AOF Write-Ahead Log + RDB Snapshots)

### Context
Different workloads require different durability guarantees, ranging from volatile caching to disaster recovery.

### Decision
Implement **both Append-Only File (AOF) with configurable fsync policies (`ALWAYS`, `EVERYSEC`, `NO`) and binary RDB snapshotting**.

### Alternatives Considered
1. **AOF Only:** Durability with linear replay, but file sizes grow large without log compaction, slowing startup recovery times.
2. **RDB Only:** Fast loading on startup, but risks losing minutes of data if a crash occurs between snapshot intervals.

### Consequences
- **Positive:** Gives operators control over the durability-latency trade-off curve (from in-memory caching to sub-second data loss tolerance). Includes background compaction (`BGREWRITEAOF`).
- **Negative:** Increases disk I/O when both are enabled; requires maintenance of two recovery pathways.

---

## ADR 006: William Pugh SkipList with Distance Spans for Sorted Sets vs Balanced Trees

### Context
Redis Sorted Sets (`ZSET`) require $O(\log N)$ score insertion, lookup, deletion, and rank queries (`ZRANK`, `ZREVRANK`, `ZRANGE`, `ZREVRANGE`).

### Decision
Implement **William Pugh's multi-level SkipList ($p = 0.25$, 32 levels) with distance spans** paired with an $O(1)$ hash map.

### Alternatives Considered
1. **Red-Black Tree / AVL Tree:** $O(\log N)$ search and insertion, but range queries require complex tree rotations and pointer updates. Augmenting balanced trees with subtree sizes for rank queries adds substantial rebalancing overhead.
2. **`java.util.TreeSet`:** Standard library balanced tree, but lacks rank-by-score ($O(\log N)$ rank lookup) and reverse rank lookups.

### Consequences
- **Positive:** Distance spans along forward pointers enable $O(\log N)$ rank lookups. Level 0 backward pointers enable $O(1)$ reverse sequential traversal. Simpler concurrent iteration properties than balanced tree rotations.
- **Negative:** Probabilistic structure: memory usage is dependent on node level distribution (average 1.33 pointers per node at $p = 0.25$).

---

## ADR 007: Circular Ring Buffer for Replication Backlog vs Unbounded Event Queue

### Context
When a replica temporarily disconnects, the master must provide missing command deltas upon reconnection without performing a full, expensive database resynchronization (`FULLRESYNC`).

### Decision
Implement a **fixed-size 1MB circular byte ring buffer (`ReplicationBacklog.java`)** tracking monotonic 64-bit offsets.

### Alternatives Considered
1. **Unbounded Linked Queue of Commands:** Retains all writes until replicas acknowledge, but risks unbounded JVM heap growth if a replica disconnects or lags indefinitely.
2. **Full Resynchronization on Every Disconnect:** Simple to implement, but generates heavy CPU, disk, and network transfer load on transient network blips.

### Consequences
- **Positive:** Memory overhead is strictly bounded to 1MB regardless of replica behavior. Enables $O(1)$ byte extraction for partial resynchronization (`PSYNC`).
- **Negative:** If a replica is disconnected longer than it takes the master to overwrite 1MB of write commands, partial resynchronization fails and triggers a full resync.

---

## ADR 008: CRC16 Hash Slot Partitioning vs Consistent Hashing Ring

### Context
Cluster sharding requires mapping keys to discrete partitions to distribute data across multiple nodes.

### Decision
Implement **Redis-standard CRC16-CCITT across 16,384 discrete slots** with `{hash_tag}` extraction and `-MOVED` redirection frames.

### Alternatives Considered
1. **Ketama Consistent Hashing Ring:** Excellent for standalone distributed caches (e.g. Memcached), but incompatible with Redis cluster tooling and standard client libraries (`JedisCluster`, `Lettuce`).
2. **Modulo Hash on Node Count ($N$):** Changing node count invalidates almost all key assignments ($N \to N+1$).

### Consequences
- **Positive:** Strict compatibility with Redis cluster protocol specification, standard client libraries, and `{hash_tag}` co-location semantics.
- **Negative:** Dynamic cluster reconfiguration requires managing slot assignment maps.

---

## Theoretical & Protocol Influences

| System / Algorithm | Original Author / Reference | Mechanism Influenced in this Project |
| :--- | :--- | :--- |
| **RESP2 Protocol & Redis Core** | Salvatore Sanfilippo (antirez) | RESP2 wire format, command vocabulary, and single-reactor sequential execution invariant. |
| **Reactor Pattern** | Douglas C. Schmidt (1995) | Event-driven network architecture in `NioEventLoop.java` using Java NIO `Selector`. |
| **SkipList with Spans** | William Pugh (1990) | Multi-level probabilistic search structure with distance spans in `SkipList.java`. |
| **Active Key Expiration** | Salvatore Sanfilippo | 10Hz probabilistic sampling algorithm in `EvictionEngine.java`. |
| **HyperLogLog** | Flajolet, Fusy, Gandouet, Meunier (2007) | 64-register cardinal estimator with harmonic mean in `HyperLogLog.java`. |
| **Write-Ahead Logging (WAL)** | Jim Gray (1998) | Append-only durability log with configurable `fsync()` policies in `AofManager.java`. |
| **CRC16 Hash Slot Routing** | Redis Cluster Specification | 16,384 discrete slot routing with hash tags in `ClusterSlotRouter.java`. |
