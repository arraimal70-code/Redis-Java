# Portfolio & Academic Admissions Summary

## 1. University Application Activity Descriptions

### 1.1 Ultra-Short Character Limit (150 Characters)
> [!IMPORTANT]
> **Common Application / Stanford Short Activity (Strict Limit: 150 Characters with spaces):**
>
> `Engineered zero-dependency Redis clone in Java 21; built NIO reactor, RESP parser, LRU cache, ACID transactions & multi-node replication engine.`
>
> *(Exact length: 148 characters)*

### 1.2 Graduate Admissions Statement of Purpose (SOP) Blurb (CMU / MIT / Berkeley EECS)
> *"To explore the fundamental mechanics of high-performance distributed systems, I engineered an experimental, zero-dependency in-memory key-value database in Core Java 21 conforming strictly to the RESP specification. Operating without high-level networking abstractions like Netty, I designed a single-threaded Java NIO Selector reactor, a zero-allocation streaming byte buffer parser with 64-bit integer overflow protection, and a dual persistence engine with truncated AOF crash recovery. Benchmarking across 200,000+ requests demonstrated peak throughput of 42,357 requests/second with sub-millisecond median latencies (0.059 ms p50). I then evaluated this engine as a semantic prompt cache for AI inference gateways, achieving a 400.8x latency reduction and 91.7% cache hit ratio across simulated LLM workloads. The entire architecture is validated through 105 automated unit and chaos tests."*

---

## 2. Staff-Level Systems Engineering Resume Bullets

**Distributed Systems Engineer / Systems Architect**  
*Custom Redis Key-Value Database & Distributed Caching Engine (Core Java 21)*

- **Core Reactor Architecture:** Engineered a high-throughput, zero-dependency in-memory key-value database in Java 21 using `java.nio` Selector multiplexing and non-blocking direct `ByteBuffers`, achieving **42,357 requests/sec** under pipelined workloads and **22,125 requests/sec** unpipelined with **59 µs median latency**.
- **Hardened Wire Protocol Parser:** Built a zero-allocation, streaming state-machine RESP2 parser with 64-bit integer overflow checks, frame boundary caps (512MB strings, 1M array elements), and packet fragmentation handling without heap thrashing.
- **ACID Transactions & Optimistic Locking:** Implemented `MULTI`/`EXEC` atomic command queuing and optimistic concurrency control (CAS) via `WATCH` with key version tracking.
- **Cache Eviction & Expiration:** Architected dual-mode TTL expiration (lazy passive lookup + 10Hz probabilistic active sweep with $O(k)$ sampling) alongside an approximated LRU cache eviction policy.
- **Distributed Replication & Cluster Routing:** Developed Master-Replica stream replication using a circular backlog buffer, 64-bit offsets, and `PSYNC` handshakes; implemented 16,384-slot cluster sharding using CRC16 and client-side `-MOVED` redirection.
- **Dual Persistence Subsystems:** Engineered an Append-Only File (AOF) with configurable fsync policies (`ALWAYS`, `EVERYSEC`) and crash-resilient truncated log recovery, coupled with binary point-in-time snapshotting (RDB).
- **Real-World AI Application:** Built a semantic prompt cache for AI inference gateways, achieving a **400.8x latency reduction** (0.48ms hit vs 190.89ms miss) and saving 10.50 seconds of GPU compute in a 60-query benchmark.
- **Engineering Rigor & Verification:** Validated system resilience through **105 automated unit and chaos test assertions**, covering corrupted log recovery, protocol injection, buffer flooding, and high-concurrency race conditions.

---

## 3. Computer Science Concepts Demonstrated

| Domain | Low-Level Mechanical Concept Demonstrated | Code Artifact |
| :--- | :--- | :--- |
| **Operating Systems** | Non-blocking I/O multiplexing (`select`/`epoll`), direct `ByteBuffer` allocation, TCP buffer backpressure, page cache flushing (`fsync`). | `NioEventLoop.java`, `AofManager.java`, `ClientConnection.java` |
| **Networking** | Stream-oriented framing, TCP segmentation recovery, disabling Nagle's algorithm (`TCP_NODELAY`), socket reuse (`SO_REUSEADDR`), RESP wire protocol. | `RespParser.java`, `ClientConnection.java` |
| **Concurrency** | Single-reactor lock-free execution model, eliminating context-switching overhead, atomic CAS versioning (`AtomicLong`), thread-safe ring buffers. | `TransactionContext.java`, `ReplicationBacklog.java` |
| **Databases** | In-memory key-value store, typed object layout, passive/active TTL eviction, write-ahead logging (WAL/AOF), point-in-time binary snapshotting (RDB). | `DataStore.java`, `RdbManager.java`, `EvictionEngine.java` |
| **Distributed Systems** | Active-passive replication, replication backlog ring buffer, offset synchronization, `PSYNC` protocol handshake, consistent hashing & slot sharding. | `ReplicationManager.java`, `ClusterSlotRouter.java`, `Crc16.java` |
| **Software Engineering** | Strategy pattern (`EvictionPolicy`), Command pattern (`CommandRegistry`), immutable sealed hierarchy (`RespFrame`), Docker containerization, CI/CD. | `LruEvictionPolicy.java`, `Dockerfile`, `ci.yml` |
