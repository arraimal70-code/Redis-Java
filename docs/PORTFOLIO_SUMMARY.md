# Portfolio & Academic Admissions Summary

## 1. Stanford Application Activity Description

> [!IMPORTANT]
> **Strict Limit: 150 Characters (including spaces)**
>
> `Engineered zero-dependency Redis clone in Java 21; built NIO reactor, RESP parser, LRU cache, ACID transactions & multi-node replication engine.`
>
> *(Exact length: 148 characters)*

---

## 2. Resume-Ready Project Description

**Distributed Systems Engineer / Systems Architect**  
*Custom Redis Key-Value Database & Distributed Caching Engine (Core Java 21)*

- **Core Reactor Architecture:** Engineered a high-throughput, zero-dependency in-memory key-value database in Java 21 using standard `java.nio` Selector multiplexing and non-blocking direct ByteBuffers, achieving over **11,400 requests/sec** with sub-3ms median latency under 50 concurrent client connections.
- **Wire Protocol & Streaming Parser:** Built a reentrant, zero-allocation RESP2/RESP3 protocol parser that recovers gracefully from TCP stream packet fragmentation and command pipelining without heap thrashing.
- **ACID Transactions & Optimistic Locking:** Implemented `MULTI`/`EXEC` atomic command queuing and optimistic concurrency control (CAS) via `WATCH` with key version tracking.
- **Cache Eviction & Expiration:** Architected dual-mode TTL expiration (lazy passive lookup + 10Hz probabilistic active sweep) alongside an $O(1)$ Doubly-Linked List LRU cache eviction policy.
- **Distributed Replication & Cluster Routing:** Developed Master-Replica asynchronous stream replication using a circular replication backlog ring buffer, 64-bit monotonic offsets, and `PSYNC` handshakes; simulated 16,384-slot cluster sharding using CRC16-CCITT and client-side `-MOVED` redirection.
- **Dual Persistence Engines:** Engineered an Append-Only File (AOF) logger with configurable fsync policies (`ALWAYS`, `EVERYSEC`) and crash replay, coupled with atomic binary point-in-time snapshotting (RDB).
- **Engineering Rigor:** Validated complete system integrity with an 82-assertion end-to-end integration test harness, Docker multi-stage containerization, and GitHub Actions CI/CD.

---

## 3. Computer Science Concepts Demonstrated

| Domain | Low-Level Mechanical Concept Demonstrated | Code Artifact |
| :--- | :--- | :--- |
| **Operating Systems** | Non-blocking I/O multiplexing (`select`/`epoll`), direct `ByteBuffer` allocation, TCP send/receive buffer management, page cache syncing (`fsync`). | `NioEventLoop.java`, `AofManager.java` |
| **Networking** | Stream-oriented framing, TCP segmentation recovery, disabling Nagle's algorithm (`TCP_NODELAY`), socket reuse (`SO_REUSEADDR`), RESP wire protocol. | `RespParser.java`, `ClientConnection.java` |
| **Concurrency** | Single-reactor lock-free execution model, eliminating context-switching overhead, atomic CAS versioning (`AtomicLong`), thread-safe ring buffers. | `TransactionContext.java`, `ReplicationBacklog.java` |
| **Databases** | In-memory key-value store, typed object layout, passive/active TTL eviction, write-ahead logging (WAL/AOF), point-in-time binary snapshotting (RDB). | `DataStore.java`, `RdbManager.java` |
| **Distributed Systems** | Active-passive replication, replication backlog ring buffer, offset synchronization, `PSYNC` protocol handshake, consistent hashing & slot sharding. | `ReplicationManager.java`, `ClusterSlotRouter.java`, `Crc16.java` |
| **Software Engineering** | Strategy pattern (`EvictionPolicy`), Command pattern (`CommandRegistry`), immutable sealed hierarchy (`RespFrame`), Docker containerization, CI/CD. | `LruEvictionPolicy.java`, `Dockerfile`, `ci.yml` |
