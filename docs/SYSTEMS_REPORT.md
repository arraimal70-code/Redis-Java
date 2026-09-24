# Systems Architecture & Engineering Report: Custom Redis Clone in Java 21

**Author:** Principal Software Architect & Distributed Systems Specialist  
**Tech Stack:** Core Java 21+, Java NIO (`Selector`, `SocketChannel`), RESP2/3 Wire Protocol, Concurrent Data Structures  

---

## 1. Executive Summary

This project is a clean-room, zero-dependency implementation of a high-performance in-memory key-value database and distributed caching engine written from scratch in Java 21+. Rather than utilizing high-level abstractions or third-party networking frameworks (such as Netty or Spring), the system is engineered directly upon operating system primitives through standard Java NIO channels and direct byte buffers.

The engine implements the complete lifecycle of an industrial-grade in-memory store:
- **Reactor Network Multiplexer:** Single-threaded event loop driven by `java.nio.channels.Selector` eliminating thread contention and synchronization overhead.
- **Streaming RESP State Machine:** Reentrant, zero-allocation protocol parser designed to recover gracefully from TCP packet fragmentation and command pipelining.
- **Hybrid Storage & Eviction Engine:** Primary key-value dictionary with passive (lazy) TTL checks, active probabilistic background sweeps (10Hz), and an $O(1)$ Doubly-Linked List LRU cache eviction policy.
- **ACID / CAS Transactions:** `MULTI`/`EXEC` atomic command queuing with optimistic concurrency control through `WATCH` version tracking.
- **Distributed Replication:** Master-replica stream propagation using a circular replication backlog ring buffer, 64-bit monotonically increasing offsets, and `PSYNC` handshakes.
- **Cluster Sharding Simulation:** 16,384 hash slots partitioned via CRC16-CCITT with hash-tag parsing and client-side `-MOVED` redirection.
- **Dual Persistence:** Append-Only File (AOF) with configurable fsync policies and binary point-in-time snapshotting (RDB) with atomic file replacement.

---

## 2. Low-Level Networking & Mechanical Sympathy

### 2.1 The Single-Reactor Pattern (`com.redisclone.network.NioEventLoop`)
Classical multi-threaded server designs assign one OS thread per connection (`Thread-per-Client`). While simple, this architecture suffers catastrophic scalability degradation under high connection counts due to kernel context switching, memory consumption (thread stack allocation of 1MB per thread), and cache thrashing across CPU cores.

Our implementation adopts the **Single-Reactor Pattern** mirroring Redis’s `ae.c`:
1. A dedicated event loop thread executes non-blocking `Selector.select(timeout)`.
2. The multiplexer monitors `OP_ACCEPT`, `OP_READ`, and `OP_WRITE` channels concurrently.
3. Socket channels are configured with:
   - `configureBlocking(false)`: Prevents kernel thread stalls on I/O.
   - `StandardSocketOptions.TCP_NODELAY = true`: Disables Nagle’s algorithm, immediately transmitting small RESP frames (such as `+OK\r\n` or `:1\r\n`) without waiting to accumulate full MTU packets.
   - `StandardSocketOptions.SO_REUSEADDR = true`: Enables immediate port re-binding upon server restart.

### 2.2 Direct ByteBuffers & Saturated Write Handling
Each connection manages a dedicated read buffer and a non-blocking queue of pending write buffers (`Queue<ByteBuffer>`). When writing replies to a client:
- If the socket send buffer is unsaturated, data is written synchronously within the loop without thread switching.
- If the OS socket buffer fills up (partial write, `channel.write()` returns fewer bytes than buffer remaining), the connection registers interest in `SelectionKey.OP_WRITE`.
- Once the OS socket buffer becomes writable again, the event loop drains the pending write queue and immediately unregisters `OP_WRITE` to prevent busy-spinning the CPU.

---

## 3. Protocol Serialization: Streaming RESP2/3 State Machine

### 3.1 Overcoming TCP Streaming Realities (`com.redisclone.resp.RespParser`)
TCP is a byte-stream protocol devoid of application message boundaries. A client request may arrive:
1. **Fragmented:** Split across arbitrary packet chunks (e.g., `*2\r\n$3\r\nS` in packet 1 and `ET\r\n$3\r\nkey\r\n` in packet 2).
2. **Pipelined:** Multiple commands packed into a single network read buffer.

Naive implementations using `Scanner`, `BufferedReader.readLine()`, or `String.split()` fail under packet fragmentation or cause severe GC pressure.

Our `RespParser` implements a reentrant state machine:
- Records an explicit `startPos = buffer.position()` before attempting to parse each token.
- If a frame is incomplete (missing CRLF or insufficient payload bytes), the parser resets the buffer position to `startPos` (`buffer.position(startPos)`) and returns `null`. The socket channel retains unconsumed bytes in place for subsequent `channel.read()` calls.
- Employs `parseAsciiLong(bytes, offset, length)` to decode integers and bulk string lengths directly from ASCII bytes without instantiating intermediate `String` objects.

---

## 4. In-Memory Storage Engine & Eviction Mechanics

### 4.1 Dual TTL Expiration (`com.redisclone.storage.DataStore`)
Key expiration operates via two complementary mechanisms:
1. **Passive (Lazy) Eviction:** On any key access (`get`, `exists`, `ttl`), the expiration timestamp is verified against `System.currentTimeMillis()`. If expired, it is purged on demand and returns `nil`.
2. **Active Probabilistic Eviction (`activeExpireCycle`):** Modeled directly after Redis’s `expire.c`, a background daemon runs at 10Hz (every 100ms):
   - Samples 20 random keys with active expirations.
   - Evicts all expired keys.
   - If $>25\%$ (more than 5 keys) were expired, it repeats the sampling cycle immediately, bounded by a 10ms execution deadline to prevent event-loop starvation.

### 4.2 O(1) Cache Eviction: LRU Strategy (`com.redisclone.storage.eviction.LruEvictionPolicy`)
When `maxKeys` or memory thresholds are exceeded, the storage engine invokes the pluggable eviction policy.
- Uses a sentinel-based Doubly Linked List (`head` = Most Recently Used, `tail` = Least Recently Used) coupled with a hash map index.
- Operations:
  - `onKeyAccess`: Moves node to `head` in $O(1)$ time.
  - `onKeyInsert`: Adds node at `head` in $O(1)$ time.
  - `onKeyDelete`: Unlinks node in $O(1)$ time.
  - `evictKey`: Removes node preceding `tail` in $O(1)$ time and purges it from the dictionary.

---

## 5. ACID / CAS Transactions (`MULTI` / `EXEC` / `WATCH`)

Redis transactions guarantee isolation and atomicity without lock contention:
- **`MULTI`:** Transitions connection state to `inTransaction = true`.
- **Command Queuing:** Incoming commands are validated and buffered in a FIFO queue (`queuedCommands`), returning `+QUEUED\r\n`.
- **`WATCH key [key...]` (Optimistic Concurrency Control):** Records the monotonically increasing `keyVersion` of each watched key.
- **`EXEC`:**
  - Verifies whether any watched key version changed since `WATCH` was executed (`isDirty()`).
  - If any watched key mutated, the entire transaction is aborted and returns `*-1\r\n` (Null Array).
  - If clean, all buffered commands execute atomically in sequence without interleaving client requests.
- **`DISCARD`:** Clears buffered queues and cancels watched keys.

---

## 6. Distributed Master-Replica Replication

### 6.1 Replication Backlog Ring Buffer (`com.redisclone.replication.ReplicationBacklog`)
- Maintains a fixed-size circular byte array (default 1MB) and a monotonically increasing 64-bit `masterOffset`.
- Generates a pseudo-random 40-character hexadecimal `masterReplId`.

### 6.2 Resynchronization Handshake (`PSYNC`)
When a replica connects to a master:
1. `PING` $\rightarrow$ `+PONG`
2. `REPLCONF listening-port <port>` $\rightarrow$ `+OK`
3. `REPLCONF capa psync2` $\rightarrow$ `+OK`
4. `PSYNC <replid> <offset>`:
   - **Partial Resync:** If the replica’s `replid` matches the master and `offset` falls within the backlog ring buffer, the master replies `+CONTINUE\r\n` and streams only the missing byte delta.
   - **Full Resync:** If disconnected beyond the backlog window or connecting for the first time, the master replies `+FULLRESYNC <replid> <offset>\r\n`.
5. **Stream Propagation:** All mutating write commands executed on the master are serialized and broadcast to connected replica sockets asynchronously.

---

## 7. Cluster Sharding Simulation

### 7.1 CRC16-CCITT Slot Partitioning (`com.redisclone.cluster.Crc16`)
Keys are partitioned deterministically across 16,384 virtual hash slots:
$$\text{Slot} = \text{CRC16}(\text{key}) \pmod{16384}$$
Supports **Hash Tags**: if a key contains curly brackets `{...}`, only the substring within the brackets is hashed (e.g. `{user100}:profile` and `{user100}:orders` hash to the same slot, enabling atomic multi-key operations).

### 7.2 Client-Side Redirection (`-MOVED`)
The `ClusterSlotRouter` tracks slot ownership ranges across topology nodes. If a command targets a key residing on a slot not owned by the recipient node, the node replies:
`-MOVED <slot> <target_node_ip:target_node_port>\r\n`
This accurately implements the Redis Cluster client redirection protocol.

---

## 8. Persistence Architecture: AOF & RDB

| Metric | Append-Only File (AOF) | Redis Database Snapshot (RDB) |
| :--- | :--- | :--- |
| **Mechanic** | Write-ahead operation logging | Point-in-time binary memory snapshot |
| **Durability** | Configurable (`ALWAYS`, `EVERYSEC`, `NO`) | Periodic or explicit (`SAVE`, `BGSAVE`) |
| **File Format** | Standard human-readable RESP protocol stream | Compact binary format (`REDIS0009` header) |
| **Recovery Speed**| Replays all mutating commands sequentially | Direct binary deserialization into memory |
| **Atomicity** | Append-only sequential disk writes | Atomic temp file write followed by `Files.move()` |

---

## 9. Empirical Performance & Systems Evaluation

Benchmarked using `RedisBenchmark.java` across 200,000+ requests with nanosecond resolution, HotSpot C2 JIT warmup, and machine-readable exports:

### 9.1 Concurrency Scaling (Depth 1, 16B Payload)

| Command | Concurrency | Throughput (RPS) | Min Latency | Median (p50) | 99th Percentile (p99) | Max Latency |
| :--- | :---: | :--- | :--- | :--- | :--- | :--- |
| **PING** | 1 | **14,361.28 req/sec** | 0.039 ms | 0.061 ms | 0.181 ms | 5.013 ms |
| **SET** | 1 | **13,360.15 req/sec** | 0.042 ms | 0.067 ms | 0.222 ms | 4.174 ms |
| **GET** | 1 | **15,368.53 req/sec** | 0.039 ms | 0.061 ms | 0.181 ms | 0.762 ms |
| **PING** | 10 | **22,125.64 req/sec** | 0.056 ms | 0.351 ms | 1.397 ms | 3.271 ms |
| **SET** | 10 | **17,698.78 req/sec** | 0.056 ms | 0.425 ms | 2.225 ms | 4.926 ms |
| **GET** | 10 | **20,273.08 req/sec** | 0.053 ms | 0.396 ms | 1.685 ms | 3.331 ms |
| **SET** | 50 | **17,126.09 req/sec** | 0.096 ms | 2.263 ms | 5.491 ms | 35.067 ms |
| **GET** | 50 | **17,933.58 req/sec** | 0.066 ms | 2.098 ms | 4.875 ms | 57.530 ms |

### 9.2 Pipelining Depth Scaling (20 Clients)

| Command | Pipeline Depth | Throughput (RPS) | Median (p50) | 99th Percentile (p99) | Speedup Multiplier |
| :--- | :---: | :--- | :--- | :--- | :--- |
| **SET** | 1 | **20,196.79 req/sec** | 0.822 ms | 2.428 ms | 1.00x (Baseline) |
| **SET** | 4 | **24,592.34 req/sec** | 0.656 ms | 1.501 ms | 1.22x |
| **SET** | 16 | **35,240.58 req/sec** | 0.502 ms | 1.038 ms | 1.74x |
| **SET** | 64 | **42,357.90 req/sec** | 0.296 ms | 3.000 ms | **2.10x** |
| **GET** | 64 | **37,425.41 req/sec** | 0.312 ms | 3.318 ms | **2.06x** |

### 9.3 Real-World Application: AI Inference Prompt Cache
- Tested with 60 simulated LLM prompts via `AiInferenceCache.java`.
- **Cache Hit Latency:** **0.48 ms** vs **190.89 ms** on GPU cache miss (**400.8x faster**).
- **Hit Ratio:** **91.7%** (55 hits / 5 misses), avoiding 10.50 seconds of GPU compute.

### 9.4 Fault Tolerance & Chaos Testing
- **105 / 105 automated unit and chaos tests passed**.
- Validated recovery from truncated AOF files, corrupted RDB magic headers, 64-bit integer overflow inputs, bounded buffer OOM protection, and 50-thread atomic concurrency contention.

