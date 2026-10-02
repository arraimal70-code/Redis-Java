# Redis Clone: Comprehensive Command Reference Manual

This document provides the definitive, production-grade specification for all **47 commands** supported by the Core Java 21 Redis Clone engine. Every entry documents syntax, arguments, time complexity, RESP2 wire format return types, edge cases, error conditions, and concrete interactive examples.

---

## Command Quick Navigation

| Category | Supported Commands |
| :--- | :--- |
| [**Strings**](#1-string-commands) | `SET`, `GET`, `INCR`, `MSET`, `MGET` |
| [**Keys & Expiration**](#2-key-management--expiration-commands) | `DEL`, `EXPIRE`, `TTL`, `EXISTS` |
| [**Hashes**](#3-hash-commands) | `HSET`, `HGET`, `HGETALL` |
| [**Lists**](#4-list-commands) | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN` |
| [**Sorted Sets (ZSET)**](#5-sorted-set-zset-commands) | `ZADD`, `ZSCORE`, `ZCARD`, `ZCOUNT`, `ZRANK`, `ZREVRANK`, `ZRANGE`, `ZREVRANGE`, `ZREM` |
| [**Bitmaps**](#6-bitmap-commands) | `SETBIT`, `GETBIT`, `BITCOUNT` |
| [**HyperLogLog**](#7-hyperloglog-cardinality-commands) | `PFADD`, `PFCOUNT` |
| [**Redis Streams**](#8-redis-stream-commands) | `XADD`, `XLEN`, `XRANGE` |
| [**Transactions (ACID/CAS)**](#9-acid--cas-transaction-commands) | `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH` |
| [**Pub/Sub Messaging**](#10-pubsub-messaging-commands) | `PUBLISH`, `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE` |
| [**Replication**](#11-replication-commands) | `REPLICAOF`, `SLAVEOF`, `PSYNC`, `REPLCONF` |
| [**Cluster Sharding**](#12-cluster-sharding-commands) | `CLUSTER KEYSLOT`, `CLUSTER SLOTS`, `CLUSTER NODES` |
| [**Persistence**](#13-persistence-commands) | `SAVE`, `BGSAVE`, `BGREWRITEAOF` |
| [**Utility & Telemetry**](#14-utility--telemetry-commands) | `PING`, `ECHO`, `INFO`, `DBSIZE`, `FLUSHDB`, `FLUSHALL`, `AUTH`, `COMMAND`, `QUIT` |

---

## 1. String Commands

### `SET`
Stores a string value at `key`. Supports optional expiration in seconds (`EX`) or milliseconds (`PX`).

- **Syntax:** `SET key value [EX seconds] [PX milliseconds]`
- **Complexity:** $O(1)$
- **Return Value:**
  - Simple String: `+OK\r\n`
- **Error Modes:**
  - `-ERR wrong number of arguments for 'set' command` if fewer than 2 arguments.
  - `-ERR value is not an integer or out of range` if `EX` or `PX` value is invalid.
- **Example (`redis-cli`):**
  ```text
  127.0.0.1:6379> SET user:100 "Alice"
  OK
  127.0.0.1:6379> SET session:token "xyz987" EX 60
  OK
  127.0.0.1:6379> SET lock:resource "thread-1" PX 5000
  OK
  ```

---

### `GET`
Retrieves the string value stored at `key`. Expired keys are passively pruned on access.

- **Syntax:** `GET key`
- **Complexity:** $O(1)$
- **Return Value:**
  - Bulk String: `$5\r\nAlice\r\n`
  - Null Bulk String: `$-1\r\n` (if `key` does not exist or has expired)
- **Error Modes:**
  - `-WRONGTYPE Operation against a key holding the wrong kind of value` if the key contains a non-string object.
  - `-ERR wrong number of arguments for 'get' command` if argument count is not 1.
- **Example:**
  ```text
  127.0.0.1:6379> GET user:100
  "Alice"
  127.0.0.1:6379> GET non_existing_key
  (nil)
  ```

---

### `INCR`
Atomically increments the integer value stored at `key` by `1`. If the key does not exist, it is initialized to `0` prior to incrementing.

- **Syntax:** `INCR key`
- **Complexity:** $O(1)$
- **Return Value:**
  - Integer: `:<new_value>\r\n`
- **Error Modes:**
  - `-ERR value is not an integer or out of range` if string payload cannot be parsed as a 64-bit signed integer.
  - `-WRONGTYPE Operation against a key holding the wrong kind of value` if key holds non-string data.
- **Example:**
  ```text
  127.0.0.1:6379> SET page_views "42"
  OK
  127.0.0.1:6379> INCR page_views
  (integer) 43
  ```

---

### `MSET`
Sets multiple key-value pairs atomically in a single round-trip.

- **Syntax:** `MSET key1 value1 [key2 value2 ...]`
- **Complexity:** $O(N)$ where $N$ is the number of keys.
- **Return Value:**
  - Simple String: `+OK\r\n`
- **Error Modes:**
  - `-ERR wrong number of arguments for 'mset' command` if argument count is odd or empty.
- **Example:**
  ```text
  127.0.0.1:6379> MSET k1 "v1" k2 "v2" k3 "v3"
  OK
  ```

---

### `MGET`
Retrieves the values of all specified keys. For keys that do not exist or hold non-string types, `nil` is returned in that array position.

- **Syntax:** `MGET key1 [key2 ...]`
- **Complexity:** $O(N)$ where $N$ is the number of keys.
- **Return Value:**
  - Array of Bulk Strings: `*3\r\n$2\r\nv1\r\n$2\r\nv2\r\n$-1\r\n`
- **Example:**
  ```text
  127.0.0.1:6379> MGET k1 k2 missing_key
  1) "v1"
  2) "v2"
  3) (nil)
  ```

---

## 2. Key Management & Expiration Commands

### `DEL`
Deletes one or more specified keys. Keys that do not exist are silently ignored.

- **Syntax:** `DEL key [key ...]`
- **Complexity:** $O(N)$ where $N$ is the number of keys to remove.
- **Return Value:**
  - Integer: `:<count_of_deleted_keys>\r\n`
- **Example:**
  ```text
  127.0.0.1:6379> DEL k1 k2 missing_key
  (integer) 2
  ```

---

### `EXPIRE`
Sets an active time-to-live (TTL) in seconds on `key`.

- **Syntax:** `EXPIRE key seconds`
- **Complexity:** $O(1)$
- **Return Value:**
  - Integer: `:1\r\n` if timeout was set; `:0\r\n` if key does not exist.
- **Error Modes:**
  - `-ERR value is not an integer or out of range` if seconds cannot be parsed as a 64-bit integer.
- **Example:**
  ```text
  127.0.0.1:6379> EXPIRE user:100 300
  (integer) 1
  ```

---

### `TTL`
Returns the remaining time to live in seconds for a key.

- **Syntax:** `TTL key`
- **Complexity:** $O(1)$
- **Return Value:**
  - Integer: `:<seconds>\r\n`
  - `:-2\r\n` if key does not exist or has expired.
  - `:-1\r\n` if key exists but has no associated expire timeout.
- **Example:**
  ```text
  127.0.0.1:6379> TTL user:100
  (integer) 289
  127.0.0.1:6379> TTL unexpiring_key
  (integer) -1
  127.0.0.1:6379> TTL missing_key
  (integer) -2
  ```

---

### `EXISTS`
Checks whether one or more keys exist in the database.

- **Syntax:** `EXISTS key [key ...]`
- **Complexity:** $O(N)$ where $N$ is the number of keys tested.
- **Return Value:**
  - Integer: Number of existing keys among the queried arguments.
- **Example:**
  ```text
  127.0.0.1:6379> EXISTS user:100 missing_key page_views
  (integer) 2
  ```

---

## 3. Hash Commands

### `HSET`
Sets the specified fields to their respective values in the hash stored at `key`.

- **Syntax:** `HSET key field value [field value ...]`
- **Complexity:** $O(M)$ where $M$ is the number of field-value pairs added.
- **Return Value:**
  - Integer: Number of new fields added (existing fields overwritten are not counted).
- **Error Modes:**
  - `-WRONGTYPE Operation against a key holding the wrong kind of value`
  - `-ERR wrong number of arguments for 'hset' command`
- **Example:**
  ```text
  127.0.0.1:6379> HSET user:profile name "Bob" age "30" role "Admin"
  (integer) 3
  ```

---

### `HGET`
Returns the value associated with `field` in the hash stored at `key`.

- **Syntax:** `HGET key field`
- **Complexity:** $O(1)$
- **Return Value:**
  - Bulk String: Value of the field.
  - Null Bulk String (`$-1\r\n`): If the key or field does not exist.
- **Example:**
  ```text
  127.0.0.1:6379> HGET user:profile name
  "Bob"
  ```

---

### `HGETALL`
Returns all fields and values of the hash stored at `key` as a flat array.

- **Syntax:** `HGETALL key`
- **Complexity:** $O(N)$ where $N$ is the size of the hash.
- **Return Value:**
  - Array: Alternating field names and values `[field1, val1, field2, val2, ...]`. Empty array if key does not exist.
- **Example:**
  ```text
  127.0.0.1:6379> HGETALL user:profile
  1) "name"
  2) "Bob"
  3) "age"
  4) "30"
  5) "role"
  6) "Admin"
  ```

---

## 4. List Commands

### `LPUSH`
Prepends one or multiple values to the head of the list at `key`.

- **Syntax:** `LPUSH key value [value ...]`
- **Complexity:** $O(M)$ where $M$ is the number of values inserted.
- **Return Value:**
  - Integer: Length of the list after the push operation.
- **Example:**
  ```text
  127.0.0.1:6379> LPUSH tasks "task1" "task2"
  (integer) 2
  ```

---

### `RPUSH`
Appends one or multiple values to the tail of the list at `key`.

- **Syntax:** `RPUSH key value [value ...]`
- **Complexity:** $O(M)$ where $M$ is the number of values inserted.
- **Return Value:**
  - Integer: Length of the list after the push operation.
- **Example:**
  ```text
  127.0.0.1:6379> RPUSH tasks "task3"
  (integer) 3
  ```

---

### `LPOP`
Removes and returns the first element of the list stored at `key`.

- **Syntax:** `LPOP key`
- **Complexity:** $O(1)$
- **Return Value:**
  - Bulk String: Value of the removed element.
  - Null Bulk String (`$-1\r\n`): If key does not exist or list is empty.
- **Example:**
  ```text
  127.0.0.1:6379> LPOP tasks
  "task2"
  ```

---

### `RPOP`
Removes and returns the last element of the list stored at `key`.

- **Syntax:** `RPOP key`
- **Complexity:** $O(1)$
- **Return Value:**
  - Bulk String: Value of the removed element.
  - Null Bulk String (`$-1\r\n`): If key does not exist.
- **Example:**
  ```text
  127.0.0.1:6379> RPOP tasks
  "task3"
  ```

---

### `LLEN`
Returns the length of the list stored at `key`.

- **Syntax:** `LLEN key`
- **Complexity:** $O(1)$
- **Return Value:**
  - Integer: Length of the list; `0` if key does not exist.
- **Example:**
  ```text
  127.0.0.1:6379> LLEN tasks
  (integer) 1
  ```

---

## 5. Sorted Set (ZSET) Commands

Implemented via a **32-Level William Pugh SkipList ($p = 0.25$)** coupled with an $O(1)$ Hash Map dictionary. Forward pointers maintain distance spans for logarithmic rank resolution.

### `ZADD`
Adds one or more members with specified scores to the sorted set stored at `key`.

- **Syntax:** `ZADD key score member [score member ...]`
- **Complexity:** $O(\log N)$ per added/updated member.
- **Return Value:**
  - Integer: Number of new elements added.
- **Example:**
  ```text
  127.0.0.1:6379> ZADD leaderboard 100 "PlayerA" 250 "PlayerB" 180 "PlayerC"
  (integer) 3
  ```

---

### `ZSCORE`
Returns the score of `member` in the sorted set at `key`.

- **Syntax:** `ZSCORE key member`
- **Complexity:** $O(1)$ (via dictionary lookup).
- **Return Value:**
  - Bulk String: Floating point score represented as string.
  - Null Bulk String: If member or key does not exist.
- **Example:**
  ```text
  127.0.0.1:6379> ZSCORE leaderboard "PlayerB"
  "250"
  ```

---

### `ZCARD`
Returns the cardinality (number of elements) of the sorted set at `key`.

- **Syntax:** `ZCARD key`
- **Complexity:** $O(1)$
- **Return Value:**
  - Integer: Number of members; `0` if key does not exist.
- **Example:**
  ```text
  127.0.0.1:6379> ZCARD leaderboard
  (integer) 3
  ```

---

### `ZCOUNT`
Counts the members in a sorted set with scores within the given values `[min, max]`. Supports `-inf` and `+inf`.

- **Syntax:** `ZCOUNT key min max`
- **Complexity:** $O(\log N)$
- **Return Value:**
  - Integer: Number of elements in specified score range.
- **Example:**
  ```text
  127.0.0.1:6379> ZCOUNT leaderboard 150 300
  (integer) 2
  127.0.0.1:6379> ZCOUNT leaderboard -inf +inf
  (integer) 3
  ```

---

### `ZRANK`
Returns the 0-based rank of `member` ordered from lowest score to highest score ($O(\log N)$ via SkipList level spans).

- **Syntax:** `ZRANK key member`
- **Complexity:** $O(\log N)$
- **Return Value:**
  - Integer: 0-based rank.
  - Null Bulk String: If member or key is not found.
- **Example:**
  ```text
  127.0.0.1:6379> ZRANK leaderboard "PlayerA"
  (integer) 0
  127.0.0.1:6379> ZRANK leaderboard "PlayerB"
  (integer) 2
  ```

---

### `ZREVRANK`
Returns the 0-based rank of `member` ordered from highest score to lowest score (reverse order).

- **Syntax:** `ZREVRANK key member`
- **Complexity:** $O(\log N)$
- **Return Value:**
  - Integer: 0-based reverse rank.
  - Null Bulk String: If member is not found.
- **Example:**
  ```text
  127.0.0.1:6379> ZREVRANK leaderboard "PlayerB"
  (integer) 0
  ```

---

### `ZRANGE`
Returns the specified range of elements in the sorted set, ordered from lowest to highest score. Supports negative indices and the `WITHSCORES` modifier.

- **Syntax:** `ZRANGE key start stop [WITHSCORES]`
- **Complexity:** $O(\log N + M)$ where $M$ is the number of elements returned.
- **Return Value:**
  - Array of Bulk Strings.
- **Example:**
  ```text
  127.0.0.1:6379> ZRANGE leaderboard 0 -1
  1) "PlayerA"
  2) "PlayerC"
  3) "PlayerB"

  127.0.0.1:6379> ZRANGE leaderboard 0 1 WITHSCORES
  1) "PlayerA"
  2) "100"
  3) "PlayerC"
  4) "180"
  ```

---

### `ZREVRANGE`
Returns the specified range of elements in reverse order (highest score to lowest score).

- **Syntax:** `ZREVRANGE key start stop [WITHSCORES]`
- **Complexity:** $O(\log N + M)$
- **Return Value:**
  - Array of Bulk Strings.
- **Example:**
  ```text
  127.0.0.1:6379> ZREVRANGE leaderboard 0 1 WITHSCORES
  1) "PlayerB"
  2) "250"
  3) "PlayerC"
  4) "180"
  ```

---

### `ZREM`
Removes one or more members from the sorted set at `key`.

- **Syntax:** `ZREM key member [member ...]`
- **Complexity:** $O(M \log N)$ where $M$ is the number of members to remove.
- **Return Value:**
  - Integer: Number of members removed.
- **Example:**
  ```text
  127.0.0.1:6379> ZREM leaderboard "PlayerC"
  (integer) 1
  ```

---

## 6. Bitmap Commands

Bitmaps treat string byte arrays as bit vectors, supporting 0-indexed bit flipping and population counts.

### `SETBIT`
Sets or clears the bit at `offset` in the string value stored at `key`.

- **Syntax:** `SETBIT key offset value`
- **Complexity:** $O(1)$
- **Return Value:**
  - Integer: Original bit value stored at `offset` before the mutation (`0` or `1`).
- **Error Modes:**
  - `-ERR bit offset is not an integer or out of range` if offset is negative.
  - `-ERR bit is not an integer or out of range` if value is neither 0 nor 1.
- **Example:**
  ```text
  127.0.0.1:6379> SETBIT dau:2026-10-02 105 1
  (integer) 0
  127.0.0.1:6379> SETBIT dau:2026-10-02 105 1
  (integer) 1
  ```

---

### `GETBIT`
Returns the bit value at `offset` in the string value stored at `key`.

- **Syntax:** `GETBIT key offset`
- **Complexity:** $O(1)$
- **Return Value:**
  - Integer: `0` or `1`. If offset is beyond string length, returns `0`.
- **Example:**
  ```text
  127.0.0.1:6379> GETBIT dau:2026-10-02 105
  (integer) 1
  127.0.0.1:6379> GETBIT dau:2026-10-02 999
  (integer) 0
  ```

---

### `BITCOUNT`
Counts the number of set bits (population count) in a string. Optional byte index range `[start, end]` supported.

- **Syntax:** `BITCOUNT key [start end]`
- **Complexity:** $O(N)$ where $N$ is the byte length.
- **Return Value:**
  - Integer: Number of bits set to 1.
- **Example:**
  ```text
  127.0.0.1:6379> BITCOUNT dau:2026-10-02
  (integer) 1
  ```

---

## 7. HyperLogLog Cardinality Commands

Probabilistic cardinal estimator utilizing 64 registers with 64-bit hashing, harmonic mean aggregation, $\alpha_m$ bias correction, and small-cardinality linear counting.

### `PFADD`
Adds elements to the HyperLogLog data structure stored at `key`.

- **Syntax:** `PFADD key element [element ...]`
- **Complexity:** $O(1)$ per element.
- **Return Value:**
  - Integer: `:1\r\n` if at least 1 internal register was altered; `:0\r\n` otherwise.
- **Example:**
  ```text
  127.0.0.1:6379> PFADD unique_visitors "192.168.1.1" "10.0.0.1" "192.168.1.1"
  (integer) 1
  ```

---

### `PFCOUNT`
Returns the approximated cardinality of the set(s) observed by the HyperLogLog at `key`.

- **Syntax:** `PFCOUNT key [key ...]`
- **Complexity:** $O(1)$ with low constant factor.
- **Return Value:**
  - Integer: Approximated unique element count.
- **Example:**
  ```text
  127.0.0.1:6379> PFCOUNT unique_visitors
  (integer) 2
  ```

---

## 8. Redis Stream Commands

Append-only event streams with monotonic millisecond IDs (`<msTime>-<seq>`), field-value tuples, and range scanning.

### `XADD`
Appends a new entry to the stream at `key`. Accepts explicit IDs or `*` for automatic timestamp sequence generation.

- **Syntax:** `XADD key ID field value [field value ...]`
- **Complexity:** $O(1)$ amortized.
- **Return Value:**
  - Bulk String: Assigned entry ID (e.g., `1727850000000-0`).
- **Error Modes:**
  - `-ERR The ID specified in XADD is equal or smaller than the target stream top item ID`
- **Example:**
  ```text
  127.0.0.1:6379> XADD sensor_stream * temperature "24.5" humidity "60"
  "1727850000000-0"
  127.0.0.1:6379> XADD sensor_stream 1727850000001-0 temperature "24.8"
  "1727850000001-0"
  ```

---

### `XLEN`
Returns the number of entries in the stream at `key`.

- **Syntax:** `XLEN key`
- **Complexity:** $O(1)$
- **Return Value:**
  - Integer: Number of stream entries; `0` if stream does not exist.
- **Example:**
  ```text
  127.0.0.1:6379> XLEN sensor_stream
  (integer) 2
  ```

---

### `XRANGE`
Queries a range of stream entries between `start` and `end` IDs. Supports `-` (minimum ID), `+` (maximum ID), and optional `COUNT`.

- **Syntax:** `XRANGE key start end [COUNT count]`
- **Complexity:** $O(N)$ where $N$ is the number of entries returned.
- **Return Value:**
  - Array of `[ID, [field1, val1, ...]]` arrays.
- **Example:**
  ```text
  127.0.0.1:6379> XRANGE sensor_stream - + COUNT 2
  1) 1) "1727850000000-0"
     2) 1) "temperature"
        2) "24.5"
        3) "humidity"
        4) "60"
  2) 1) "1727850000001-0"
     2) 1) "temperature"
        2) "24.8"
  ```

---

## 9. ACID & CAS Transaction Commands

Transactions provide atomic isolation without interleaving client commands, coupled with Optimistic Concurrency Control (OCC) via monotonic key version checks.

### `MULTI`
Enters transaction block. Subsequent commands are queued until `EXEC` or `DISCARD` is called.

- **Syntax:** `MULTI`
- **Return Value:**
  - Simple String: `+OK\r\n`
- **Error Modes:**
  - `-ERR MULTI calls can not be nested`

---

### `EXEC`
Executes all queued commands in the transaction block atomically. If any key monitored by `WATCH` was mutated by another client, the transaction cleanly aborts.

- **Syntax:** `EXEC`
- **Return Value:**
  - Array: Results of each queued command.
  - Null Array (`*-1\r\n`): If a watched key was dirtied (CAS failure).
- **Error Modes:**
  - `-ERR EXEC without MULTI`

---

### `DISCARD`
Flushes all queued commands and exits transaction state.

- **Syntax:** `DISCARD`
- **Return Value:**
  - Simple String: `+OK\r\n`
- **Error Modes:**
  - `-ERR DISCARD without MULTI`

---

### `WATCH`
Monitors one or more keys for optimistic concurrency control (CAS).

- **Syntax:** `WATCH key [key ...]`
- **Return Value:**
  - Simple String: `+OK\r\n`
- **Error Modes:**
  - `-ERR WATCH inside MULTI is not allowed`

---

### `UNWATCH`
Flushes all watched keys for the current client connection.

- **Syntax:** `UNWATCH`
- **Return Value:**
  - Simple String: `+OK\r\n`

---

## 10. Pub/Sub Messaging Commands

Supports both literal channel subscriptions and glob wildcard patterns (`*`, `?`, `[abc]`).

### `PUBLISH`
Posts a message to the specified channel.

- **Syntax:** `PUBLISH channel message`
- **Complexity:** $O(N + M)$ where $N$ is subscribers and $M$ is pattern subscribers.
- **Return Value:**
  - Integer: Number of clients that received the message.
- **Example:**
  ```text
  127.0.0.1:6379> PUBLISH news.tech "Java 21 Released"
  (integer) 3
  ```

---

### `SUBSCRIBE`
Subscribes the client to one or more channels. The client enters push-mode.

- **Syntax:** `SUBSCRIBE channel [channel ...]`
- **Return Value:**
  - 3-element RESP push array: `["subscribe", channel, subscription_count]`.

---

### `UNSUBSCRIBE`
Unsubscribes the client from the given channels or all channels if none specified.

- **Syntax:** `UNSUBSCRIBE [channel ...]`
- **Return Value:**
  - 3-element RESP push array: `["unsubscribe", channel, remaining_count]`.

---

### `PSUBSCRIBE`
Subscribes the client to channels matching glob wildcard patterns (e.g. `news.*`, `h?llo`, `user[0-9]`).

- **Syntax:** `PSUBSCRIBE pattern [pattern ...]`
- **Return Value:**
  - 3-element RESP push array: `["psubscribe", pattern, pattern_count]`.
- **Delivered Message Format:**
  - When matching message arrives, client receives 4-element array:
    `["pmessage", pattern, channel, payload]`

---

### `PUNSUBSCRIBE`
Unsubscribes the client from given patterns or all patterns.

- **Syntax:** `PUNSUBSCRIBE [pattern ...]`
- **Return Value:**
  - 3-element RESP push array: `["punsubscribe", pattern, remaining_patterns]`.

---

## 11. Replication Commands

### `REPLICAOF` / `SLAVEOF`
Dynamically configures this server as a replica of another node or promotes it to master.

- **Syntax:**
  - Make replica: `REPLICAOF <host> <port>`
  - Promote to master: `REPLICAOF NO ONE`
- **Return Value:**
  - Simple String: `+OK\r\n`

---

### `PSYNC`
Initiates replication stream resynchronization between master and replica.

- **Syntax:** `PSYNC <master_replid> <offset>`
- **Return Value:**
  - Partial Resync: `+CONTINUE\r\n` (if replid matches and offset is in ring buffer).
  - Full Resync: `+FULLRESYNC <replid> <offset>\r\n`

---

### `REPLCONF`
Replication configuration handshake (`listening-port`, `capa psync2`, `ack`).

- **Syntax:** `REPLCONF <option> <value>`
- **Return Value:**
  - Simple String: `+OK\r\n` (or silent ACK).

---

## 12. Cluster Sharding Commands

Partitions keys across **16,384 virtual slots** using deterministic CRC16-CCITT and `{tag}` hashing.

### `CLUSTER KEYSLOT`
Returns the integer hash slot (0 to 16,383) to which `key` hashes.

- **Syntax:** `CLUSTER KEYSLOT key`
- **Complexity:** $O(K)$ where $K$ is key length.
- **Return Value:**
  - Integer: Slot number between `0` and `16383`.
- **Example:**
  ```text
  127.0.0.1:6379> CLUSTER KEYSLOT user:100
  (integer) 7365
  127.0.0.1:6379> CLUSTER KEYSLOT {user}:profile
  (integer) 5474
  ```

---

### `CLUSTER SLOTS`
Returns the slot-to-node topology mapping.

- **Syntax:** `CLUSTER SLOTS`
- **Return Value:**
  - Nested array detailing slot ranges and host/port endpoints.

---

### `CLUSTER NODES`
Returns cluster configuration and node states formatted according to the Redis Cluster specification.

- **Syntax:** `CLUSTER NODES`
- **Return Value:**
  - Bulk String with node IDs, addresses, roles, and slot allocations.

---

## 13. Persistence Commands

### `SAVE`
Synchronously writes a binary snapshot (`dump.rdb`) of the database to disk.

- **Syntax:** `SAVE`
- **Return Value:**
  - Simple String: `+OK\r\n`

---

### `BGSAVE`
Spawns an asynchronous background worker thread to serialize the database snapshot to disk.

- **Syntax:** `BGSAVE`
- **Return Value:**
  - Simple String: `+Background saving started\r\n`

---

### `BGREWRITEAOF`
Initiates an asynchronous Append-Only File rewrite. Iterates the keyspace snapshot, emits minimal state commands (`SET`, `HSET`, `RPUSH`, `ZADD`), and performs an atomic file replacement (`ATOMIC_MOVE`).

- **Syntax:** `BGREWRITEAOF`
- **Return Value:**
  - Simple String: `+Background append only file rewriting started\r\n`

---

## 14. Utility & Telemetry Commands

### `PING`
Tests connection liveliness.

- **Syntax:** `PING [message]`
- **Return Value:**
  - `+PONG\r\n` if no argument.
  - `$length\r\nmessage\r\n` if argument provided.

---

### `ECHO`
Returns the supplied argument string.

- **Syntax:** `ECHO message`
- **Return Value:**
  - Bulk String containing the message.

---

### `INFO`
Returns comprehensive operational diagnostics, memory allocations, replication metrics, and keyspace statistics.

- **Syntax:** `INFO`
- **Return Value:**
  - Bulk String formatted in standard `# Section\r\nkey:value\r\n` Redis format.

---

### `DBSIZE`
Returns the total number of keys stored in the database.

- **Syntax:** `DBSIZE`
- **Return Value:**
  - Integer: Key count.

---

### `FLUSHDB` / `FLUSHALL`
Purges all keys from the current database.

- **Syntax:** `FLUSHDB` or `FLUSHALL`
- **Return Value:**
  - Simple String: `+OK\r\n`

---

### `AUTH`
Authenticates client connection (compatible with standard clients).

- **Syntax:** `AUTH [username] password`
- **Return Value:**
  - Simple String: `+OK\r\n`

---

### `COMMAND`
Returns server command metadata (sent by modern `redis-cli` upon connection initialization).

- **Syntax:** `COMMAND [DOCS]`
- **Return Value:**
  - Array: Command descriptions.

---

### `QUIT`
Closes the client connection gracefully.

- **Syntax:** `QUIT`
- **Return Value:**
  - Simple String: `+OK\r\n` (then socket disconnects).
