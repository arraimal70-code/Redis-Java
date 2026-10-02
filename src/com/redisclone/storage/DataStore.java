package com.redisclone.storage;

import com.redisclone.storage.eviction.EvictionPolicy;
import com.redisclone.storage.eviction.LruEvictionPolicy;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * High-performance in-memory key-value dictionary engine.
 * Supports typed RedisObjects, passive (lazy) TTL eviction, active
 * probabilistic expiration cycles (Redis expire.c), pluggable LRU eviction,
 * and key version tracking for ACID/CAS transactions (WATCH/EXEC).
 */
public class DataStore {

    // Primary dictionary mapping keys to Redis objects
    private final ConcurrentHashMap<String, RedisObject> db = new ConcurrentHashMap<>();

    // Secondary dictionary tracking expiration timestamps (epoch milliseconds)
    private final ConcurrentHashMap<String, Long> expires = new ConcurrentHashMap<>();

    // Key version tracking for optimistic concurrency control (WATCH command)
    private final ConcurrentHashMap<String, Long> keyVersions = new ConcurrentHashMap<>();
    private final AtomicLong globalVersionGen = new AtomicLong(1);

    // Eviction settings
    private volatile int maxKeys = 0; // 0 = unlimited
    private volatile EvictionPolicy evictionPolicy;

    public DataStore() {
        this(0, LruEvictionPolicy.allKeysLru());
    }

    public DataStore(int maxKeys, EvictionPolicy evictionPolicy) {
        this.maxKeys = maxKeys;
        this.evictionPolicy = evictionPolicy != null ? evictionPolicy : LruEvictionPolicy.allKeysLru();
    }

    public int getMaxKeys() {
        return maxKeys;
    }

    public void setMaxKeys(int maxKeys) {
        this.maxKeys = maxKeys;
    }

    public EvictionPolicy getEvictionPolicy() {
        return evictionPolicy;
    }

    public void setEvictionPolicy(EvictionPolicy evictionPolicy) {
        this.evictionPolicy = evictionPolicy;
    }

    public long getKeyVersion(String key) {
        return keyVersions.getOrDefault(key, 0L);
    }

    private void bumpKeyVersion(String key) {
        keyVersions.put(key, globalVersionGen.incrementAndGet());
    }

    /**
     * Ensures space is available if a maxKeys cap is enforced.
     */
    private synchronized void ensureCapacity() {
        if (maxKeys <= 0 || evictionPolicy == null) {
            return;
        }

        while (db.size() >= maxKeys) {
            String evictedKey = evictionPolicy.evictKey();
            if (evictedKey == null) {
                // If policy cannot evict (e.g. noeviction or empty), abort
                break;
            }
            db.remove(evictedKey);
            expires.remove(evictedKey);
            bumpKeyVersion(evictedKey);
            com.redisclone.server.ServerMetrics.getInstance().recordEvictedKey();
        }
    }

    /**
     * Passive (lazy) eviction check.
     * Evaluates key expiration on-demand during lookup.
     *
     * @return true if key was expired and evicted, false otherwise
     */
    public boolean checkAndExpire(String key) {
        Long expireTime = expires.get(key);
        if (expireTime != null && System.currentTimeMillis() >= expireTime) {
            // Key has expired; purge atomically
            expires.remove(key);
            db.remove(key);
            if (evictionPolicy != null) {
                evictionPolicy.onKeyDelete(key);
            }
            bumpKeyVersion(key);
            com.redisclone.server.ServerMetrics.getInstance().recordExpiredKey();
            return true;
        }
        return false;
    }

    /**
     * Active probabilistic expiration cycle (runs periodically via background timer).
     * Algorithm (Redis activeExpireCycle):
     * 1. Sample up to 20 random keys with an expiration.
     * 2. Evict any sampled keys that have passed their TTL.
     * 3. If >25% (>5 keys) were expired, repeat the cycle immediately up to maxIterations
     *    or time threshold (10ms) to rapidly reclaim memory during expiration bursts.
     */
    public int activeExpireCycle(int maxKeysToSample, int maxIterations) {
        if (expires.isEmpty()) {
            return 0;
        }

        int totalEvicted = 0;
        long startTime = System.currentTimeMillis();
        int iteration = 0;

        while (iteration++ < maxIterations && (System.currentTimeMillis() - startTime) < 10) {
            List<String> sampledKeys = sampleRandomExpiryKeys(maxKeysToSample);
            if (sampledKeys.isEmpty()) {
                break;
            }

            int expiredCount = 0;
            long now = System.currentTimeMillis();

            for (String key : sampledKeys) {
                Long expireTime = expires.get(key);
                if (expireTime != null && now >= expireTime) {
                    expires.remove(key);
                    db.remove(key);
                    if (evictionPolicy != null) {
                        evictionPolicy.onKeyDelete(key);
                    }
                    bumpKeyVersion(key);
                    expiredCount++;
                    totalEvicted++;
                }
            }

            // If less than 25% expired, the sampling density is sufficiently low to stop
            if (expiredCount <= (sampledKeys.size() / 4)) {
                break;
            }
        }

        if (totalEvicted > 0) {
            com.redisclone.server.ServerMetrics.getInstance().recordExpiredKeys(totalEvicted);
        }
        return totalEvicted;
    }

    private List<String> sampleRandomExpiryKeys(int count) {
        if (expires.isEmpty()) return Collections.emptyList();

        List<String> sampled = new ArrayList<>(Math.min(count, expires.size()));
        Iterator<String> it = expires.keySet().iterator();

        int size = expires.size();
        int step = Math.max(1, size / Math.max(1, count * 2));
        int current = 0;

        while (it.hasNext() && sampled.size() < count) {
            String key = it.next();
            if (current++ % step == 0) {
                sampled.add(key);
            }
        }

        // If step skipped too many and sample is under capacity, drain remaining
        while (it.hasNext() && sampled.size() < count) {
            sampled.add(it.next());
        }
        return sampled;
    }

    // --- Key & Expiry Methods ---

    public boolean exists(String key) {
        if (checkAndExpire(key)) return false;
        boolean exists = db.containsKey(key);
        if (exists && evictionPolicy != null) {
            evictionPolicy.onKeyAccess(key);
        }
        return exists;
    }

    public RedisObject get(String key) {
        if (checkAndExpire(key)) {
            com.redisclone.server.ServerMetrics.getInstance().recordMiss();
            return null;
        }
        RedisObject obj = db.get(key);
        if (obj != null) {
            if (evictionPolicy != null) {
                evictionPolicy.onKeyAccess(key);
            }
            com.redisclone.server.ServerMetrics.getInstance().recordHit();
        } else {
            com.redisclone.server.ServerMetrics.getInstance().recordMiss();
        }
        return obj;
    }

    public void set(String key, RedisObject value) {
        set(key, value, null);
    }

    public void set(String key, RedisObject value, Long ttlMillis) {
        if (!db.containsKey(key)) {
            ensureCapacity();
        }

        db.put(key, value);
        if (ttlMillis != null && ttlMillis > 0) {
            expires.put(key, System.currentTimeMillis() + ttlMillis);
        } else {
            expires.remove(key);
        }

        if (evictionPolicy != null) {
            evictionPolicy.onKeyInsert(key);
        }
        bumpKeyVersion(key);
    }

    public int del(String... keys) {
        int deleted = 0;
        for (String key : keys) {
            expires.remove(key);
            if (db.remove(key) != null) {
                if (evictionPolicy != null) {
                    evictionPolicy.onKeyDelete(key);
                }
                bumpKeyVersion(key);
                deleted++;
            }
        }
        return deleted;
    }

    public boolean expire(String key, long seconds) {
        if (checkAndExpire(key) || !db.containsKey(key)) {
            return false;
        }
        if (seconds <= 0) {
            del(key);
            return true;
        }
        expires.put(key, System.currentTimeMillis() + (seconds * 1000L));
        bumpKeyVersion(key);
        return true;
    }

    public boolean pexpire(String key, long millis) {
        if (checkAndExpire(key) || !db.containsKey(key)) {
            return false;
        }
        if (millis <= 0) {
            del(key);
            return true;
        }
        expires.put(key, System.currentTimeMillis() + millis);
        bumpKeyVersion(key);
        return true;
    }

    public long ttl(String key) {
        if (checkAndExpire(key) || !db.containsKey(key)) {
            return -2;
        }
        Long expireTime = expires.get(key);
        if (expireTime == null) {
            return -1;
        }
        long remainingMs = expireTime - System.currentTimeMillis();
        return Math.max(0, remainingMs / 1000L);
    }

    // --- String Commands ---

    public long incr(String key) {
        if (checkAndExpire(key)) {
            db.remove(key);
            expires.remove(key);
        }

        RedisObject obj = db.get(key);
        if (obj == null) {
            ensureCapacity();
            long initialVal = 1;
            db.put(key, RedisObject.ofString(Long.toString(initialVal)));
            if (evictionPolicy != null) {
                evictionPolicy.onKeyInsert(key);
            }
            bumpKeyVersion(key);
            return initialVal;
        }

        String strVal = obj.asUtf8String();
        try {
            long val = Long.parseLong(strVal);
            val++;
            obj.setValue(Long.toString(val).getBytes(StandardCharsets.UTF_8));
            if (evictionPolicy != null) {
                evictionPolicy.onKeyAccess(key);
            }
            bumpKeyVersion(key);
            return val;
        } catch (NumberFormatException e) {
            throw new IllegalStateException("ERR value is not an integer or out of range");
        }
    }

    // --- Hash Commands ---

    public int hset(String key, String field, byte[] value) {
        if (checkAndExpire(key)) {
            db.remove(key);
            expires.remove(key);
        }

        if (!db.containsKey(key)) {
            ensureCapacity();
        }

        RedisObject obj = db.computeIfAbsent(key, k -> RedisObject.ofHash());
        Map<String, byte[]> hash = obj.asHash();
        boolean isNew = !hash.containsKey(field);
        hash.put(field, value);

        if (evictionPolicy != null) {
            evictionPolicy.onKeyInsert(key);
        }
        bumpKeyVersion(key);
        return isNew ? 1 : 0;
    }

    public byte[] hget(String key, String field) {
        if (checkAndExpire(key)) return null;
        RedisObject obj = db.get(key);
        if (obj == null) return null;
        if (evictionPolicy != null) {
            evictionPolicy.onKeyAccess(key);
        }
        return obj.asHash().get(field);
    }

    // --- List Commands ---

    public int lpush(String key, byte[]... values) {
        if (checkAndExpire(key)) {
            db.remove(key);
            expires.remove(key);
        }

        if (!db.containsKey(key)) {
            ensureCapacity();
        }

        RedisObject obj = db.computeIfAbsent(key, k -> RedisObject.ofList());
        Deque<byte[]> list = obj.asList();
        for (byte[] val : values) {
            list.addFirst(val);
        }

        if (evictionPolicy != null) {
            evictionPolicy.onKeyInsert(key);
        }
        bumpKeyVersion(key);
        return list.size();
    }

    public byte[] lpop(String key) {
        if (checkAndExpire(key)) return null;
        RedisObject obj = db.get(key);
        if (obj == null) return null;
        Deque<byte[]> list = obj.asList();
        byte[] val = list.pollFirst();
        if (evictionPolicy != null) {
            evictionPolicy.onKeyAccess(key);
        }
        bumpKeyVersion(key);
        return val;
    }

    public int rpush(String key, byte[]... values) {
        if (checkAndExpire(key)) {
            db.remove(key);
            expires.remove(key);
        }

        if (!db.containsKey(key)) {
            ensureCapacity();
        }

        RedisObject obj = db.computeIfAbsent(key, k -> RedisObject.ofList());
        Deque<byte[]> list = obj.asList();
        for (byte[] val : values) {
            list.addLast(val);
        }

        if (evictionPolicy != null) {
            evictionPolicy.onKeyInsert(key);
        }
        bumpKeyVersion(key);
        return list.size();
    }

    public byte[] rpop(String key) {
        if (checkAndExpire(key)) return null;
        RedisObject obj = db.get(key);
        if (obj == null) return null;
        Deque<byte[]> list = obj.asList();
        byte[] val = list.pollLast();
        if (evictionPolicy != null) {
            evictionPolicy.onKeyAccess(key);
        }
        bumpKeyVersion(key);
        return val;
    }

    // --- Bitmap Operations ---

    public int setbit(String key, int offset, int bitValue) {
        if (checkAndExpire(key)) {
            db.remove(key);
            expires.remove(key);
        }
        int byteIndex = offset / 8;
        int bitOffset = 7 - (offset % 8);

        RedisObject obj = db.get(key);
        byte[] bytes;
        if (obj == null || obj.getType() != RedisType.STRING) {
            bytes = new byte[byteIndex + 1];
            obj = RedisObject.ofString(bytes);
            db.put(key, obj);
        } else {
            byte[] existing = obj.asStringBytes();
            if (byteIndex >= existing.length) {
                bytes = Arrays.copyOf(existing, byteIndex + 1);
                obj.setValue(bytes);
            } else {
                bytes = existing;
            }
        }

        int oldBit = (bytes[byteIndex] >> bitOffset) & 1;
        if (bitValue == 1) {
            bytes[byteIndex] = (byte) (bytes[byteIndex] | (1 << bitOffset));
        } else {
            bytes[byteIndex] = (byte) (bytes[byteIndex] & ~(1 << bitOffset));
        }

        if (evictionPolicy != null) evictionPolicy.onKeyInsert(key);
        bumpKeyVersion(key);
        return oldBit;
    }

    public int getbit(String key, int offset) {
        if (checkAndExpire(key)) return 0;
        RedisObject obj = db.get(key);
        if (obj == null || obj.getType() != RedisType.STRING) return 0;
        byte[] bytes = obj.asStringBytes();
        int byteIndex = offset / 8;
        if (byteIndex >= bytes.length) return 0;
        int bitOffset = 7 - (offset % 8);
        return (bytes[byteIndex] >> bitOffset) & 1;
    }

    public long bitcount(String key, int start, int end) {
        if (checkAndExpire(key)) return 0;
        RedisObject obj = db.get(key);
        if (obj == null || obj.getType() != RedisType.STRING) return 0;
        byte[] bytes = obj.asStringBytes();
        if (bytes.length == 0) return 0;

        int len = bytes.length;
        if (start < 0) start = Math.max(0, len + start);
        if (end < 0) end = len + end;
        if (start >= len || start > end) return 0;
        end = Math.min(len - 1, end);

        long count = 0;
        for (int i = start; i <= end; i++) {
            count += Integer.bitCount(bytes[i] & 0xFF);
        }
        return count;
    }

    // --- HyperLogLog Operations ---

    public boolean pfadd(String key, List<byte[]> elements) {
        if (checkAndExpire(key)) {
            db.remove(key);
            expires.remove(key);
        }
        if (!db.containsKey(key)) {
            ensureCapacity();
        }
        RedisObject obj = db.get(key);
        HyperLogLog hll;
        if (obj == null || obj.getType() != RedisType.STRING) {
            hll = new HyperLogLog();
            obj = RedisObject.ofString(hll.getBytes());
            db.put(key, obj);
        } else {
            hll = new HyperLogLog(obj.asStringBytes());
        }

        boolean updated = false;
        for (byte[] el : elements) {
            if (hll.add(el)) {
                updated = true;
            }
        }
        if (updated) {
            obj.setValue(hll.getBytes());
            bumpKeyVersion(key);
            if (evictionPolicy != null) evictionPolicy.onKeyInsert(key);
        }
        return updated;
    }

    public long pfcount(List<String> keys) {
        HyperLogLog merged = new HyperLogLog();
        for (String k : keys) {
            if (checkAndExpire(k)) {
                db.remove(k);
                expires.remove(k);
                continue;
            }
            RedisObject obj = db.get(k);
            if (obj != null && obj.getType() == RedisType.STRING) {
                merged.merge(new HyperLogLog(obj.asStringBytes()));
            }
        }
        return merged.estimate();
    }

    // --- Stream Operations ---

    public synchronized String xadd(String key, String idSpec, List<byte[]> fields) {
        if (checkAndExpire(key)) {
            db.remove(key);
            expires.remove(key);
        }
        if (!db.containsKey(key)) {
            ensureCapacity();
        }
        RedisObject obj = db.computeIfAbsent(key, k -> RedisObject.ofStream());
        if (obj.getType() != RedisType.STREAM) {
            throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        List<StreamEntry> stream = obj.asStream();

        long now = System.currentTimeMillis();
        long ts;
        long seq;

        if ("*".equals(idSpec)) {
            ts = now;
            long lastSeq = -1;
            if (!stream.isEmpty()) {
                StreamEntry last = stream.get(stream.size() - 1);
                if (last.timestampMs() == ts) {
                    lastSeq = last.sequence();
                } else if (last.timestampMs() > ts) {
                    ts = last.timestampMs();
                    lastSeq = last.sequence();
                }
            }
            seq = lastSeq + 1;
        } else {
            String[] parts = idSpec.split("-");
            ts = Long.parseLong(parts[0]);
            if (parts.length > 1 && "*".equals(parts[1])) {
                long lastSeq = -1;
                if (!stream.isEmpty()) {
                    StreamEntry last = stream.get(stream.size() - 1);
                    if (last.timestampMs() == ts) {
                        lastSeq = last.sequence();
                    }
                }
                seq = lastSeq + 1;
            } else {
                seq = parts.length > 1 ? Long.parseLong(parts[1]) : 0L;
            }
        }

        // Enforce monotonic ID validation
        if (!stream.isEmpty()) {
            StreamEntry last = stream.get(stream.size() - 1);
            if (ts < last.timestampMs() || (ts == last.timestampMs() && seq <= last.sequence())) {
                throw new IllegalArgumentException("ERR The ID specified in XADD is equal or smaller than the target stream top item");
            }
        }

        String finalId = ts + "-" + seq;
        StreamEntry entry = new StreamEntry(finalId, ts, seq, fields);
        stream.add(entry);

        if (evictionPolicy != null) evictionPolicy.onKeyInsert(key);
        bumpKeyVersion(key);
        return finalId;
    }

    public int xlen(String key) {
        if (checkAndExpire(key)) return 0;
        RedisObject obj = db.get(key);
        if (obj == null) return 0;
        if (obj.getType() != RedisType.STREAM) {
            throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return obj.asStream().size();
    }

    public List<StreamEntry> xrange(String key, String start, String end, int count) {
        if (checkAndExpire(key)) return Collections.emptyList();
        RedisObject obj = db.get(key);
        if (obj == null) return Collections.emptyList();
        if (obj.getType() != RedisType.STREAM) {
            throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        List<StreamEntry> stream = obj.asStream();
        List<StreamEntry> result = new ArrayList<>();

        for (StreamEntry entry : stream) {
            if (!"-".equals(start) && compareStreamId(entry.id(), start) < 0) {
                continue;
            }
            if (!"+".equals(end) && compareStreamId(entry.id(), end) > 0) {
                break;
            }
            result.add(entry);
            if (count > 0 && result.size() >= count) break;
        }
        return result;
    }

    public static int compareStreamId(String id1, String id2) {
        String[] p1 = id1.split("-");
        String[] p2 = id2.split("-");
        long t1 = Long.parseLong(p1[0]);
        long t2 = Long.parseLong(p2[0]);
        if (t1 != t2) return Long.compare(t1, t2);
        long s1 = p1.length > 1 ? Long.parseLong(p1[1]) : 0;
        long s2 = p2.length > 1 ? Long.parseLong(p2[1]) : 0;
        return Long.compare(s1, s2);
    }

    // --- Sorted Set (ZSET) Operations ---

    public synchronized int zadd(String key, Map<String, Double> scoreMembers) {
        if (checkAndExpire(key)) {
            db.remove(key);
            expires.remove(key);
        }
        if (!db.containsKey(key)) {
            ensureCapacity();
        }
        RedisObject obj = db.computeIfAbsent(key, k -> RedisObject.ofZSet());
        if (obj.getType() != RedisType.ZSET) {
            throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        SortedSet zset = obj.asZSet();
        int added = 0;
        for (Map.Entry<String, Double> entry : scoreMembers.entrySet()) {
            added += zset.add(entry.getKey(), entry.getValue());
        }
        if (evictionPolicy != null) evictionPolicy.onKeyInsert(key);
        bumpKeyVersion(key);
        return added;
    }

    public Double zscore(String key, String member) {
        if (checkAndExpire(key)) return null;
        RedisObject obj = db.get(key);
        if (obj == null) return null;
        if (obj.getType() != RedisType.ZSET) {
            throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return obj.asZSet().score(member);
    }

    public long zcard(String key) {
        if (checkAndExpire(key)) return 0;
        RedisObject obj = db.get(key);
        if (obj == null) return 0;
        if (obj.getType() != RedisType.ZSET) {
            throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return obj.asZSet().card();
    }

    public long zrank(String key, String member) {
        if (checkAndExpire(key)) return -1;
        RedisObject obj = db.get(key);
        if (obj == null) return -1;
        if (obj.getType() != RedisType.ZSET) {
            throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return obj.asZSet().rank(member);
    }

    public long zrevrank(String key, String member) {
        if (checkAndExpire(key)) return -1;
        RedisObject obj = db.get(key);
        if (obj == null) return -1;
        if (obj.getType() != RedisType.ZSET) {
            throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return obj.asZSet().revrank(member);
    }

    public long zcount(String key, double min, double max) {
        if (checkAndExpire(key)) return 0;
        RedisObject obj = db.get(key);
        if (obj == null) return 0;
        if (obj.getType() != RedisType.ZSET) {
            throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return obj.asZSet().count(min, max);
    }

    public List<String> zrange(String key, long start, long stop, boolean withScores) {
        if (checkAndExpire(key)) return Collections.emptyList();
        RedisObject obj = db.get(key);
        if (obj == null) return Collections.emptyList();
        if (obj.getType() != RedisType.ZSET) {
            throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return obj.asZSet().range(start, stop, withScores);
    }

    public List<String> zrevrange(String key, long start, long stop, boolean withScores) {
        if (checkAndExpire(key)) return Collections.emptyList();
        RedisObject obj = db.get(key);
        if (obj == null) return Collections.emptyList();
        if (obj.getType() != RedisType.ZSET) {
            throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return obj.asZSet().revrange(start, stop, withScores);
    }

    public synchronized int zrem(String key, List<String> members) {
        if (checkAndExpire(key)) return 0;
        RedisObject obj = db.get(key);
        if (obj == null) return 0;
        if (obj.getType() != RedisType.ZSET) {
            throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        SortedSet zset = obj.asZSet();
        int removed = 0;
        for (String m : members) {
            if (zset.remove(m)) {
                removed++;
            }
        }
        if (removed > 0) {
            bumpKeyVersion(key);
        }
        return removed;
    }

    // --- Inspection, Telemetry & Persistence Helpers ---

    public int keyCount() {
        return db.size();
    }

    public Map<String, RedisObject> getRawDbSnapshot() {
        return Collections.unmodifiableMap(new HashMap<>(db));
    }

    public Map<String, Long> getRawExpiresSnapshot() {
        return Collections.unmodifiableMap(new HashMap<>(expires));
    }

    public void clear() {
        db.clear();
        expires.clear();
        keyVersions.clear();
    }

    public void restoreKey(String key, RedisObject obj, Long expireTimeMs) {
        db.put(key, obj);
        if (expireTimeMs != null && expireTimeMs > System.currentTimeMillis()) {
            expires.put(key, expireTimeMs);
        }
        if (evictionPolicy != null) {
            evictionPolicy.onKeyInsert(key);
        }
        bumpKeyVersion(key);
    }

    /**
     * Approximates memory usage in bytes of current data stored in keys and values.
     */
    public long estimateMemoryBytes() {
        long bytes = 0;
        for (Map.Entry<String, RedisObject> entry : db.entrySet()) {
            bytes += entry.getKey().length() * 2L + 32; // Key String overhead
            RedisObject obj = entry.getValue();
            bytes += 24; // Object overhead
            switch (obj.getType()) {
                case STRING -> bytes += obj.asStringBytes().length;
                case LIST -> {
                    for (byte[] item : obj.asList()) {
                        bytes += item.length + 16;
                    }
                }
                case HASH -> {
                    for (Map.Entry<String, byte[]> hEntry : obj.asHash().entrySet()) {
                        bytes += hEntry.getKey().length() * 2L + hEntry.getValue().length + 32;
                    }
                }
                default -> bytes += 64;
            }
        }
        return bytes;
    }
}
