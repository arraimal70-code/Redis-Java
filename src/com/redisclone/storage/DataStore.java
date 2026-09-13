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

        return totalEvicted;
    }

    private List<String> sampleRandomExpiryKeys(int count) {
        int size = expires.size();
        if (size == 0) return Collections.emptyList();

        List<String> allKeys = new ArrayList<>(expires.keySet());
        if (allKeys.size() <= count) {
            return allKeys;
        }

        List<String> sampled = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int idx = ThreadLocalRandom.current().nextInt(allKeys.size());
            sampled.add(allKeys.get(idx));
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
        if (checkAndExpire(key)) return null;
        RedisObject obj = db.get(key);
        if (obj != null && evictionPolicy != null) {
            evictionPolicy.onKeyAccess(key);
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
