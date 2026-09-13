package com.redisclone.storage;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * In-memory representation of a Redis value object.
 * Mirrors Redis's robj struct (type, encoding, ptr, lru).
 */
public class RedisObject {

    private final RedisType type;
    private Object value;
    private long lruClock;

    public RedisObject(RedisType type, Object value) {
        this.type = type;
        this.value = value;
        this.lruClock = System.currentTimeMillis();
    }

    public RedisType getType() {
        return type;
    }

    public Object getValue() {
        this.lruClock = System.currentTimeMillis();
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
        this.lruClock = System.currentTimeMillis();
    }

    public long getLruClock() {
        return lruClock;
    }

    // --- Helpers for STRING ---
    public static RedisObject ofString(byte[] data) {
        return new RedisObject(RedisType.STRING, data);
    }

    public static RedisObject ofString(String text) {
        return new RedisObject(RedisType.STRING, text.getBytes(StandardCharsets.UTF_8));
    }

    public byte[] asStringBytes() {
        if (type != RedisType.STRING) {
            throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return (byte[]) value;
    }

    public String asUtf8String() {
        byte[] bytes = asStringBytes();
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }

    // --- Helpers for LIST ---
    public static RedisObject ofList() {
        return new RedisObject(RedisType.LIST, new ArrayDeque<byte[]>());
    }

    @SuppressWarnings("unchecked")
    public Deque<byte[]> asList() {
        if (type != RedisType.LIST) {
            throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return (Deque<byte[]>) value;
    }

    // --- Helpers for HASH ---
    public static RedisObject ofHash() {
        return new RedisObject(RedisType.HASH, new HashMap<String, byte[]>());
    }

    @SuppressWarnings("unchecked")
    public Map<String, byte[]> asHash() {
        if (type != RedisType.HASH) {
            throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return (Map<String, byte[]>) value;
    }
}
