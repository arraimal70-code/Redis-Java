package com.redisclone.replication;

import java.security.SecureRandom;
import java.util.HexFormat;

/**
 * High-performance circular ring buffer representing the Redis replication backlog.
 * Stores recent write stream bytes alongside a monotonically increasing 64-bit master replication offset.
 * Enables PSYNC partial resynchronization when replicas reconnect after transient network disconnections.
 */
public class ReplicationBacklog {

    private final byte[] buffer;
    private final int capacity;
    private long masterOffset = 0;
    private final String masterReplId;

    public ReplicationBacklog(int capacity) {
        this.capacity = capacity;
        this.buffer = new byte[capacity];
        this.masterReplId = generateReplId();
    }

    private static String generateReplId() {
        byte[] bytes = new byte[20];
        new SecureRandom().nextBytes(bytes);
        return HexFormat.of().formatHex(bytes);
    }

    public String getMasterReplId() {
        return masterReplId;
    }

    public synchronized long getMasterOffset() {
        return masterOffset;
    }

    public synchronized void write(byte[] data) {
        if (data == null || data.length == 0) return;

        for (byte b : data) {
            int index = (int) (masterOffset % capacity);
            buffer[index] = b;
            masterOffset++;
        }
    }

    public synchronized boolean canPartiallyResync(String requestedReplId, long requestedOffset) {
        if (!this.masterReplId.equals(requestedReplId)) {
            return false;
        }
        if (requestedOffset > masterOffset) {
            return false;
        }
        long earliestOffset = Math.max(0, masterOffset - capacity);
        return requestedOffset >= earliestOffset;
    }

    public synchronized byte[] getBytesFromOffset(long fromOffset) {
        if (fromOffset < 0 || fromOffset >= masterOffset) {
            return new byte[0];
        }

        int length = (int) (masterOffset - fromOffset);
        if (length > capacity) {
            length = capacity;
            fromOffset = masterOffset - capacity;
        }

        byte[] result = new byte[length];
        for (int i = 0; i < length; i++) {
            int index = (int) ((fromOffset + i) % capacity);
            result[i] = buffer[index];
        }
        return result;
    }
}
