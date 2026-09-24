package com.redisclone.server;

import java.util.concurrent.atomic.AtomicLong;

/**
 * High-performance telemetry and runtime metrics collector for Redis Server.
 * Exposes live operational stats mirroring standard Redis INFO telemetry:
 * uptime, connections, command throughput, hit/miss ratios, and eviction counts.
 */
public class ServerMetrics {

    private static final ServerMetrics INSTANCE = new ServerMetrics();

    private final long startTimeMs = System.currentTimeMillis();
    private final AtomicLong totalConnectionsReceived = new AtomicLong(0);
    private final AtomicLong totalCommandsProcessed = new AtomicLong(0);
    private final AtomicLong totalCommandErrors = new AtomicLong(0);
    private final AtomicLong keyspaceHits = new AtomicLong(0);
    private final AtomicLong keyspaceMisses = new AtomicLong(0);
    private final AtomicLong expiredKeys = new AtomicLong(0);
    private final AtomicLong evictedKeys = new AtomicLong(0);
    private final AtomicLong totalNetInputBytes = new AtomicLong(0);
    private final AtomicLong totalNetOutputBytes = new AtomicLong(0);

    public static ServerMetrics getInstance() {
        return INSTANCE;
    }

    public long getUptimeSeconds() {
        return Math.max(0, (System.currentTimeMillis() - startTimeMs) / 1000L);
    }

    public void recordConnection() {
        totalConnectionsReceived.incrementAndGet();
    }

    public void recordCommand() {
        totalCommandsProcessed.incrementAndGet();
    }

    public void recordCommandError() {
        totalCommandErrors.incrementAndGet();
    }

    public void recordHit() {
        keyspaceHits.incrementAndGet();
    }

    public void recordMiss() {
        keyspaceMisses.incrementAndGet();
    }

    public void recordExpiredKey() {
        expiredKeys.incrementAndGet();
    }

    public void recordExpiredKeys(int count) {
        expiredKeys.addAndGet(count);
    }

    public void recordEvictedKey() {
        evictedKeys.incrementAndGet();
    }

    public void recordNetInput(long bytes) {
        totalNetInputBytes.addAndGet(bytes);
    }

    public void recordNetOutput(long bytes) {
        totalNetOutputBytes.addAndGet(bytes);
    }

    // --- Getters ---

    public long getTotalConnectionsReceived() {
        return totalConnectionsReceived.get();
    }

    public long getTotalCommandsProcessed() {
        return totalCommandsProcessed.get();
    }

    public long getTotalCommandErrors() {
        return totalCommandErrors.get();
    }

    public long getKeyspaceHits() {
        return keyspaceHits.get();
    }

    public long getKeyspaceMisses() {
        return keyspaceMisses.get();
    }

    public long getExpiredKeys() {
        return expiredKeys.get();
    }

    public long getEvictedKeys() {
        return evictedKeys.get();
    }

    public long getTotalNetInputBytes() {
        return totalNetInputBytes.get();
    }

    public long getTotalNetOutputBytes() {
        return totalNetOutputBytes.get();
    }

    public double getHitRatio() {
        long hits = keyspaceHits.get();
        long total = hits + keyspaceMisses.get();
        return total == 0 ? 0.0 : (double) hits / total;
    }
}
