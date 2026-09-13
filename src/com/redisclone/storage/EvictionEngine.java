package com.redisclone.storage;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Background eviction daemon running active expiration cycles at 10Hz (every 100ms),
 * mirroring Redis serverCron() activeExpireCycle.
 */
public class EvictionEngine {

    private static final Logger LOGGER = Logger.getLogger(EvictionEngine.class.getName());
    private final DataStore dataStore;
    private final ScheduledExecutorService scheduler;
    private volatile boolean running = false;

    public EvictionEngine(DataStore dataStore) {
        this.dataStore = dataStore;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "redis-active-eviction-cron");
            t.setDaemon(true);
            return t;
        });
    }

    public synchronized void start() {
        if (running) return;
        running = true;
        // Schedule active expiration cycle every 100ms (10Hz)
        scheduler.scheduleAtFixedRate(() -> {
            try {
                int evicted = dataStore.activeExpireCycle(20, 16);
                if (evicted > 0 && LOGGER.isLoggable(Level.FINE)) {
                    LOGGER.fine("Active eviction cycle pruned " + evicted + " expired keys");
                }
            } catch (Throwable t) {
                LOGGER.log(Level.SEVERE, "Error in active eviction cycle", t);
            }
        }, 100, 100, TimeUnit.MILLISECONDS);
    }

    public synchronized void stop() {
        running = false;
        scheduler.shutdownNow();
    }
}
