package com.redisclone.command;

import com.redisclone.resp.RespFrame;
import com.redisclone.storage.DataStore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tracks the transactional execution state of an individual client connection.
 * Supports MULTI/EXEC queuing and optimistic concurrency control (CAS) via WATCH.
 */
public class TransactionContext {

    private boolean inTransaction = false;
    private final List<RespFrame.Array> queuedCommands = new ArrayList<>();
    private final Map<String, Long> watchedKeys = new HashMap<>();

    public boolean isInTransaction() {
        return inTransaction;
    }

    public void startTransaction() {
        this.inTransaction = true;
        this.queuedCommands.clear();
    }

    public void queueCommand(RespFrame.Array command) {
        this.queuedCommands.add(command);
    }

    public List<RespFrame.Array> getQueuedCommands() {
        return new ArrayList<>(queuedCommands);
    }

    public void discard() {
        this.inTransaction = false;
        this.queuedCommands.clear();
        this.watchedKeys.clear();
    }

    public void complete() {
        this.inTransaction = false;
        this.queuedCommands.clear();
        this.watchedKeys.clear();
    }

    public void watch(String key, long version) {
        this.watchedKeys.put(key, version);
    }

    public void unwatch() {
        this.watchedKeys.clear();
    }

    /**
     * Checks whether any watched key has mutated since WATCH was issued.
     *
     * @param dataStore Current data store instance
     * @return true if any watched key version has changed (aborts transaction)
     */
    public boolean isDirty(DataStore dataStore) {
        for (Map.Entry<String, Long> entry : watchedKeys.entrySet()) {
            String key = entry.getKey();
            long watchedVersion = entry.getValue();
            long currentVersion = dataStore.getKeyVersion(key);
            if (watchedVersion != currentVersion) {
                return true;
            }
        }
        return false;
    }
}
