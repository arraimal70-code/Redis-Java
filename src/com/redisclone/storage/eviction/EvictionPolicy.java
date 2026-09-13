package com.redisclone.storage.eviction;

/**
 * Strategy interface defining key eviction algorithms (LRU, LFU, FIFO, NoEviction).
 * Follows the Strategy design pattern to allow pluggable memory reclamation policies.
 */
public interface EvictionPolicy {

    /**
     * Called whenever an existing key is looked up or updated.
     */
    void onKeyAccess(String key);

    /**
     * Called when a new key is added into the store.
     */
    void onKeyInsert(String key);

    /**
     * Called when a key is explicitly deleted or expired.
     */
    void onKeyDelete(String key);

    /**
     * Selects and removes a key according to the policy to free memory.
     *
     * @return The evicted key name, or null if no key can be evicted.
     */
    String evictKey();

    /**
     * Human-readable name of the eviction policy (e.g. allkeys-lru, noeviction).
     */
    String getName();
}
