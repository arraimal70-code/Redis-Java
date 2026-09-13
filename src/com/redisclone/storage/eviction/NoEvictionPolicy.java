package com.redisclone.storage.eviction;

/**
 * NoEviction policy: Never evicts keys automatically; write operations fail
 * when memory threshold is exceeded.
 */
public class NoEvictionPolicy implements EvictionPolicy {

    @Override
    public void onKeyAccess(String key) {}

    @Override
    public void onKeyInsert(String key) {}

    @Override
    public void onKeyDelete(String key) {}

    @Override
    public String evictKey() {
        return null;
    }

    @Override
    public String getName() {
        return "noeviction";
    }
}
