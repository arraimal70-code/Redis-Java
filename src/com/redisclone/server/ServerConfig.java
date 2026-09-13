package com.redisclone.server;

import com.redisclone.persistence.AofManager;
import com.redisclone.storage.eviction.EvictionPolicy;
import com.redisclone.storage.eviction.LruEvictionPolicy;
import com.redisclone.storage.eviction.NoEvictionPolicy;

public class ServerConfig {
    private String host = "0.0.0.0";
    private int port = 6379;
    private boolean aofEnabled = true;
    private String aofFilePath = "appendonly.aof";
    private AofManager.FsyncPolicy aofFsync = AofManager.FsyncPolicy.EVERYSEC;
    private boolean rdbEnabled = true;
    private String rdbFilePath = "dump.rdb";

    // Eviction settings
    private int maxKeys = 0; // 0 = unlimited
    private EvictionPolicy evictionPolicy = LruEvictionPolicy.allKeysLru();

    // Replication settings
    private String replicaOfHost = null;
    private int replicaOfPort = 0;

    // Cluster settings
    private boolean clusterEnabled = false;
    private int clusterStartSlot = 0;
    private int clusterEndSlot = 16383;

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public boolean isAofEnabled() { return aofEnabled; }
    public void setAofEnabled(boolean aofEnabled) { this.aofEnabled = aofEnabled; }

    public String getAofFilePath() { return aofFilePath; }
    public void setAofFilePath(String aofFilePath) { this.aofFilePath = aofFilePath; }

    public AofManager.FsyncPolicy getAofFsync() { return aofFsync; }
    public void setAofFsync(AofManager.FsyncPolicy aofFsync) { this.aofFsync = aofFsync; }

    public boolean isRdbEnabled() { return rdbEnabled; }
    public void setRdbEnabled(boolean rdbEnabled) { this.rdbEnabled = rdbEnabled; }

    public String getRdbFilePath() { return rdbFilePath; }
    public void setRdbFilePath(String rdbFilePath) { this.rdbFilePath = rdbFilePath; }

    public int getMaxKeys() { return maxKeys; }
    public void setMaxKeys(int maxKeys) { this.maxKeys = maxKeys; }

    public EvictionPolicy getEvictionPolicy() { return evictionPolicy; }
    public void setEvictionPolicy(EvictionPolicy evictionPolicy) { this.evictionPolicy = evictionPolicy; }

    public void setEvictionPolicyByName(String name) {
        if ("noeviction".equalsIgnoreCase(name)) {
            this.evictionPolicy = new NoEvictionPolicy();
        } else {
            this.evictionPolicy = LruEvictionPolicy.allKeysLru();
        }
    }

    public String getReplicaOfHost() { return replicaOfHost; }
    public int getReplicaOfPort() { return replicaOfPort; }
    public void setReplicaOf(String host, int port) {
        this.replicaOfHost = host;
        this.replicaOfPort = port;
    }

    public boolean isClusterEnabled() { return clusterEnabled; }
    public void setClusterEnabled(boolean clusterEnabled) { this.clusterEnabled = clusterEnabled; }

    public int getClusterStartSlot() { return clusterStartSlot; }
    public void setClusterStartSlot(int clusterStartSlot) { this.clusterStartSlot = clusterStartSlot; }

    public int getClusterEndSlot() { return clusterEndSlot; }
    public void setClusterEndSlot(int clusterEndSlot) { this.clusterEndSlot = clusterEndSlot; }
}
