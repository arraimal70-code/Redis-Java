package com.redisclone.command;

import com.redisclone.cluster.ClusterSlotRouter;
import com.redisclone.network.ClientConnection;
import com.redisclone.persistence.AofManager;
import com.redisclone.persistence.RdbManager;
import com.redisclone.pubsub.PubSubManager;
import com.redisclone.replication.ReplicationManager;
import com.redisclone.storage.DataStore;

/**
 * Execution context provided to each Command instance during execution.
 * Aggregates access to the client connection, in-memory store, pub/sub manager,
 * persistence engines, replication manager, cluster router, and command registry.
 */
public class CommandContext {

    private final ClientConnection client;
    private final DataStore dataStore;
    private final PubSubManager pubSubManager;
    private final AofManager aofManager;
    private final RdbManager rdbManager;
    private final ReplicationManager replicationManager;
    private final ClusterSlotRouter clusterSlotRouter;
    private final CommandRegistry commandRegistry;
    private final int serverPort;

    public CommandContext(ClientConnection client,
                          DataStore dataStore,
                          PubSubManager pubSubManager,
                          AofManager aofManager,
                          RdbManager rdbManager,
                          ReplicationManager replicationManager,
                          ClusterSlotRouter clusterSlotRouter,
                          CommandRegistry commandRegistry,
                          int serverPort) {
        this.client = client;
        this.dataStore = dataStore;
        this.pubSubManager = pubSubManager;
        this.aofManager = aofManager;
        this.rdbManager = rdbManager;
        this.replicationManager = replicationManager;
        this.clusterSlotRouter = clusterSlotRouter;
        this.commandRegistry = commandRegistry;
        this.serverPort = serverPort;
    }

    public ClientConnection getClient() {
        return client;
    }

    public DataStore getDataStore() {
        return dataStore;
    }

    public PubSubManager getPubSubManager() {
        return pubSubManager;
    }

    public AofManager getAofManager() {
        return aofManager;
    }

    public RdbManager getRdbManager() {
        return rdbManager;
    }

    public ReplicationManager getReplicationManager() {
        return replicationManager;
    }

    public ClusterSlotRouter getClusterSlotRouter() {
        return clusterSlotRouter;
    }

    public CommandRegistry getCommandRegistry() {
        return commandRegistry;
    }

    public int getServerPort() {
        return serverPort;
    }
}
