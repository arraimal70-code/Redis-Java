package com.redisclone.server;

import com.redisclone.cluster.ClusterSlotRouter;
import com.redisclone.command.CommandRegistry;
import com.redisclone.network.ClientConnection;
import com.redisclone.network.FrameHandler;
import com.redisclone.network.NioEventLoop;
import com.redisclone.persistence.AofManager;
import com.redisclone.persistence.RdbManager;
import com.redisclone.pubsub.PubSubManager;
import com.redisclone.replication.ReplicationManager;
import com.redisclone.resp.RespFrame;
import com.redisclone.storage.DataStore;
import com.redisclone.storage.EvictionEngine;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main Redis server orchestration hub.
 * Integrates the Java NIO reactor event loop, in-memory store with LRU eviction,
 * ACID/CAS transaction engine, pub/sub manager, active eviction daemon,
 * dual persistence layers (AOF & RDB), master-replica replication, and cluster slot router.
 */
public class RedisServer implements FrameHandler {

    private static final Logger LOGGER = Logger.getLogger(RedisServer.class.getName());

    private final ServerConfig config;
    private final DataStore dataStore;
    private final PubSubManager pubSubManager;
    private final AofManager aofManager;
    private final RdbManager rdbManager;
    private final ReplicationManager replicationManager;
    private final ClusterSlotRouter clusterSlotRouter;
    private final CommandRegistry commandRegistry;
    private final EvictionEngine evictionEngine;
    private final NioEventLoop eventLoop;

    public RedisServer(ServerConfig config) {
        this.config = config;
        this.dataStore = new DataStore(config.getMaxKeys(), config.getEvictionPolicy());
        this.pubSubManager = new PubSubManager();
        this.aofManager = new AofManager(config.getAofFilePath(), config.getAofFsync(), config.isAofEnabled());
        this.rdbManager = new RdbManager(config.getRdbFilePath());
        this.replicationManager = new ReplicationManager();
        this.clusterSlotRouter = new ClusterSlotRouter(config.getHost(), config.getPort());
        this.clusterSlotRouter.setClusterEnabled(config.isClusterEnabled());
        this.clusterSlotRouter.setMySlotRange(config.getClusterStartSlot(), config.getClusterEndSlot());

        this.commandRegistry = new CommandRegistry(
                dataStore, pubSubManager, aofManager, rdbManager,
                replicationManager, clusterSlotRouter, config.getPort()
        );
        this.evictionEngine = new EvictionEngine(dataStore);
        this.eventLoop = new NioEventLoop(config.getHost(), config.getPort(), this);
    }

    public synchronized void start() throws IOException {
        LOGGER.info("Initializing Custom Redis Server v1.0.0...");

        // 1. Crash recovery: Replay state from disk
        recoverStateFromDisk();

        // 2. Start Active Eviction background daemon
        evictionEngine.start();

        // 3. Start AOF persistence stream
        if (config.isAofEnabled()) {
            aofManager.start();
        }

        // 4. Start NIO Reactor Network Event Loop
        eventLoop.start();

        // 5. Connect to master if configured as replica
        if (config.getReplicaOfHost() != null && config.getReplicaOfPort() > 0) {
            replicationManager.replicaOf(
                    config.getReplicaOfHost(), config.getReplicaOfPort(),
                    config.getPort(), dataStore, commandRegistry
            );
        }

        // 6. Register Graceful Shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop, "redis-shutdown-hook"));

        printAsciiBanner();
    }

    private void recoverStateFromDisk() {
        File aofFile = new File(config.getAofFilePath());
        File rdbFile = new File(config.getRdbFilePath());

        // AOF takes precedence over RDB in standard Redis persistence hierarchy
        if (config.isAofEnabled() && aofFile.exists() && aofFile.length() > 0) {
            int replayed = aofManager.replay(commandRegistry);
            LOGGER.info("Restored " + replayed + " commands from AOF log.");
        } else if (config.isRdbEnabled() && rdbFile.exists() && rdbFile.length() > 0) {
            int restored = rdbManager.load(dataStore);
            LOGGER.info("Restored " + restored + " keys from RDB snapshot.");
        }
    }

    @Override
    public void handleFrame(ClientConnection client, RespFrame frame) {
        commandRegistry.dispatch(client, frame);
    }

    @Override
    public void onClientDisconnected(ClientConnection client) {
        pubSubManager.onClientDisconnected(client);
        replicationManager.removeReplica(client);
    }

    public synchronized void stop() {
        LOGGER.info("Initiating graceful Redis server shutdown...");

        // Stop accepting new network traffic
        eventLoop.stop();

        // Stop eviction cycles
        evictionEngine.stop();

        // Sync snapshot if RDB enabled
        if (config.isRdbEnabled()) {
            LOGGER.info("Saving DB snapshot to disk before shutdown...");
            rdbManager.save(dataStore);
        }

        // Close AOF log
        if (config.isAofEnabled()) {
            aofManager.close();
        }

        LOGGER.info("Redis server shutdown complete. Bye!");
    }

    public DataStore getDataStore() {
        return dataStore;
    }

    public PubSubManager getPubSubManager() {
        return pubSubManager;
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

    public ServerConfig getConfig() {
        return config;
    }

    private void printAsciiBanner() {
        System.out.println("""
                
                    _._                                                 \s
               _.-``__ ''-._                                            \s
          _.-``    `.  `_.  ''-._           Redis Clone (Java 21 Core)  \s
      .-`` .-```.  ```\\/    _.,_ ''-._                                  \s
     (    '      `       .-`  | `,    )     Port: """ + config.getPort() + """
                                          \s
     |`-._`-...-` __...-.``-._|'` _.-'|     PID:  """ + ProcessHandle.current().pid() + """
                                          \s
     |    `-._   `._    /     _.-'    |     Role: """ + replicationManager.getRole() + """
                                          \s
      `-._    `-._  `-./  _.-'    _.-'      Running in Reactor Mode     \s
     |`-._`-._    `-.__.-'    _.-'_.-'|     Active Eviction: 10Hz       \s
     |    `-._`-._        _.-'_.-'    |     AOF: """ + config.isAofEnabled() + " (" + config.getAofFsync() + ")" + """
                                          \s
      `-._    `-._`-.__.-'_.-'    _.-'      RDB: """ + config.isRdbEnabled() + """
                                          \s
          `-._    `-.__.-'    _.-'          Cluster: """ + config.isClusterEnabled() + """
                                          \s
              `-._        _.-'              Eviction: """ + (config.getEvictionPolicy() != null ? config.getEvictionPolicy().getName() : "none") + """
                                          \s
                  `-.__.-'                                              \s
                """);
    }

    public static void main(String[] args) {
        ServerConfig config = new ServerConfig();

        for (int i = 0; i < args.length; i++) {
            if ("--port".equals(args[i]) && i + 1 < args.length) {
                config.setPort(Integer.parseInt(args[++i]));
            } else if ("--host".equals(args[i]) && i + 1 < args.length) {
                config.setHost(args[++i]);
            } else if ("--replicaof".equals(args[i]) && i + 2 < args.length) {
                config.setReplicaOf(args[++i], Integer.parseInt(args[++i]));
            } else if ("--maxkeys".equals(args[i]) && i + 1 < args.length) {
                config.setMaxKeys(Integer.parseInt(args[++i]));
            } else if ("--cluster-enabled".equals(args[i]) && i + 1 < args.length) {
                config.setClusterEnabled("yes".equalsIgnoreCase(args[++i]) || "true".equalsIgnoreCase(args[i]));
            } else if ("--aof".equals(args[i]) && i + 1 < args.length) {
                config.setAofEnabled(Boolean.parseBoolean(args[++i]));
            } else if ("--rdb".equals(args[i]) && i + 1 < args.length) {
                config.setRdbEnabled(Boolean.parseBoolean(args[++i]));
            }
        }

        try {
            RedisServer server = new RedisServer(config);
            server.start();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to start Redis server", e);
            System.exit(1);
        }
    }
}
