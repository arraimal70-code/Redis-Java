package com.redisclone.command;

import com.redisclone.cluster.ClusterSlotRouter;
import com.redisclone.cluster.Crc16;
import com.redisclone.command.impl.*;
import com.redisclone.network.ClientConnection;
import com.redisclone.persistence.AofManager;
import com.redisclone.persistence.RdbManager;
import com.redisclone.pubsub.PubSubManager;
import com.redisclone.replication.ReplicationManager;
import com.redisclone.resp.RespFrame;
import com.redisclone.storage.DataStore;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Command registry and dispatch table.
 * Routes incoming RESP command frames, enforces transaction queuing (MULTI/EXEC),
 * coordinates cluster slot redirection (-MOVED), and replicates mutations to replicas and AOF.
 */
public class CommandRegistry {

    private final Map<String, Command> commands = new HashMap<>();
    private final DataStore dataStore;
    private final PubSubManager pubSubManager;
    private final AofManager aofManager;
    private final RdbManager rdbManager;
    private final ReplicationManager replicationManager;
    private final ClusterSlotRouter clusterSlotRouter;
    private final int serverPort;

    private static final Set<String> PUBSUB_ALLOWED_COMMANDS = Set.of(
            "SUBSCRIBE", "PSUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT", "RESET"
    );

    private static final Set<String> TX_CONTROL_COMMANDS = Set.of(
            "EXEC", "DISCARD", "MULTI", "WATCH", "UNWATCH", "QUIT"
    );

    public CommandRegistry(DataStore dataStore,
                           PubSubManager pubSubManager,
                           AofManager aofManager,
                           RdbManager rdbManager,
                           ReplicationManager replicationManager,
                           ClusterSlotRouter clusterSlotRouter,
                           int serverPort) {
        this.dataStore = dataStore;
        this.pubSubManager = pubSubManager;
        this.aofManager = aofManager;
        this.rdbManager = rdbManager;
        this.replicationManager = replicationManager;
        this.clusterSlotRouter = clusterSlotRouter;
        this.serverPort = serverPort;
        registerDefaultCommands();
    }

    private void registerDefaultCommands() {
        // String commands
        commands.put("SET", new StringCommands.SetCommand());
        commands.put("GET", new StringCommands.GetCommand());
        commands.put("INCR", new StringCommands.IncrCommand());

        // Key commands
        commands.put("DEL", new KeyCommands.DelCommand());
        commands.put("EXPIRE", new KeyCommands.ExpireCommand());
        commands.put("TTL", new KeyCommands.TtlCommand());
        commands.put("EXISTS", new KeyCommands.ExistsCommand());

        // Hash commands
        commands.put("HSET", new HashCommands.HSetCommand());
        commands.put("HGET", new HashCommands.HGetCommand());
        commands.put("HGETALL", new HashCommands.HGetAllCommand());

        // List commands
        commands.put("LPUSH", new ListCommands.LPushCommand());
        commands.put("LPOP", new ListCommands.LPopCommand());
        commands.put("LLEN", new ListCommands.LLenCommand());

        // Pub/Sub commands
        commands.put("PUBLISH", new PubSubCommands.PublishCommand());
        commands.put("SUBSCRIBE", new PubSubCommands.SubscribeCommand());
        commands.put("UNSUBSCRIBE", new PubSubCommands.UnsubscribeCommand());

        // Transaction commands (ACID/CAS)
        commands.put("MULTI", new TransactionCommands.MultiCommand());
        commands.put("EXEC", new TransactionCommands.ExecCommand());
        commands.put("DISCARD", new TransactionCommands.DiscardCommand());
        commands.put("WATCH", new TransactionCommands.WatchCommand());
        commands.put("UNWATCH", new TransactionCommands.UnwatchCommand());

        // Replication commands
        commands.put("REPLICAOF", new ReplicationCommands.ReplicaOfCommand());
        commands.put("SLAVEOF", new ReplicationCommands.ReplicaOfCommand());
        commands.put("PSYNC", new ReplicationCommands.PsyncCommand());
        commands.put("REPLCONF", new ReplicationCommands.ReplConfCommand());

        // Cluster commands
        commands.put("CLUSTER", new ClusterCommands.ClusterCommand());

        // Operational / Utility commands
        commands.put("PING", new UtilityCommands.PingCommand());
        commands.put("ECHO", new UtilityCommands.EchoCommand());
        commands.put("COMMAND", new UtilityCommands.CommandInfoCommand());
        commands.put("INFO", new UtilityCommands.InfoCommand());
        commands.put("SAVE", new UtilityCommands.SaveCommand());
        commands.put("BGSAVE", new UtilityCommands.BgSaveCommand());
        commands.put("QUIT", new UtilityCommands.QuitCommand());
    }

    public void registerCommand(String name, Command command) {
        commands.put(name.toUpperCase(Locale.ROOT), command);
    }

    /**
     * Dispatches an incoming parsed RESP frame from a client socket.
     */
    public void dispatch(ClientConnection client, RespFrame frame) {
        if (!(frame instanceof RespFrame.Array arrayFrame) || arrayFrame.elements() == null || arrayFrame.elements().isEmpty()) {
            client.sendReply(RespFrame.ofError("ERR invalid command format"));
            return;
        }

        List<RespFrame> elements = arrayFrame.elements();
        String commandName = extractString(elements.get(0)).toUpperCase(Locale.ROOT);

        // Enforce Pub/Sub mode command restrictions
        if (client.isSubscribed() && !PUBSUB_ALLOWED_COMMANDS.contains(commandName)) {
            client.sendReply(RespFrame.ofError("ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context"));
            return;
        }

        // Transaction Queuing Check: if inside MULTI, buffer command except for transaction control commands
        if (client.getTransactionContext().isInTransaction() && !TX_CONTROL_COMMANDS.contains(commandName)) {
            client.getTransactionContext().queueCommand(arrayFrame);
            client.sendReply(RespFrame.ofSimpleString("QUEUED"));
            return;
        }

        // Cluster Slot Redirection: if cluster sharding is enabled, inspect key and redirect if slot not owned
        if (clusterSlotRouter != null && clusterSlotRouter.isClusterEnabled() && elements.size() > 1) {
            String key = extractString(elements.get(1));
            int slot = Crc16.getSlot(key);
            if (!clusterSlotRouter.isMySlot(slot)) {
                client.sendReply(clusterSlotRouter.createMovedError(slot));
                return;
            }
        }

        RespFrame reply = executeDirect(client, arrayFrame);
        if (reply != null) {
            client.sendReply(reply);
        }
    }

    /**
     * Directly executes a command frame, appends to AOF and propagates to replicas if mutating.
     */
    public RespFrame executeDirect(ClientConnection client, RespFrame.Array arrayFrame) {
        List<RespFrame> elements = arrayFrame.elements();
        if (elements == null || elements.isEmpty()) {
            return RespFrame.ofError("ERR empty command");
        }

        String commandName = extractString(elements.get(0)).toUpperCase(Locale.ROOT);
        Command command = commands.get(commandName);
        if (command == null) {
            return RespFrame.ofError("ERR unknown command '" + commandName + "'");
        }

        List<byte[]> args = new ArrayList<>(elements.size() - 1);
        for (int i = 1; i < elements.size(); i++) {
            args.add(extractBytes(elements.get(i)));
        }

        CommandContext ctx = new CommandContext(
                client, dataStore, pubSubManager, aofManager, rdbManager,
                replicationManager, clusterSlotRouter, this, serverPort
        );
        com.redisclone.server.ServerMetrics.getInstance().recordCommand();
        RespFrame reply = command.execute(ctx, args);
        if (reply instanceof RespFrame.Error) {
            com.redisclone.server.ServerMetrics.getInstance().recordCommandError();
        }

        // State mutation: Persist to AOF and broadcast to connected replicas
        if (command.isWriteCommand()) {
            if (aofManager != null && aofManager.isEnabled()) {
                aofManager.append(arrayFrame);
            }
            if (replicationManager != null) {
                replicationManager.propagateWrite(arrayFrame);
            }
        }

        return reply;
    }

    /**
     * Internal execution for AOF log replay or replica stream execution.
     */
    public void executeInternal(RespFrame.Array arrayFrame) {
        List<RespFrame> elements = arrayFrame.elements();
        if (elements == null || elements.isEmpty()) return;

        String commandName = extractString(elements.get(0)).toUpperCase(Locale.ROOT);
        Command command = commands.get(commandName);
        if (command == null) return;

        List<byte[]> args = new ArrayList<>(elements.size() - 1);
        for (int i = 1; i < elements.size(); i++) {
            args.add(extractBytes(elements.get(i)));
        }

        CommandContext ctx = new CommandContext(
                null, dataStore, pubSubManager, null, rdbManager,
                null, clusterSlotRouter, this, serverPort
        );
        command.execute(ctx, args);
    }

    private String extractString(RespFrame frame) {
        if (frame instanceof RespFrame.BulkString b) {
            return b.asUtf8String();
        } else if (frame instanceof RespFrame.SimpleString s) {
            return s.value();
        }
        return frame.toString();
    }

    private byte[] extractBytes(RespFrame frame) {
        if (frame instanceof RespFrame.BulkString b) {
            return b.data() != null ? b.data() : new byte[0];
        } else if (frame instanceof RespFrame.SimpleString s) {
            return s.value().getBytes(StandardCharsets.UTF_8);
        } else if (frame instanceof RespFrame.Integer i) {
            return Long.toString(i.value()).getBytes(StandardCharsets.US_ASCII);
        }
        return new byte[0];
    }
}
