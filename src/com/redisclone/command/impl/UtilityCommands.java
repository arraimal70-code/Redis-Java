package com.redisclone.command.impl;

import com.redisclone.command.Command;
import com.redisclone.command.CommandContext;
import com.redisclone.resp.RespFrame;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class UtilityCommands {

    public static class PingCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.isEmpty()) {
                return RespFrame.ofSimpleString("PONG");
            }
            return RespFrame.ofBulkString(args.get(0));
        }
    }

    public static class EchoCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.size() != 1) {
                return RespFrame.ofError("ERR wrong number of arguments for 'echo' command");
            }
            return RespFrame.ofBulkString(args.get(0));
        }
    }

    public static class CommandInfoCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            // Modern redis-cli sends "COMMAND" or "COMMAND DOCS" on connect
            return RespFrame.ofArray(List.of());
        }
    }

    public static class InfoCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            Runtime rt = Runtime.getRuntime();
            long totalMem = rt.totalMemory();
            long freeMem = rt.freeMemory();
            long jvmUsed = totalMem - freeMem;
            long dataMem = ctx.getDataStore().estimateMemoryBytes();

            String role = ctx.getReplicationManager() != null ? ctx.getReplicationManager().getRole().name().toLowerCase() : "master";
            int slaves = ctx.getReplicationManager() != null ? ctx.getReplicationManager().getConnectedReplicasCount() : 0;
            String replId = ctx.getReplicationManager() != null ? ctx.getReplicationManager().getBacklog().getMasterReplId() : "0";
            long replOffset = ctx.getReplicationManager() != null ? ctx.getReplicationManager().getBacklog().getMasterOffset() : 0L;
            boolean clusterOn = ctx.getClusterSlotRouter() != null && ctx.getClusterSlotRouter().isClusterEnabled();

            String info = "# Server\r\n" +
                    "redis_version:7.0.0-systems-clone\r\n" +
                    "os:" + System.getProperty("os.name") + "\r\n" +
                    "process_id:" + ProcessHandle.current().pid() + "\r\n" +
                    "tcp_port:" + ctx.getServerPort() + "\r\n" +
                    "# Memory\r\n" +
                    "used_memory:" + dataMem + "\r\n" +
                    "used_memory_human:" + (dataMem / 1024) + "K\r\n" +
                    "jvm_heap_used:" + jvmUsed + "\r\n" +
                    "jvm_heap_total:" + totalMem + "\r\n" +
                    "maxmemory_policy:" + (ctx.getDataStore().getEvictionPolicy() != null ? ctx.getDataStore().getEvictionPolicy().getName() : "none") + "\r\n" +
                    "# Replication\r\n" +
                    "role:" + role + "\r\n" +
                    "connected_slaves:" + slaves + "\r\n" +
                    "master_replid:" + replId + "\r\n" +
                    "master_repl_offset:" + replOffset + "\r\n" +
                    "# Cluster\r\n" +
                    "cluster_enabled:" + (clusterOn ? 1 : 0) + "\r\n" +
                    "# Keyspace\r\n" +
                    "db0:keys=" + ctx.getDataStore().keyCount() + "\r\n";
            return RespFrame.ofBulkString(info);
        }
    }

    public static class SaveCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (ctx.getRdbManager() == null) {
                return RespFrame.ofError("ERR RDB snapshotting is disabled");
            }
            boolean success = ctx.getRdbManager().save(ctx.getDataStore());
            return success ? RespFrame.ofSimpleString("OK") : RespFrame.ofError("ERR failed saving RDB");
        }
    }

    public static class BgSaveCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (ctx.getRdbManager() == null) {
                return RespFrame.ofError("ERR RDB snapshotting is disabled");
            }
            ctx.getRdbManager().bgSave(ctx.getDataStore());
            return RespFrame.ofSimpleString("Background saving started");
        }
    }

    public static class QuitCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (ctx.getClient() != null) {
                ctx.getClient().sendReply(RespFrame.ofSimpleString("OK"));
                ctx.getClient().close();
                return null;
            }
            return RespFrame.ofSimpleString("OK");
        }
    }
}
