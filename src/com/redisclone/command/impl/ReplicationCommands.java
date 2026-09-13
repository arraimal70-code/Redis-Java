package com.redisclone.command.impl;

import com.redisclone.command.Command;
import com.redisclone.command.CommandContext;
import com.redisclone.resp.RespFrame;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class ReplicationCommands {

    public static class ReplicaOfCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.size() != 2) {
                return RespFrame.ofError("ERR wrong number of arguments for 'replicaof' command");
            }

            String host = new String(args.get(0), StandardCharsets.UTF_8);
            String portStr = new String(args.get(1), StandardCharsets.UTF_8);

            if ("NO".equalsIgnoreCase(host) && "ONE".equalsIgnoreCase(portStr)) {
                ctx.getReplicationManager().replicaOfNoOne();
                return RespFrame.ofSimpleString("OK");
            }

            try {
                int port = Integer.parseInt(portStr);
                ctx.getReplicationManager().replicaOf(host, port, ctx.getServerPort(), ctx.getDataStore(), ctx.getCommandRegistry());
                return RespFrame.ofSimpleString("OK");
            } catch (NumberFormatException e) {
                return RespFrame.ofError("ERR port is not a valid integer");
            }
        }
    }

    public static class PsyncCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.size() != 2) {
                return RespFrame.ofError("ERR wrong number of arguments for 'psync' command");
            }

            String replId = new String(args.get(0), StandardCharsets.UTF_8);
            long offset = -1;
            try {
                offset = Long.parseLong(new String(args.get(1), StandardCharsets.US_ASCII));
            } catch (NumberFormatException ignored) {}

            return ctx.getReplicationManager().handlePsync(ctx.getClient(), replId, offset);
        }
    }

    public static class ReplConfCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.size() >= 2) {
                String sub = new String(args.get(0), StandardCharsets.UTF_8).toLowerCase();
                if ("ack".equals(sub)) {
                    // ACK from replica
                    return null;
                }
            }
            return RespFrame.ofSimpleString("OK");
        }
    }
}
