package com.redisclone.command.impl;

import com.redisclone.command.Command;
import com.redisclone.command.CommandContext;
import com.redisclone.resp.RespFrame;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class KeyCommands {

    public static class DelCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.isEmpty()) {
                return RespFrame.ofError("ERR wrong number of arguments for 'del' command");
            }

            String[] keys = new String[args.size()];
            for (int i = 0; i < args.size(); i++) {
                keys[i] = new String(args.get(i), StandardCharsets.UTF_8);
            }

            int deleted = ctx.getDataStore().del(keys);
            return RespFrame.ofInteger(deleted);
        }

        @Override
        public boolean isWriteCommand() {
            return true;
        }
    }

    public static class ExpireCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.size() != 2) {
                return RespFrame.ofError("ERR wrong number of arguments for 'expire' command");
            }

            String key = new String(args.get(0), StandardCharsets.UTF_8);
            long seconds;
            try {
                seconds = Long.parseLong(new String(args.get(1), StandardCharsets.US_ASCII));
            } catch (NumberFormatException e) {
                return RespFrame.ofError("ERR value is not an integer or out of range");
            }

            boolean updated = ctx.getDataStore().expire(key, seconds);
            return RespFrame.ofInteger(updated ? 1 : 0);
        }

        @Override
        public boolean isWriteCommand() {
            return true;
        }
    }

    public static class TtlCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.size() != 1) {
                return RespFrame.ofError("ERR wrong number of arguments for 'ttl' command");
            }

            String key = new String(args.get(0), StandardCharsets.UTF_8);
            long ttl = ctx.getDataStore().ttl(key);
            return RespFrame.ofInteger(ttl);
        }
    }

    public static class ExistsCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.isEmpty()) {
                return RespFrame.ofError("ERR wrong number of arguments for 'exists' command");
            }

            int count = 0;
            for (byte[] arg : args) {
                String key = new String(arg, StandardCharsets.UTF_8);
                if (ctx.getDataStore().exists(key)) {
                    count++;
                }
            }
            return RespFrame.ofInteger(count);
        }
    }
}
