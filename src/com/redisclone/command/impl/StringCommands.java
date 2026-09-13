package com.redisclone.command.impl;

import com.redisclone.command.Command;
import com.redisclone.command.CommandContext;
import com.redisclone.resp.RespFrame;
import com.redisclone.storage.RedisObject;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

public class StringCommands {

    public static class SetCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.size() < 2) {
                return RespFrame.ofError("ERR wrong number of arguments for 'set' command");
            }

            String key = new String(args.get(0), StandardCharsets.UTF_8);
            byte[] value = args.get(1);

            Long ttlMillis = null;

            // Parse optional EX / PX arguments
            for (int i = 2; i < args.size(); i++) {
                String option = new String(args.get(i), StandardCharsets.UTF_8).toUpperCase(Locale.ROOT);
                if ("EX".equals(option) && i + 1 < args.size()) {
                    try {
                        long seconds = Long.parseLong(new String(args.get(++i), StandardCharsets.US_ASCII));
                        ttlMillis = seconds * 1000L;
                    } catch (NumberFormatException e) {
                        return RespFrame.ofError("ERR value is not an integer or out of range");
                    }
                } else if ("PX".equals(option) && i + 1 < args.size()) {
                    try {
                        ttlMillis = Long.parseLong(new String(args.get(++i), StandardCharsets.US_ASCII));
                    } catch (NumberFormatException e) {
                        return RespFrame.ofError("ERR value is not an integer or out of range");
                    }
                }
            }

            ctx.getDataStore().set(key, RedisObject.ofString(value), ttlMillis);
            return RespFrame.ofSimpleString("OK");
        }

        @Override
        public boolean isWriteCommand() {
            return true;
        }
    }

    public static class GetCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.size() != 1) {
                return RespFrame.ofError("ERR wrong number of arguments for 'get' command");
            }

            String key = new String(args.get(0), StandardCharsets.UTF_8);
            RedisObject obj = ctx.getDataStore().get(key);
            if (obj == null) {
                return RespFrame.ofNullBulkString();
            }

            try {
                return RespFrame.ofBulkString(obj.asStringBytes());
            } catch (IllegalStateException e) {
                return RespFrame.ofError(e.getMessage());
            }
        }
    }

    public static class IncrCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.size() != 1) {
                return RespFrame.ofError("ERR wrong number of arguments for 'incr' command");
            }

            String key = new String(args.get(0), StandardCharsets.UTF_8);
            try {
                long newVal = ctx.getDataStore().incr(key);
                return RespFrame.ofInteger(newVal);
            } catch (IllegalStateException e) {
                return RespFrame.ofError(e.getMessage());
            }
        }

        @Override
        public boolean isWriteCommand() {
            return true;
        }
    }
}
