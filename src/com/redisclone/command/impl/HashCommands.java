package com.redisclone.command.impl;

import com.redisclone.command.Command;
import com.redisclone.command.CommandContext;
import com.redisclone.resp.RespFrame;
import com.redisclone.storage.RedisObject;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HashCommands {

    public static class HSetCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.size() < 3 || (args.size() - 1) % 2 != 0) {
                return RespFrame.ofError("ERR wrong number of arguments for 'hset' command");
            }

            String key = new String(args.get(0), StandardCharsets.UTF_8);
            int addedCount = 0;

            for (int i = 1; i < args.size(); i += 2) {
                String field = new String(args.get(i), StandardCharsets.UTF_8);
                byte[] value = args.get(i + 1);
                try {
                    addedCount += ctx.getDataStore().hset(key, field, value);
                } catch (IllegalStateException e) {
                    return RespFrame.ofError(e.getMessage());
                }
            }

            return RespFrame.ofInteger(addedCount);
        }

        @Override
        public boolean isWriteCommand() {
            return true;
        }
    }

    public static class HGetCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.size() != 2) {
                return RespFrame.ofError("ERR wrong number of arguments for 'hget' command");
            }

            String key = new String(args.get(0), StandardCharsets.UTF_8);
            String field = new String(args.get(1), StandardCharsets.UTF_8);

            try {
                byte[] value = ctx.getDataStore().hget(key, field);
                if (value == null) {
                    return RespFrame.ofNullBulkString();
                }
                return RespFrame.ofBulkString(value);
            } catch (IllegalStateException e) {
                return RespFrame.ofError(e.getMessage());
            }
        }
    }

    public static class HGetAllCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.size() != 1) {
                return RespFrame.ofError("ERR wrong number of arguments for 'hgetall' command");
            }

            String key = new String(args.get(0), StandardCharsets.UTF_8);
            RedisObject obj = ctx.getDataStore().get(key);
            if (obj == null) {
                return RespFrame.ofArray(List.of());
            }

            try {
                Map<String, byte[]> hash = obj.asHash();
                List<RespFrame> items = new ArrayList<>(hash.size() * 2);
                for (Map.Entry<String, byte[]> entry : hash.entrySet()) {
                    items.add(RespFrame.ofBulkString(entry.getKey()));
                    items.add(RespFrame.ofBulkString(entry.getValue()));
                }
                return RespFrame.ofArray(items);
            } catch (IllegalStateException e) {
                return RespFrame.ofError(e.getMessage());
            }
        }
    }
}
