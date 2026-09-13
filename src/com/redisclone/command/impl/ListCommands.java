package com.redisclone.command.impl;

import com.redisclone.command.Command;
import com.redisclone.command.CommandContext;
import com.redisclone.resp.RespFrame;
import com.redisclone.storage.RedisObject;

import java.nio.charset.StandardCharsets;
import java.util.Deque;
import java.util.List;

public class ListCommands {

    public static class LPushCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.size() < 2) {
                return RespFrame.ofError("ERR wrong number of arguments for 'lpush' command");
            }

            String key = new String(args.get(0), StandardCharsets.UTF_8);
            byte[][] values = new byte[args.size() - 1][];
            for (int i = 1; i < args.size(); i++) {
                values[i - 1] = args.get(i);
            }

            try {
                int length = ctx.getDataStore().lpush(key, values);
                return RespFrame.ofInteger(length);
            } catch (IllegalStateException e) {
                return RespFrame.ofError(e.getMessage());
            }
        }

        @Override
        public boolean isWriteCommand() {
            return true;
        }
    }

    public static class LPopCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.isEmpty()) {
                return RespFrame.ofError("ERR wrong number of arguments for 'lpop' command");
            }

            String key = new String(args.get(0), StandardCharsets.UTF_8);
            try {
                byte[] popped = ctx.getDataStore().lpop(key);
                if (popped == null) {
                    return RespFrame.ofNullBulkString();
                }
                return RespFrame.ofBulkString(popped);
            } catch (IllegalStateException e) {
                return RespFrame.ofError(e.getMessage());
            }
        }

        @Override
        public boolean isWriteCommand() {
            return true;
        }
    }

    public static class LLenCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.size() != 1) {
                return RespFrame.ofError("ERR wrong number of arguments for 'llen' command");
            }

            String key = new String(args.get(0), StandardCharsets.UTF_8);
            RedisObject obj = ctx.getDataStore().get(key);
            if (obj == null) {
                return RespFrame.ofInteger(0);
            }

            try {
                Deque<byte[]> list = obj.asList();
                return RespFrame.ofInteger(list.size());
            } catch (IllegalStateException e) {
                return RespFrame.ofError(e.getMessage());
            }
        }
    }
}
