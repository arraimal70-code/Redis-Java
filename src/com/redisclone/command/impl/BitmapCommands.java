package com.redisclone.command.impl;

import com.redisclone.command.Command;
import com.redisclone.command.CommandContext;
import com.redisclone.resp.RespFrame;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class BitmapCommands {

    public static class SetBitCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.size() != 3) {
                return RespFrame.ofError("ERR wrong number of arguments for 'setbit' command");
            }
            String key = new String(args.get(0), StandardCharsets.UTF_8);
            try {
                int offset = Integer.parseInt(new String(args.get(1), StandardCharsets.US_ASCII));
                int bitVal = Integer.parseInt(new String(args.get(2), StandardCharsets.US_ASCII));
                if (offset < 0 || (bitVal != 0 && bitVal != 1)) {
                    return RespFrame.ofError("ERR bit is not an integer or out of range");
                }
                int old = ctx.getDataStore().setbit(key, offset, bitVal);
                return RespFrame.ofInteger(old);
            } catch (NumberFormatException e) {
                return RespFrame.ofError("ERR bit offset is not an integer or out of range");
            }
        }

        @Override
        public boolean isWriteCommand() {
            return true;
        }
    }

    public static class GetBitCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.size() != 2) {
                return RespFrame.ofError("ERR wrong number of arguments for 'getbit' command");
            }
            String key = new String(args.get(0), StandardCharsets.UTF_8);
            try {
                int offset = Integer.parseInt(new String(args.get(1), StandardCharsets.US_ASCII));
                if (offset < 0) {
                    return RespFrame.ofError("ERR bit offset is not an integer or out of range");
                }
                int bit = ctx.getDataStore().getbit(key, offset);
                return RespFrame.ofInteger(bit);
            } catch (NumberFormatException e) {
                return RespFrame.ofError("ERR bit offset is not an integer or out of range");
            }
        }
    }

    public static class BitCountCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.isEmpty()) {
                return RespFrame.ofError("ERR wrong number of arguments for 'bitcount' command");
            }
            String key = new String(args.get(0), StandardCharsets.UTF_8);
            int start = 0;
            int end = -1;
            if (args.size() >= 3) {
                try {
                    start = Integer.parseInt(new String(args.get(1), StandardCharsets.US_ASCII));
                    end = Integer.parseInt(new String(args.get(2), StandardCharsets.US_ASCII));
                } catch (NumberFormatException e) {
                    return RespFrame.ofError("ERR value is not an integer or out of range");
                }
            }
            long count = ctx.getDataStore().bitcount(key, start, end);
            return RespFrame.ofInteger(count);
        }
    }
}
