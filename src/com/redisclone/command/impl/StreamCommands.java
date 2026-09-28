package com.redisclone.command.impl;

import com.redisclone.command.Command;
import com.redisclone.command.CommandContext;
import com.redisclone.resp.RespFrame;
import com.redisclone.storage.StreamEntry;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class StreamCommands {

    public static class XAddCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            // XADD key ID field value [field value ...]
            if (args.size() < 4 || (args.size() % 2 != 0)) {
                return RespFrame.ofError("ERR wrong number of arguments for 'xadd' command");
            }

            String key = new String(args.get(0), StandardCharsets.UTF_8);
            String idSpec = new String(args.get(1), StandardCharsets.US_ASCII);

            List<byte[]> fields = new ArrayList<>(args.size() - 2);
            for (int i = 2; i < args.size(); i++) {
                fields.add(args.get(i));
            }

            try {
                String assignedId = ctx.getDataStore().xadd(key, idSpec, fields);
                return RespFrame.ofBulkString(assignedId.getBytes(StandardCharsets.US_ASCII));
            } catch (IllegalArgumentException e) {
                return RespFrame.ofError(e.getMessage());
            } catch (IllegalStateException e) {
                return RespFrame.ofError(e.getMessage());
            }
        }

        @Override
        public boolean isWriteCommand() {
            return true;
        }
    }

    public static class XLenCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.size() != 1) {
                return RespFrame.ofError("ERR wrong number of arguments for 'xlen' command");
            }
            String key = new String(args.get(0), StandardCharsets.UTF_8);
            try {
                int len = ctx.getDataStore().xlen(key);
                return RespFrame.ofInteger(len);
            } catch (IllegalStateException e) {
                return RespFrame.ofError(e.getMessage());
            }
        }
    }

    public static class XRangeCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            // XRANGE key start end [COUNT count]
            if (args.size() < 3) {
                return RespFrame.ofError("ERR wrong number of arguments for 'xrange' command");
            }

            String key = new String(args.get(0), StandardCharsets.UTF_8);
            String start = new String(args.get(1), StandardCharsets.US_ASCII);
            String end = new String(args.get(2), StandardCharsets.US_ASCII);
            int count = -1;

            if (args.size() >= 5) {
                String opt = new String(args.get(3), StandardCharsets.US_ASCII);
                if ("COUNT".equalsIgnoreCase(opt)) {
                    try {
                        count = Integer.parseInt(new String(args.get(4), StandardCharsets.US_ASCII));
                    } catch (NumberFormatException e) {
                        return RespFrame.ofError("ERR value is not an integer or out of range");
                    }
                }
            }

            try {
                List<StreamEntry> entries = ctx.getDataStore().xrange(key, start, end, count);
                List<RespFrame> entryFrames = new ArrayList<>(entries.size());
                for (StreamEntry entry : entries) {
                    List<RespFrame> fieldFrames = new ArrayList<>(entry.fields().size());
                    for (byte[] f : entry.fields()) {
                        fieldFrames.add(RespFrame.ofBulkString(f));
                    }
                    RespFrame idFrame = RespFrame.ofBulkString(entry.id().getBytes(StandardCharsets.US_ASCII));
                    RespFrame fieldsArray = RespFrame.ofArray(fieldFrames);
                    entryFrames.add(RespFrame.ofArray(List.of(idFrame, fieldsArray)));
                }
                return RespFrame.ofArray(entryFrames);
            } catch (IllegalStateException e) {
                return RespFrame.ofError(e.getMessage());
            }
        }
    }
}
