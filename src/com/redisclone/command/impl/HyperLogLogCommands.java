package com.redisclone.command.impl;

import com.redisclone.command.Command;
import com.redisclone.command.CommandContext;
import com.redisclone.resp.RespFrame;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class HyperLogLogCommands {

    public static class PfAddCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.size() < 2) {
                return RespFrame.ofError("ERR wrong number of arguments for 'pfadd' command");
            }
            String key = new String(args.get(0), StandardCharsets.UTF_8);
            List<byte[]> elements = new ArrayList<>(args.size() - 1);
            for (int i = 1; i < args.size(); i++) {
                elements.add(args.get(i));
            }

            boolean updated = ctx.getDataStore().pfadd(key, elements);
            return RespFrame.ofInteger(updated ? 1 : 0);
        }

        @Override
        public boolean isWriteCommand() {
            return true;
        }
    }

    public static class PfCountCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.isEmpty()) {
                return RespFrame.ofError("ERR wrong number of arguments for 'pfcount' command");
            }
            List<String> keys = new ArrayList<>(args.size());
            for (byte[] arg : args) {
                keys.add(new String(arg, StandardCharsets.UTF_8));
            }

            long cardinality = ctx.getDataStore().pfcount(keys);
            return RespFrame.ofInteger(cardinality);
        }
    }
}
