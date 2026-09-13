package com.redisclone.command.impl;

import com.redisclone.cluster.ClusterSlotRouter;
import com.redisclone.cluster.Crc16;
import com.redisclone.command.Command;
import com.redisclone.command.CommandContext;
import com.redisclone.resp.RespFrame;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class ClusterCommands {

    public static class ClusterCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.isEmpty()) {
                return RespFrame.ofError("ERR wrong number of arguments for 'cluster' command");
            }

            String subCommand = new String(args.get(0), StandardCharsets.UTF_8).toUpperCase(Locale.ROOT);

            switch (subCommand) {
                case "KEYSLOT" -> {
                    if (args.size() != 2) {
                        return RespFrame.ofError("ERR wrong number of arguments for 'cluster keyslot'");
                    }
                    String key = new String(args.get(1), StandardCharsets.UTF_8);
                    int slot = Crc16.getSlot(key);
                    return RespFrame.ofInteger(slot);
                }
                case "SLOTS" -> {
                    List<ClusterSlotRouter.ClusterNode> nodes = ctx.getClusterSlotRouter().getClusterTopology();
                    List<RespFrame> result = new ArrayList<>();
                    for (ClusterSlotRouter.ClusterNode node : nodes) {
                        List<RespFrame> slotRange = List.of(
                                RespFrame.ofInteger(node.startSlot()),
                                RespFrame.ofInteger(node.endSlot()),
                                RespFrame.ofArray(List.of(
                                        RespFrame.ofBulkString(node.host()),
                                        RespFrame.ofInteger(node.port()),
                                        RespFrame.ofBulkString(node.id())
                                ))
                        );
                        result.add(RespFrame.ofArray(slotRange));
                    }
                    return RespFrame.ofArray(result);
                }
                case "NODES" -> {
                    StringBuilder sb = new StringBuilder();
                    for (ClusterSlotRouter.ClusterNode node : ctx.getClusterSlotRouter().getClusterTopology()) {
                        sb.append(node.id()).append(" ")
                                .append(node.host()).append(":").append(node.port())
                                .append("@").append(node.port() + 10000).append(" ")
                                .append("master - 0 0 connected ")
                                .append(node.startSlot()).append("-").append(node.endSlot())
                                .append("\n");
                    }
                    return RespFrame.ofBulkString(sb.toString());
                }
                default -> {
                    return RespFrame.ofError("ERR unknown subcommand or wrong number of arguments for 'cluster'");
                }
            }
        }
    }
}
