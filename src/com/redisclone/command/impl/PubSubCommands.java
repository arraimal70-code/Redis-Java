package com.redisclone.command.impl;

import com.redisclone.command.Command;
import com.redisclone.command.CommandContext;
import com.redisclone.resp.RespFrame;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class PubSubCommands {

    public static class PublishCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.size() != 2) {
                return RespFrame.ofError("ERR wrong number of arguments for 'publish' command");
            }

            String channel = new String(args.get(0), StandardCharsets.UTF_8);
            byte[] message = args.get(1);

            int receiverCount = ctx.getPubSubManager().publish(channel, message);
            return RespFrame.ofInteger(receiverCount);
        }
    }

    public static class SubscribeCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.isEmpty()) {
                return RespFrame.ofError("ERR wrong number of arguments for 'subscribe' command");
            }

            if (ctx.getClient() == null) {
                return RespFrame.ofError("ERR client connection context missing");
            }

            for (byte[] arg : args) {
                String channel = new String(arg, StandardCharsets.UTF_8);
                ctx.getPubSubManager().subscribe(ctx.getClient(), channel);
            }

            // The subscription push confirmations are written directly to the client connection
            return null;
        }
    }

    public static class UnsubscribeCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (ctx.getClient() == null) {
                return RespFrame.ofError("ERR client connection context missing");
            }

            if (args.isEmpty()) {
                // Unsubscribe from all channels
                for (String channel : List.copyOf(ctx.getClient().getSubscribedChannels())) {
                    ctx.getPubSubManager().unsubscribe(ctx.getClient(), channel);
                }
            } else {
                for (byte[] arg : args) {
                    String channel = new String(arg, StandardCharsets.UTF_8);
                    ctx.getPubSubManager().unsubscribe(ctx.getClient(), channel);
                }
            }

            return null;
        }
    }
}
