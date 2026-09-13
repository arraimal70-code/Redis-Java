package com.redisclone.command.impl;

import com.redisclone.command.Command;
import com.redisclone.command.CommandContext;
import com.redisclone.command.TransactionContext;
import com.redisclone.resp.RespFrame;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * ACID/CAS Transaction command implementations:
 * MULTI, EXEC, DISCARD, WATCH, UNWATCH.
 */
public class TransactionCommands {

    public static class MultiCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (ctx.getClient() == null) {
                return RespFrame.ofError("ERR client context required");
            }
            TransactionContext tx = ctx.getClient().getTransactionContext();
            if (tx.isInTransaction()) {
                return RespFrame.ofError("ERR MULTI calls can not be nested");
            }
            tx.startTransaction();
            return RespFrame.ofSimpleString("OK");
        }
    }

    public static class DiscardCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (ctx.getClient() == null) {
                return RespFrame.ofError("ERR client context required");
            }
            TransactionContext tx = ctx.getClient().getTransactionContext();
            if (!tx.isInTransaction()) {
                return RespFrame.ofError("ERR DISCARD without MULTI");
            }
            tx.discard();
            return RespFrame.ofSimpleString("OK");
        }
    }

    public static class WatchCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (ctx.getClient() == null) {
                return RespFrame.ofError("ERR client context required");
            }
            TransactionContext tx = ctx.getClient().getTransactionContext();
            if (tx.isInTransaction()) {
                return RespFrame.ofError("ERR WATCH inside MULTI is not allowed");
            }
            if (args.isEmpty()) {
                return RespFrame.ofError("ERR wrong number of arguments for 'watch' command");
            }

            for (byte[] arg : args) {
                String key = new String(arg, StandardCharsets.UTF_8);
                long currentVersion = ctx.getDataStore().getKeyVersion(key);
                tx.watch(key, currentVersion);
            }
            return RespFrame.ofSimpleString("OK");
        }
    }

    public static class UnwatchCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (ctx.getClient() != null) {
                ctx.getClient().getTransactionContext().unwatch();
            }
            return RespFrame.ofSimpleString("OK");
        }
    }

    public static class ExecCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (ctx.getClient() == null) {
                return RespFrame.ofError("ERR client context required");
            }
            TransactionContext tx = ctx.getClient().getTransactionContext();
            if (!tx.isInTransaction()) {
                return RespFrame.ofError("ERR EXEC without MULTI");
            }

            // Check if any watched key was modified (Optimistic locking CAS check)
            if (tx.isDirty(ctx.getDataStore())) {
                tx.discard();
                return RespFrame.ofNullArray(); // Redis specification returns Null Array on WATCH failure
            }

            List<RespFrame.Array> queued = tx.getQueuedCommands();
            tx.complete();

            List<RespFrame> replies = new ArrayList<>(queued.size());
            for (RespFrame.Array cmd : queued) {
                RespFrame reply = ctx.getCommandRegistry().executeDirect(ctx.getClient(), cmd);
                replies.add(reply != null ? reply : RespFrame.ofSimpleString("OK"));
            }

            return RespFrame.ofArray(replies);
        }
    }
}
