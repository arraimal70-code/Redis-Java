package com.redisclone.command.impl;

import com.redisclone.command.Command;
import com.redisclone.command.CommandContext;
import com.redisclone.resp.RespFrame;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class ZSetCommands {

    public static class ZAddCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            // ZADD key score member [score member ...]
            if (args.size() < 3 || (args.size() - 1) % 2 != 0) {
                return RespFrame.ofError("ERR wrong number of arguments for 'zadd' command");
            }
            String key = new String(args.get(0), StandardCharsets.UTF_8);
            Map<String, Double> scoreMembers = new LinkedHashMap<>();

            try {
                for (int i = 1; i < args.size(); i += 2) {
                    double score = Double.parseDouble(new String(args.get(i), StandardCharsets.US_ASCII));
                    String member = new String(args.get(i + 1), StandardCharsets.UTF_8);
                    scoreMembers.put(member, score);
                }
            } catch (NumberFormatException e) {
                return RespFrame.ofError("ERR value is not a valid float");
            }

            try {
                int added = ctx.getDataStore().zadd(key, scoreMembers);
                return RespFrame.ofInteger(added);
            } catch (IllegalStateException e) {
                return RespFrame.ofError(e.getMessage());
            }
        }

        @Override
        public boolean isWriteCommand() {
            return true;
        }
    }

    public static class ZScoreCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.size() != 2) {
                return RespFrame.ofError("ERR wrong number of arguments for 'zscore' command");
            }
            String key = new String(args.get(0), StandardCharsets.UTF_8);
            String member = new String(args.get(1), StandardCharsets.UTF_8);

            try {
                Double score = ctx.getDataStore().zscore(key, member);
                if (score == null) {
                    return RespFrame.ofNullBulkString();
                }
                String scoreStr = (score == (long) score.doubleValue()) ?
                        String.valueOf((long) score.doubleValue()) : String.valueOf(score);
                return RespFrame.ofBulkString(scoreStr.getBytes(StandardCharsets.US_ASCII));
            } catch (IllegalStateException e) {
                return RespFrame.ofError(e.getMessage());
            }
        }
    }

    public static class ZCardCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.size() != 1) {
                return RespFrame.ofError("ERR wrong number of arguments for 'zcard' command");
            }
            String key = new String(args.get(0), StandardCharsets.UTF_8);
            try {
                long card = ctx.getDataStore().zcard(key);
                return RespFrame.ofInteger(card);
            } catch (IllegalStateException e) {
                return RespFrame.ofError(e.getMessage());
            }
        }
    }

    public static class ZCountCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.size() != 3) {
                return RespFrame.ofError("ERR wrong number of arguments for 'zcount' command");
            }
            String key = new String(args.get(0), StandardCharsets.UTF_8);
            try {
                double min = parseScoreBound(new String(args.get(1), StandardCharsets.US_ASCII));
                double max = parseScoreBound(new String(args.get(2), StandardCharsets.US_ASCII));
                long count = ctx.getDataStore().zcount(key, min, max);
                return RespFrame.ofInteger(count);
            } catch (NumberFormatException e) {
                return RespFrame.ofError("ERR min or max is not a float");
            } catch (IllegalStateException e) {
                return RespFrame.ofError(e.getMessage());
            }
        }
    }

    public static class ZRankCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.size() != 2) {
                return RespFrame.ofError("ERR wrong number of arguments for 'zrank' command");
            }
            String key = new String(args.get(0), StandardCharsets.UTF_8);
            String member = new String(args.get(1), StandardCharsets.UTF_8);

            try {
                long rank = ctx.getDataStore().zrank(key, member);
                if (rank == -1) {
                    return RespFrame.ofNullBulkString();
                }
                return RespFrame.ofInteger(rank);
            } catch (IllegalStateException e) {
                return RespFrame.ofError(e.getMessage());
            }
        }
    }

    public static class ZRevRankCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.size() != 2) {
                return RespFrame.ofError("ERR wrong number of arguments for 'zrevrank' command");
            }
            String key = new String(args.get(0), StandardCharsets.UTF_8);
            String member = new String(args.get(1), StandardCharsets.UTF_8);

            try {
                long rank = ctx.getDataStore().zrevrank(key, member);
                if (rank == -1) {
                    return RespFrame.ofNullBulkString();
                }
                return RespFrame.ofInteger(rank);
            } catch (IllegalStateException e) {
                return RespFrame.ofError(e.getMessage());
            }
        }
    }

    public static class ZRangeCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            // ZRANGE key start stop [WITHSCORES]
            if (args.size() < 3) {
                return RespFrame.ofError("ERR wrong number of arguments for 'zrange' command");
            }
            String key = new String(args.get(0), StandardCharsets.UTF_8);
            try {
                long start = Long.parseLong(new String(args.get(1), StandardCharsets.US_ASCII));
                long stop = Long.parseLong(new String(args.get(2), StandardCharsets.US_ASCII));
                boolean withScores = args.size() >= 4 &&
                        "WITHSCORES".equalsIgnoreCase(new String(args.get(3), StandardCharsets.US_ASCII));

                List<String> items = ctx.getDataStore().zrange(key, start, stop, withScores);
                List<RespFrame> frames = new ArrayList<>(items.size());
                for (String s : items) {
                    frames.add(RespFrame.ofBulkString(s));
                }
                return RespFrame.ofArray(frames);
            } catch (NumberFormatException e) {
                return RespFrame.ofError("ERR value is not an integer or out of range");
            } catch (IllegalStateException e) {
                return RespFrame.ofError(e.getMessage());
            }
        }
    }

    public static class ZRevRangeCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            // ZREVRANGE key start stop [WITHSCORES]
            if (args.size() < 3) {
                return RespFrame.ofError("ERR wrong number of arguments for 'zrevrange' command");
            }
            String key = new String(args.get(0), StandardCharsets.UTF_8);
            try {
                long start = Long.parseLong(new String(args.get(1), StandardCharsets.US_ASCII));
                long stop = Long.parseLong(new String(args.get(2), StandardCharsets.US_ASCII));
                boolean withScores = args.size() >= 4 &&
                        "WITHSCORES".equalsIgnoreCase(new String(args.get(3), StandardCharsets.US_ASCII));

                List<String> items = ctx.getDataStore().zrevrange(key, start, stop, withScores);
                List<RespFrame> frames = new ArrayList<>(items.size());
                for (String s : items) {
                    frames.add(RespFrame.ofBulkString(s));
                }
                return RespFrame.ofArray(frames);
            } catch (NumberFormatException e) {
                return RespFrame.ofError("ERR value is not an integer or out of range");
            } catch (IllegalStateException e) {
                return RespFrame.ofError(e.getMessage());
            }
        }
    }

    public static class ZRemCommand implements Command {
        @Override
        public RespFrame execute(CommandContext ctx, List<byte[]> args) {
            if (args.size() < 2) {
                return RespFrame.ofError("ERR wrong number of arguments for 'zrem' command");
            }
            String key = new String(args.get(0), StandardCharsets.UTF_8);
            List<String> members = new ArrayList<>(args.size() - 1);
            for (int i = 1; i < args.size(); i++) {
                members.add(new String(args.get(i), StandardCharsets.UTF_8));
            }

            try {
                int removed = ctx.getDataStore().zrem(key, members);
                return RespFrame.ofInteger(removed);
            } catch (IllegalStateException e) {
                return RespFrame.ofError(e.getMessage());
            }
        }

        @Override
        public boolean isWriteCommand() {
            return true;
        }
    }

    private static double parseScoreBound(String bound) {
        if ("-inf".equalsIgnoreCase(bound)) return Double.NEGATIVE_INFINITY;
        if ("+inf".equalsIgnoreCase(bound) || "inf".equalsIgnoreCase(bound)) return Double.POSITIVE_INFINITY;
        return Double.parseDouble(bound);
    }
}
