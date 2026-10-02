package com.redisclone.storage;

import java.util.*;

/**
 * Production Redis Sorted Set (ZSET) implementation.
 * Combines an O(1) hash dictionary with William Pugh's O(log N) SkipList.
 */
public class SortedSet {

    private final Map<String, Double> dict = new HashMap<>();
    private final SkipList skiplist = new SkipList();

    public synchronized int add(String member, double score) {
        Double oldScore = dict.get(member);
        if (oldScore != null) {
            if (oldScore == score) {
                return 0; // Unchanged
            }
            skiplist.delete(oldScore, member);
            skiplist.insert(score, member);
            dict.put(member, score);
            return 0; // Updated
        } else {
            skiplist.insert(score, member);
            dict.put(member, score);
            return 1; // Newly added
        }
    }

    public synchronized Double score(String member) {
        return dict.get(member);
    }

    public synchronized long card() {
        return dict.size();
    }

    public synchronized boolean remove(String member) {
        Double oldScore = dict.remove(member);
        if (oldScore != null) {
            skiplist.delete(oldScore, member);
            return true;
        }
        return false;
    }

    public synchronized long rank(String member) {
        Double score = dict.get(member);
        if (score == null) return -1;
        long rank = skiplist.getRank(score, member);
        return rank > 0 ? rank - 1 : -1; // Convert 1-based to 0-based
    }

    public synchronized long revrank(String member) {
        Double score = dict.get(member);
        if (score == null) return -1;
        long rank = skiplist.getRank(score, member);
        return rank > 0 ? skiplist.length() - rank : -1; // 0-based from top
    }

    public synchronized long count(double min, double max) {
        return skiplist.countRange(min, max);
    }

    public synchronized List<String> range(long start, long stop, boolean withScores) {
        long len = skiplist.length();
        if (len == 0) return Collections.emptyList();

        if (start < 0) start = Math.max(0, len + start);
        if (stop < 0) stop = len + stop;
        if (start >= len || start > stop) return Collections.emptyList();
        stop = Math.min(len - 1, stop);

        // Convert 0-based indexes to 1-based ranks
        List<SkipList.SkipListNode> nodes = skiplist.getRangeByRank(start + 1, stop + 1);
        List<String> result = new ArrayList<>();
        for (SkipList.SkipListNode n : nodes) {
            result.add(n.member);
            if (withScores) {
                result.add(formatScore(n.score));
            }
        }
        return result;
    }

    public synchronized List<String> revrange(long start, long stop, boolean withScores) {
        long len = skiplist.length();
        if (len == 0) return Collections.emptyList();

        if (start < 0) start = Math.max(0, len + start);
        if (stop < 0) stop = len + stop;
        if (start >= len || start > stop) return Collections.emptyList();
        stop = Math.min(len - 1, stop);

        List<SkipList.SkipListNode> nodes = skiplist.getRevRangeByRank(start + 1, stop + 1);
        List<String> result = new ArrayList<>();
        for (SkipList.SkipListNode n : nodes) {
            result.add(n.member);
            if (withScores) {
                result.add(formatScore(n.score));
            }
        }
        return result;
    }

    public synchronized Map<String, Double> getDictSnapshot() {
        return new HashMap<>(dict);
    }

    public SkipList getSkipList() {
        return skiplist;
    }

    private static String formatScore(double score) {
        if (score == (long) score) {
            return String.format(Locale.US, "%d", (long) score);
        } else {
            return String.format(Locale.US, "%f", score);
        }
    }
}
