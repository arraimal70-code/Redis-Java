package com.redisclone.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * William Pugh's SkipList implementation with span indexing.
 * Faithfully mirrors Redis's zskiplist (server.h / t_zset.c).
 *
 * Provides:
 * - O(log N) insertion, deletion, and score updates.
 * - O(log N) rank determination and element lookup by rank using level spans.
 * - Bidirectional traversal via backward pointer at Level 0.
 */
public class SkipList {

    public static final int ZSKIPLIST_MAXLEVEL = 32;
    public static final double ZSKIPLIST_P = 0.25;

    public static class SkipListLevel {
        public SkipListNode forward;
        public int span;

        public SkipListLevel() {
            this.forward = null;
            this.span = 0;
        }
    }

    public static class SkipListNode {
        public final String member;
        public double score;
        public SkipListNode backward;
        public final SkipListLevel[] level;

        public SkipListNode(int levelCount, double score, String member) {
            this.score = score;
            this.member = member;
            this.backward = null;
            this.level = new SkipListLevel[levelCount];
            for (int i = 0; i < levelCount; i++) {
                this.level[i] = new SkipListLevel();
            }
        }
    }

    private final SkipListNode header;
    private SkipListNode tail;
    private long length;
    private int level;
    private final Random random = new Random();

    public SkipList() {
        this.level = 1;
        this.length = 0;
        this.header = new SkipListNode(ZSKIPLIST_MAXLEVEL, 0, null);
        for (int i = 0; i < ZSKIPLIST_MAXLEVEL; i++) {
            this.header.level[i].forward = null;
            this.header.level[i].span = 0;
        }
        this.header.backward = null;
        this.tail = null;
    }

    public long length() {
        return length;
    }

    public int getLevel() {
        return level;
    }

    public SkipListNode getHeader() {
        return header;
    }

    public SkipListNode getTail() {
        return tail;
    }

    private int randomLevel() {
        int lvl = 1;
        while ((random.nextInt() & 0xFFFF) < (ZSKIPLIST_P * 0xFFFF)) {
            lvl += 1;
        }
        return Math.min(lvl, ZSKIPLIST_MAXLEVEL);
    }

    /**
     * Inserts a new node into the skiplist.
     */
    public SkipListNode insert(double score, String member) {
        SkipListNode[] update = new SkipListNode[ZSKIPLIST_MAXLEVEL];
        int[] rank = new int[ZSKIPLIST_MAXLEVEL];
        SkipListNode x = header;

        for (int i = level - 1; i >= 0; i--) {
            rank[i] = (i == level - 1) ? 0 : rank[i + 1];
            while (x.level[i].forward != null &&
                    (x.level[i].forward.score < score ||
                            (x.level[i].forward.score == score && x.level[i].forward.member.compareTo(member) < 0))) {
                rank[i] += x.level[i].span;
                x = x.level[i].forward;
            }
            update[i] = x;
        }

        int newLevel = randomLevel();
        if (newLevel > level) {
            for (int i = level; i < newLevel; i++) {
                rank[i] = 0;
                update[i] = header;
                update[i].level[i].span = (int) length;
            }
            level = newLevel;
        }

        x = new SkipListNode(newLevel, score, member);
        for (int i = 0; i < newLevel; i++) {
            x.level[i].forward = update[i].level[i].forward;
            update[i].level[i].forward = x;

            x.level[i].span = update[i].level[i].span - (rank[0] - rank[i]);
            update[i].level[i].span = (rank[0] - rank[i]) + 1;
        }

        // Increment span for untouched upper levels
        for (int i = newLevel; i < level; i++) {
            update[i].level[i].span++;
        }

        x.backward = (update[0] == header) ? null : update[0];
        if (x.level[0].forward != null) {
            x.level[0].forward.backward = x;
        } else {
            tail = x;
        }
        length++;
        return x;
    }

    /**
     * Deletes a node with matching score and member from the skiplist.
     */
    public boolean delete(double score, String member) {
        SkipListNode[] update = new SkipListNode[ZSKIPLIST_MAXLEVEL];
        SkipListNode x = header;

        for (int i = level - 1; i >= 0; i--) {
            while (x.level[i].forward != null &&
                    (x.level[i].forward.score < score ||
                            (x.level[i].forward.score == score && x.level[i].forward.member.compareTo(member) < 0))) {
                x = x.level[i].forward;
            }
            update[i] = x;
        }

        x = x.level[0].forward;
        if (x != null && x.score == score && x.member.equals(member)) {
            deleteNode(x, update);
            return true;
        }
        return false;
    }

    private void deleteNode(SkipListNode x, SkipListNode[] update) {
        for (int i = 0; i < level; i++) {
            if (update[i].level[i].forward == x) {
                update[i].level[i].span += x.level[i].span - 1;
                update[i].level[i].forward = x.level[i].forward;
            } else {
                update[i].level[i].span -= 1;
            }
        }
        if (x.level[0].forward != null) {
            x.level[0].forward.backward = x.backward;
        } else {
            tail = x.backward;
        }
        while (level > 1 && header.level[level - 1].forward == null) {
            level--;
        }
        length--;
    }

    /**
     * Returns the 1-based rank of the element, or 0 if not found.
     * O(log N) complexity using span indexing.
     */
    public long getRank(double score, String member) {
        long rank = 0;
        SkipListNode x = header;

        for (int i = level - 1; i >= 0; i--) {
            while (x.level[i].forward != null &&
                    (x.level[i].forward.score < score ||
                            (x.level[i].forward.score == score && x.level[i].forward.member.compareTo(member) <= 0))) {
                rank += x.level[i].span;
                x = x.level[i].forward;
            }
            if (x.member != null && x.member.equals(member)) {
                return rank;
            }
        }
        return 0;
    }

    /**
     * Finds an element by its 1-based rank.
     * O(log N) complexity using span indexing.
     */
    public SkipListNode getNodeByRank(long rank) {
        if (rank <= 0 || rank > length) return null;
        long traversed = 0;
        SkipListNode x = header;

        for (int i = level - 1; i >= 0; i--) {
            while (x.level[i].forward != null && (traversed + x.level[i].span) <= rank) {
                traversed += x.level[i].span;
                x = x.level[i].forward;
            }
            if (traversed == rank) {
                return x;
            }
        }
        return null;
    }

    /**
     * Returns a range of nodes by rank range [startRank, endRank] (1-based, inclusive).
     */
    public List<SkipListNode> getRangeByRank(long startRank, long endRank) {
        List<SkipListNode> result = new ArrayList<>();
        if (startRank > endRank || startRank > length || endRank <= 0) {
            return result;
        }
        if (startRank < 1) startRank = 1;
        if (endRank > length) endRank = length;

        SkipListNode node = getNodeByRank(startRank);
        long count = endRank - startRank + 1;
        while (node != null && count > 0) {
            result.add(node);
            node = node.level[0].forward;
            count--;
        }
        return result;
    }

    /**
     * Returns a reverse range of nodes by reverse rank range [startRank, endRank] (1-based, inclusive).
     */
    public List<SkipListNode> getRevRangeByRank(long startRank, long endRank) {
        List<SkipListNode> result = new ArrayList<>();
        if (startRank > endRank || startRank > length || endRank <= 0) {
            return result;
        }
        if (startRank < 1) startRank = 1;
        if (endRank > length) endRank = length;

        // Convert reverse 1-based ranks to forward 1-based ranks
        long forwardStart = length - startRank + 1;
        SkipListNode node = getNodeByRank(forwardStart);
        long count = endRank - startRank + 1;
        while (node != null && count > 0) {
            result.add(node);
            node = node.backward;
            count--;
        }
        return result;
    }

    /**
     * Counts elements whose score is within [minScore, maxScore].
     */
    public long countRange(double minScore, double maxScore) {
        if (minScore > maxScore || length == 0) return 0;
        long count = 0;
        SkipListNode x = header.level[0].forward;
        while (x != null) {
            if (x.score >= minScore && x.score <= maxScore) {
                count++;
            } else if (x.score > maxScore) {
                break;
            }
            x = x.level[0].forward;
        }
        return count;
    }
}
