package com.redisclone.pubsub;

import com.redisclone.network.ClientConnection;
import com.redisclone.resp.RespEncoder;
import com.redisclone.resp.RespFrame;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * High-performance Pub/Sub channel and glob-pattern subscription engine.
 * Supports exact topic subscriptions, pattern matching (*, ?, [abc]),
 * client session state tracking, and RESP push framing (*3\r\n$7\r\nmessage... / *4\r\n$8\r\npmessage...).
 */
public class PubSubManager {

    // Maps channel name -> Set of active subscribed ClientConnections
    private final ConcurrentHashMap<String, Set<ClientConnection>> channelSubscribers = new ConcurrentHashMap<>();

    // Maps glob pattern -> Set of active subscribed ClientConnections
    private final ConcurrentHashMap<String, Set<ClientConnection>> patternSubscribers = new ConcurrentHashMap<>();

    public PubSubManager() {}

    /**
     * Subscribes a client to a channel and immediately transmits the RESP subscription confirmation.
     * Confirmation format:
     * *3\r\n$9\r\nsubscribe\r\n$<channelLen>\r\n<channel>\r\n:<subscribedCount>\r\n
     */
    public synchronized void subscribe(ClientConnection client, String channel) {
        channelSubscribers.computeIfAbsent(channel, k -> Collections.synchronizedSet(new HashSet<>())).add(client);
        client.addSubscription(channel);

        int count = client.getSubscribedChannels().size() + client.getSubscribedPatterns().size();
        List<RespFrame> confirmation = List.of(
                RespFrame.ofBulkString("subscribe"),
                RespFrame.ofBulkString(channel),
                RespFrame.ofInteger(count)
        );
        client.sendReply(RespFrame.ofArray(confirmation));
    }

    /**
     * Unsubscribes a client from a specific channel.
     */
    public synchronized void unsubscribe(ClientConnection client, String channel) {
        Set<ClientConnection> clients = channelSubscribers.get(channel);
        if (clients != null) {
            clients.remove(client);
            if (clients.isEmpty()) {
                channelSubscribers.remove(channel);
            }
        }
        client.removeSubscription(channel);

        int count = client.getSubscribedChannels().size() + client.getSubscribedPatterns().size();
        List<RespFrame> confirmation = List.of(
                RespFrame.ofBulkString("unsubscribe"),
                RespFrame.ofBulkString(channel),
                RespFrame.ofInteger(count)
        );
        client.sendReply(RespFrame.ofArray(confirmation));
    }

    /**
     * Subscribes a client to a glob pattern and transmits the RESP confirmation:
     * *3\r\n$10\r\npsubscribe\r\n$<patLen>\r\n<pattern>\r\n:<subscribedCount>\r\n
     */
    public synchronized void psubscribe(ClientConnection client, String pattern) {
        patternSubscribers.computeIfAbsent(pattern, k -> Collections.synchronizedSet(new HashSet<>())).add(client);
        client.addPatternSubscription(pattern);

        int count = client.getSubscribedChannels().size() + client.getSubscribedPatterns().size();
        List<RespFrame> confirmation = List.of(
                RespFrame.ofBulkString("psubscribe"),
                RespFrame.ofBulkString(pattern),
                RespFrame.ofInteger(count)
        );
        client.sendReply(RespFrame.ofArray(confirmation));
    }

    /**
     * Unsubscribes a client from a glob pattern:
     * *3\r\n$12\r\npunsubscribe\r\n$<patLen>\r\n<pattern>\r\n:<subscribedCount>\r\n
     */
    public synchronized void punsubscribe(ClientConnection client, String pattern) {
        Set<ClientConnection> clients = patternSubscribers.get(pattern);
        if (clients != null) {
            clients.remove(client);
            if (clients.isEmpty()) {
                patternSubscribers.remove(pattern);
            }
        }
        client.removePatternSubscription(pattern);

        int count = client.getSubscribedChannels().size() + client.getSubscribedPatterns().size();
        List<RespFrame> confirmation = List.of(
                RespFrame.ofBulkString("punsubscribe"),
                RespFrame.ofBulkString(pattern),
                RespFrame.ofInteger(count)
        );
        client.sendReply(RespFrame.ofArray(confirmation));
    }

    /**
     * Publishes a message to all active subscribers of a channel AND all matching pattern subscribers.
     * Exact channel push: *3\r\n$7\r\nmessage\r\n$<chanLen>\r\n<channel>\r\n$<msgLen>\r\n<message>\r\n
     * Pattern push:       *4\r\n$8\r\npmessage\r\n$<patLen>\r\n<pattern>\r\n$<chanLen>\r\n<channel>\r\n$<msgLen>\r\n<message>\r\n
     *
     * @return The total number of clients that received the message.
     */
    public int publish(String channel, byte[] message) {
        int receiverCount = 0;

        // 1. Direct channel subscribers
        Set<ClientConnection> directSubscribers = channelSubscribers.get(channel);
        if (directSubscribers != null && !directSubscribers.isEmpty()) {
            List<RespFrame> pushMessage = List.of(
                    RespFrame.ofBulkString("message"),
                    RespFrame.ofBulkString(channel),
                    RespFrame.ofBulkString(message)
            );
            byte[] encoded = RespEncoder.encode(RespFrame.ofArray(pushMessage));

            synchronized (directSubscribers) {
                for (ClientConnection client : directSubscribers) {
                    try {
                        client.sendRawBytes(encoded);
                        receiverCount++;
                    } catch (Exception ignored) {}
                }
            }
        }

        // 2. Pattern subscribers matching this channel
        for (Map.Entry<String, Set<ClientConnection>> entry : patternSubscribers.entrySet()) {
            String pattern = entry.getKey();
            if (stringMatch(pattern, channel)) {
                Set<ClientConnection> patSubscribers = entry.getValue();
                if (patSubscribers != null && !patSubscribers.isEmpty()) {
                    List<RespFrame> pushPMessage = List.of(
                            RespFrame.ofBulkString("pmessage"),
                            RespFrame.ofBulkString(pattern),
                            RespFrame.ofBulkString(channel),
                            RespFrame.ofBulkString(message)
                    );
                    byte[] encodedPattern = RespEncoder.encode(RespFrame.ofArray(pushPMessage));

                    synchronized (patSubscribers) {
                        for (ClientConnection client : patSubscribers) {
                            try {
                                client.sendRawBytes(encodedPattern);
                                receiverCount++;
                            } catch (Exception ignored) {}
                        }
                    }
                }
            }
        }

        return receiverCount;
    }

    /**
     * Automatically purges client from all subscribed channels and patterns upon socket disconnect.
     */
    public void onClientDisconnected(ClientConnection client) {
        Set<String> channels = new HashSet<>(client.getSubscribedChannels());
        for (String channel : channels) {
            unsubscribe(client, channel);
        }
        Set<String> patterns = new HashSet<>(client.getSubscribedPatterns());
        for (String pattern : patterns) {
            punsubscribe(client, pattern);
        }
    }

    /**
     * Glob pattern matching supporting '*', '?', and '[abc]' character ranges.
     * Mirrors stringmatchlen from Redis util.c.
     */
    public static boolean stringMatch(String pattern, String str) {
        return stringMatchLen(pattern.toCharArray(), 0, str.toCharArray(), 0);
    }

    private static boolean stringMatchLen(char[] p, int pi, char[] s, int si) {
        while (pi < p.length) {
            if (p[pi] == '*') {
                while (pi + 1 < p.length && p[pi + 1] == '*') {
                    pi++;
                }
                if (pi + 1 == p.length) return true;
                while (si < s.length) {
                    if (stringMatchLen(p, pi + 1, s, si)) return true;
                    si++;
                }
                return false;
            } else if (p[pi] == '?') {
                if (si == s.length) return false;
                si++;
                pi++;
            } else if (p[pi] == '[') {
                if (si == s.length) return false;
                pi++;
                boolean match = false;
                boolean not = false;
                if (p[pi] == '^') {
                    not = true;
                    pi++;
                }
                while (pi < p.length && p[pi] != ']') {
                    if (pi + 2 < p.length && p[pi + 1] == '-') {
                        if (s[si] >= p[pi] && s[si] <= p[pi + 2]) match = true;
                        pi += 3;
                    } else {
                        if (s[si] == p[pi]) match = true;
                        pi++;
                    }
                }
                if (not) match = !match;
                if (!match) return false;
                if (pi < p.length) pi++; // Skip ']'
                si++;
            } else {
                if (si == s.length || p[pi] != s[si]) return false;
                pi++;
                si++;
            }
        }
        return si == s.length;
    }
}
