package com.redisclone.pubsub;

import com.redisclone.network.ClientConnection;
import com.redisclone.resp.RespEncoder;
import com.redisclone.resp.RespFrame;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * High-performance Pub/Sub channel subscription and atomic message dispatch engine.
 * Supports thread-safe topic subscriptions, client session state tracking,
 * and RESP push framing (*3\r\n$7\r\nmessage...).
 */
public class PubSubManager {

    // Maps channel name -> Set of active subscribed ClientConnections
    private final ConcurrentHashMap<String, Set<ClientConnection>> channelSubscribers = new ConcurrentHashMap<>();

    public PubSubManager() {}

    /**
     * Subscribes a client to a channel and immediately transmits the RESP subscription confirmation.
     * Confirmation format:
     * *3\r\n
     * $9\r\nsubscribe\r\n
     * $<channelLen>\r\n<channel>\r\n
     * :<subscribedChannelsCount>\r\n
     */
    public synchronized void subscribe(ClientConnection client, String channel) {
        channelSubscribers.computeIfAbsent(channel, k -> Collections.synchronizedSet(new HashSet<>())).add(client);
        client.addSubscription(channel);

        int count = client.getSubscribedChannels().size();
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

        int count = client.getSubscribedChannels().size();
        List<RespFrame> confirmation = List.of(
                RespFrame.ofBulkString("unsubscribe"),
                RespFrame.ofBulkString(channel),
                RespFrame.ofInteger(count)
        );
        client.sendReply(RespFrame.ofArray(confirmation));
    }

    /**
     * Publishes a message to all active subscribers of a channel.
     * Message push frame format:
     * *3\r\n
     * $7\r\nmessage\r\n
     * $<channelLen>\r\n<channel>\r\n
     * $<msgLen>\r\n<message>\r\n
     *
     * @return The number of clients that received the message.
     */
    public int publish(String channel, byte[] message) {
        Set<ClientConnection> subscribers = channelSubscribers.get(channel);
        if (subscribers == null || subscribers.isEmpty()) {
            return 0;
        }

        List<RespFrame> pushMessage = List.of(
                RespFrame.ofBulkString("message"),
                RespFrame.ofBulkString(channel),
                RespFrame.ofBulkString(message)
        );
        byte[] encoded = RespEncoder.encode(RespFrame.ofArray(pushMessage));

        int receiverCount = 0;
        synchronized (subscribers) {
            for (ClientConnection client : subscribers) {
                try {
                    client.sendRawBytes(encoded);
                    receiverCount++;
                } catch (Exception ignored) {}
            }
        }
        return receiverCount;
    }

    /**
     * Automatically purges client from all subscribed channels upon socket disconnect.
     */
    public void onClientDisconnected(ClientConnection client) {
        Set<String> channels = new HashSet<>(client.getSubscribedChannels());
        for (String channel : channels) {
            unsubscribe(client, channel);
        }
    }
}
