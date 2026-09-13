package com.redisclone.network;

import com.redisclone.resp.RespEncoder;
import com.redisclone.resp.RespFrame;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Encapsulates the lifecycle, connection state, read/write direct buffers,
 * and Pub/Sub channel subscriptions for an individual client TCP connection.
 */
public class ClientConnection {

    private static final int INITIAL_BUFFER_CAPACITY = 64 * 1024; // 64 KB

    private final SocketChannel channel;
    private final SelectionKey key;
    private final SocketAddress remoteAddress;

    // Buffer for reading incoming bytes from TCP stream
    private ByteBuffer readBuffer;

    // Queue of buffers awaiting transmission over the socket
    private final Queue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<>();

    // Tracks Pub/Sub subscriptions for this client
    private final Set<String> subscribedChannels = Collections.synchronizedSet(new HashSet<>());

    // Transaction state for MULTI/EXEC/WATCH
    private final com.redisclone.command.TransactionContext transactionContext = new com.redisclone.command.TransactionContext();

    public ClientConnection(SocketChannel channel, SelectionKey key) throws IOException {
        this.channel = channel;
        this.key = key;
        this.remoteAddress = channel.getRemoteAddress();
        this.readBuffer = ByteBuffer.allocate(INITIAL_BUFFER_CAPACITY);
    }

    public com.redisclone.command.TransactionContext getTransactionContext() {
        return transactionContext;
    }

    public SocketChannel getChannel() {
        return channel;
    }

    public SelectionKey getKey() {
        return key;
    }

    public SocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    public ByteBuffer getReadBuffer() {
        return readBuffer;
    }

    /**
     * Reads available bytes from the socket channel into the readBuffer.
     * Expands the buffer if full, or compacts if data remains.
     *
     * @return Number of bytes read, or -1 if the client closed the connection.
     */
    public int readFromChannel() throws IOException {
        if (!readBuffer.hasRemaining()) {
            // Buffer is full; expand capacity
            expandReadBuffer();
        }
        return channel.read(readBuffer);
    }

    private void expandReadBuffer() {
        ByteBuffer newBuffer = ByteBuffer.allocate(readBuffer.capacity() * 2);
        readBuffer.flip();
        newBuffer.put(readBuffer);
        readBuffer = newBuffer;
    }

    /**
     * Prepares the read buffer for reading parsed frames (flips to read-mode).
     */
    public void prepareForParsing() {
        readBuffer.flip();
    }

    /**
     * Restores the read buffer for subsequent network reads (compacts remaining bytes).
     */
    public void resumeAfterParsing() {
        readBuffer.compact();
    }

    /**
     * Enqueues a RESP frame to be sent back to this client.
     */
    public void sendReply(RespFrame frame) {
        byte[] bytes = RespEncoder.encode(frame);
        sendRawBytes(bytes);
    }

    /**
     * Enqueues raw bytes directly into the write queue and signals interest in OP_WRITE.
     */
    public void sendRawBytes(byte[] bytes) {
        writeQueue.add(ByteBuffer.wrap(bytes));
        enableWriteInterest();
    }

    public void enableWriteInterest() {
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            key.selector().wakeup();
        }
    }

    /**
     * Flushes queued write buffers to the socket channel without blocking.
     *
     * @return true if all queued writes were completed, false if socket buffer is full.
     */
    public boolean flushWrites() throws IOException {
        while (!writeQueue.isEmpty()) {
            ByteBuffer buffer = writeQueue.peek();
            channel.write(buffer);
            if (buffer.hasRemaining()) {
                // OS TCP send buffer is full; keep OP_WRITE set
                return false;
            }
            writeQueue.poll();
        }

        // Write queue completely drained; disable OP_WRITE interest to avoid busy-spin
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
        }
        return true;
    }

    // --- Pub/Sub State ---

    public boolean isSubscribed() {
        return !subscribedChannels.isEmpty();
    }

    public Set<String> getSubscribedChannels() {
        return subscribedChannels;
    }

    public void addSubscription(String channelName) {
        subscribedChannels.add(channelName);
    }

    public void removeSubscription(String channelName) {
        subscribedChannels.remove(channelName);
    }

    public void close() {
        try {
            if (key != null) key.cancel();
            if (channel != null && channel.isOpen()) channel.close();
        } catch (IOException ignored) {}
    }
}
