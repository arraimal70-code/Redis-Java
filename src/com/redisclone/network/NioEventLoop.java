package com.redisclone.network;

import com.redisclone.resp.RespParser;
import com.redisclone.resp.RespFrame;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Reactor Pattern Event Loop implementation using standard Java NIO.
 * Manages TCP connection acceptance, non-blocking I/O multiplexing,
 * and dispatching incoming frames to the command engine.
 */
public class NioEventLoop implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(NioEventLoop.class.getName());

    private final String host;
    private final int port;
    private final FrameHandler frameHandler;

    private Selector selector;
    private ServerSocketChannel serverChannel;
    private volatile boolean running = false;
    private Thread loopThread;

    public NioEventLoop(String host, int port, FrameHandler frameHandler) {
        this.host = host;
        this.port = port;
        this.frameHandler = frameHandler;
    }

    public synchronized void start() throws IOException {
        if (running) return;

        this.selector = Selector.open();
        this.serverChannel = ServerSocketChannel.open();
        this.serverChannel.configureBlocking(false);
        this.serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        this.serverChannel.bind(new InetSocketAddress(host, port));
        this.serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        this.running = true;
        this.loopThread = new Thread(this, "redis-nio-reactor");
        this.loopThread.start();
        LOGGER.info("Redis Server listening on " + host + ":" + port + " [Java NIO Reactor]");
    }

    @Override
    public void run() {
        while (running) {
            try {
                int selected = selector.select(100); // 100ms timeout for periodic maintenance
                if (selected == 0) {
                    continue;
                }

                Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                while (it.hasNext()) {
                    SelectionKey key = it.next();
                    it.remove();

                    if (!key.isValid()) {
                        continue;
                    }

                    try {
                        if (key.isAcceptable()) {
                            handleAccept();
                        }
                        if (key.isReadable()) {
                            handleRead(key);
                        }
                        if (key.isValid() && key.isWritable()) {
                            handleWrite(key);
                        }
                    } catch (IOException e) {
                        // Connection reset or client aborted
                        closeConnection(key);
                    }
                }
            } catch (ClosedSelectorException e) {
                break;
            } catch (Throwable t) {
                LOGGER.log(Level.SEVERE, "Unexpected error in NIO Reactor Event Loop", t);
            }
        }
    }

    private void handleAccept() throws IOException {
        SocketChannel clientChannel = serverChannel.accept();
        if (clientChannel == null) return;

        clientChannel.configureBlocking(false);
        clientChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
        clientChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);

        SelectionKey clientKey = clientChannel.register(selector, SelectionKey.OP_READ);
        ClientConnection client = new ClientConnection(clientChannel, clientKey);
        clientKey.attach(client);

        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Accepted client connection from: " + client.getRemoteAddress());
        }
    }

    private void handleRead(SelectionKey key) throws IOException {
        ClientConnection client = (ClientConnection) key.attachment();
        int bytesRead = client.readFromChannel();

        if (bytesRead == -1) {
            // EOF: Remote closed connection gracefully
            closeConnection(key);
            return;
        }

        if (bytesRead > 0) {
            client.prepareForParsing();
            try {
                RespFrame frame;
                while ((frame = RespParser.parse(client.getReadBuffer())) != null) {
                    frameHandler.handleFrame(client, frame);
                }
            } finally {
                client.resumeAfterParsing();
            }

            // Flush any synchronous replies queued during command handling
            if (!client.flushWrites()) {
                client.enableWriteInterest();
            }
        }
    }

    private void handleWrite(SelectionKey key) throws IOException {
        ClientConnection client = (ClientConnection) key.attachment();
        client.flushWrites();
    }

    private void closeConnection(SelectionKey key) {
        ClientConnection client = (ClientConnection) key.attachment();
        if (client != null) {
            frameHandler.onClientDisconnected(client);
            client.close();
        }
        key.cancel();
    }

    public synchronized void stop() {
        running = false;
        if (selector != null && selector.isOpen()) {
            selector.wakeup();
            try {
                selector.close();
            } catch (IOException ignored) {}
        }
        if (serverChannel != null && serverChannel.isOpen()) {
            try {
                serverChannel.close();
            } catch (IOException ignored) {}
        }
    }

    public boolean isRunning() {
        return running;
    }
}
