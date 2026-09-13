package com.redisclone.replication;

import com.redisclone.command.CommandRegistry;
import com.redisclone.network.ClientConnection;
import com.redisclone.resp.RespEncoder;
import com.redisclone.resp.RespFrame;
import com.redisclone.resp.RespParser;
import com.redisclone.storage.DataStore;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Distributed Replication Engine.
 * Manages Node roles (MASTER vs REPLICA), active replica synchronization streams,
 * PSYNC handshakes, and live command propagation.
 */
public class ReplicationManager {

    private static final Logger LOGGER = Logger.getLogger(ReplicationManager.class.getName());

    public enum Role {
        MASTER,
        REPLICA
    }

    private volatile Role role = Role.MASTER;
    private final ReplicationBacklog backlog = new ReplicationBacklog(1024 * 1024); // 1 MB ring buffer
    private final Set<ClientConnection> connectedReplicas = ConcurrentHashMap.newKeySet();

    // Replica state
    private volatile String masterHost = null;
    private volatile int masterPort = 0;
    private volatile Socket masterSocket = null;
    private volatile Thread replicaSyncThread = null;

    public ReplicationManager() {}

    public Role getRole() {
        return role;
    }

    public ReplicationBacklog getBacklog() {
        return backlog;
    }

    public int getConnectedReplicasCount() {
        return connectedReplicas.size();
    }

    /**
     * Propagates a mutating write command to the replication backlog and all connected replicas.
     */
    public void propagateWrite(RespFrame.Array commandFrame) {
        if (role != Role.MASTER) {
            return;
        }

        byte[] encoded = RespEncoder.encode(commandFrame);
        backlog.write(encoded);

        for (ClientConnection replica : connectedReplicas) {
            try {
                replica.sendRawBytes(encoded);
            } catch (Exception e) {
                LOGGER.warning("Failed to propagate write to replica, removing replica: " + e.getMessage());
                connectedReplicas.remove(replica);
            }
        }
    }

    /**
     * Processes PSYNC requests from replica nodes.
     */
    public RespFrame handlePsync(ClientConnection replica, String replId, long offset) {
        if (backlog.canPartiallyResync(replId, offset)) {
            LOGGER.info("PSYNC: Performing PARTIAL resynchronization for replica at offset " + offset);
            replica.sendReply(RespFrame.ofSimpleString("CONTINUE"));
            byte[] missingBytes = backlog.getBytesFromOffset(offset);
            if (missingBytes.length > 0) {
                replica.sendRawBytes(missingBytes);
            }
            connectedReplicas.add(replica);
            return null;
        } else {
            LOGGER.info("PSYNC: Performing FULL resynchronization for replica.");
            String fullResyncMsg = "FULLRESYNC " + backlog.getMasterReplId() + " " + backlog.getMasterOffset();
            replica.sendReply(RespFrame.ofSimpleString(fullResyncMsg));
            connectedReplicas.add(replica);
            return null;
        }
    }

    public void removeReplica(ClientConnection client) {
        connectedReplicas.remove(client);
    }

    /**
     * Configures this node to replicate from a master node.
     */
    public synchronized void replicaOf(String host, int port, int myPort, DataStore dataStore, CommandRegistry registry) {
        if ("NO".equalsIgnoreCase(host) && port == 1) {
            // REPLICAOF NO ONE
            replicaOfNoOne();
            return;
        }

        this.role = Role.REPLICA;
        this.masterHost = host;
        this.masterPort = port;

        if (replicaSyncThread != null && replicaSyncThread.isAlive()) {
            replicaSyncThread.interrupt();
        }

        this.replicaSyncThread = new Thread(() -> runReplicaSyncLoop(host, port, myPort, dataStore, registry), "replica-sync-thread");
        this.replicaSyncThread.setDaemon(true);
        this.replicaSyncThread.start();
        LOGGER.info("Configured node as REPLICA of master " + host + ":" + port);
    }

    public synchronized void replicaOfNoOne() {
        this.role = Role.MASTER;
        this.masterHost = null;
        this.masterPort = 0;
        if (masterSocket != null) {
            try {
                masterSocket.close();
            } catch (Exception ignored) {}
        }
        if (replicaSyncThread != null) {
            replicaSyncThread.interrupt();
        }
        LOGGER.info("Node promoted to MASTER (REPLICAOF NO ONE executed).");
    }

    private void runReplicaSyncLoop(String host, int port, int myPort, DataStore dataStore, CommandRegistry registry) {
        try {
            masterSocket = new Socket(host, port);
            OutputStream out = masterSocket.getOutputStream();
            InputStream in = masterSocket.getInputStream();

            // 1. Handshake: PING
            out.write("*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            readExpectedLine(in); // Consume +PONG

            // 2. Handshake: REPLCONF listening-port <myPort>
            String replConfPort = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" +
                    Integer.toString(myPort).length() + "\r\n" + myPort + "\r\n";
            out.write(replConfPort.getBytes(StandardCharsets.US_ASCII));
            out.flush();
            readExpectedLine(in); // Consume +OK

            // 3. Handshake: REPLCONF capa psync2
            out.write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            readExpectedLine(in); // Consume +OK

            // 4. Handshake: PSYNC ? -1 (Request initial sync)
            out.write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            readExpectedLine(in); // Consume +FULLRESYNC <replid> <offset>

            LOGGER.info("Replica handshake with master " + host + ":" + port + " succeeded. Listening for replication stream...");

            // 5. Continuous replication stream reading loop
            byte[] buffer = new byte[64 * 1024];
            ByteBuffer byteBuffer = ByteBuffer.allocate(256 * 1024);

            while (!Thread.currentThread().isInterrupted()) {
                int read = in.read(buffer);
                if (read == -1) {
                    LOGGER.warning("Replication stream closed by master.");
                    break;
                }

                byteBuffer.put(buffer, 0, read);
                byteBuffer.flip();

                RespFrame frame;
                while ((frame = RespParser.parse(byteBuffer)) != null) {
                    if (frame instanceof RespFrame.Array arrayFrame) {
                        // Apply replicated write command to local data store
                        registry.executeInternal(arrayFrame);
                    }
                }
                byteBuffer.compact();
            }
        } catch (Exception e) {
            if (!Thread.currentThread().isInterrupted()) {
                LOGGER.log(Level.WARNING, "Replication sync loop encountered an error with master: " + e.getMessage());
            }
        }
    }

    private void readExpectedLine(InputStream in) throws Exception {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') break;
            if (b != '\r') sb.append((char) b);
        }
    }
}
