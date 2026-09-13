package com.redisclone;

import com.redisclone.cluster.ClusterSlotRouter;
import com.redisclone.cluster.Crc16;
import com.redisclone.persistence.AofManager;
import com.redisclone.resp.RespEncoder;
import com.redisclone.resp.RespFrame;
import com.redisclone.resp.RespParser;
import com.redisclone.server.RedisServer;
import com.redisclone.server.ServerConfig;
import com.redisclone.storage.DataStore;
import com.redisclone.storage.RedisObject;
import com.redisclone.storage.eviction.LruEvictionPolicy;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * End-to-End Test Suite for Custom Redis Clone.
 * Tests RESP parsing, byte fragmentation, pipelining, in-memory store,
 * active/passive TTL eviction, LRU cache eviction, transactions (MULTI/EXEC/WATCH),
 * multi-client TCP networking, Pub/Sub, AOF/RDB persistence, CRC16 cluster routing,
 * and Master-Replica distributed replication.
 */
public class RedisServerTest {

    private static int passedTests = 0;
    private static int totalTests = 0;

    public static void main(String[] args) throws Exception {
        System.out.println("=================================================");
        System.out.println(" RUNNING CUSTOM REDIS CLONE ARCHITECTURE TESTS   ");
        System.out.println("=================================================");

        testRespParserBasic();
        testRespFragmentationAndPipelining();
        testDataStoreAndPassiveEviction();
        testActiveEvictionCycle();
        testLruEvictionPolicy();
        testTransactionsAndOptimisticWatch();
        testCrc16AndClusterRouting();
        testLiveTcpCommandsAndPubSub();
        testPersistenceRecovery();
        testMasterReplicaReplication();

        System.out.println("=================================================");
        System.out.println(" ALL TESTS PASSED: " + passedTests + " / " + totalTests);
        System.out.println("=================================================");
    }

    private static void assertEquals(Object expected, Object actual, String testName) {
        totalTests++;
        if (!java.util.Objects.equals(expected, actual)) {
            System.err.println("❌ FAILED: " + testName + " | Expected: " + expected + ", Got: " + actual);
            throw new AssertionError("Test failed: " + testName);
        } else {
            passedTests++;
            System.out.println("✅ PASSED: " + testName);
        }
    }

    private static void assertTrue(boolean condition, String testName) {
        assertEquals(true, condition, testName);
    }

    // --- 1. RESP PARSER TESTS ---
    private static void testRespParserBasic() {
        System.out.println("\n--- Testing RESP2/3 Parser & Encoder ---");

        // Simple String
        ByteBuffer buf = ByteBuffer.wrap("+OK\r\n".getBytes(StandardCharsets.US_ASCII));
        RespFrame frame = RespParser.parse(buf);
        assertTrue(frame instanceof RespFrame.SimpleString, "Parse Simple String Type");
        assertEquals("OK", ((RespFrame.SimpleString) frame).value(), "Parse Simple String Value");

        // Error
        buf = ByteBuffer.wrap("-ERR syntax error\r\n".getBytes(StandardCharsets.US_ASCII));
        frame = RespParser.parse(buf);
        assertTrue(frame instanceof RespFrame.Error, "Parse Error Type");
        assertEquals("ERR syntax error", ((RespFrame.Error) frame).message(), "Parse Error Message");

        // Integer
        buf = ByteBuffer.wrap(":1048576\r\n".getBytes(StandardCharsets.US_ASCII));
        frame = RespParser.parse(buf);
        assertTrue(frame instanceof RespFrame.Integer, "Parse Integer Type");
        assertEquals(1048576L, ((RespFrame.Integer) frame).value(), "Parse Integer Value");

        // Bulk String
        buf = ByteBuffer.wrap("$6\r\nfoobar\r\n".getBytes(StandardCharsets.US_ASCII));
        frame = RespParser.parse(buf);
        assertTrue(frame instanceof RespFrame.BulkString, "Parse Bulk String Type");
        assertEquals("foobar", ((RespFrame.BulkString) frame).asUtf8String(), "Parse Bulk String Value");

        // Null Bulk String
        buf = ByteBuffer.wrap("$-1\r\n".getBytes(StandardCharsets.US_ASCII));
        frame = RespParser.parse(buf);
        assertTrue(frame instanceof RespFrame.BulkString, "Parse Null Bulk String Type");
        assertTrue(((RespFrame.BulkString) frame).isNull(), "Parse Null Bulk String isNull");

        // Array
        buf = ByteBuffer.wrap("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n".getBytes(StandardCharsets.US_ASCII));
        frame = RespParser.parse(buf);
        assertTrue(frame instanceof RespFrame.Array, "Parse Array Type");
        List<RespFrame> elements = ((RespFrame.Array) frame).elements();
        assertEquals(2, elements.size(), "Array element count");
        assertEquals("GET", ((RespFrame.BulkString) elements.get(0)).asUtf8String(), "Array[0]");
        assertEquals("foo", ((RespFrame.BulkString) elements.get(1)).asUtf8String(), "Array[1]");
    }

    // --- 2. TCP FRAGMENTATION & PIPELINING TESTS ---
    private static void testRespFragmentationAndPipelining() {
        System.out.println("\n--- Testing TCP Packet Fragmentation & Pipelining ---");

        // Simulate fragmented TCP packet: half the frame arrives first
        byte[] chunk1 = "*2\r\n$3\r\nS".getBytes(StandardCharsets.US_ASCII);
        byte[] chunk2 = "ET\r\n$3\r\nkey\r\n".getBytes(StandardCharsets.US_ASCII);

        ByteBuffer buf = ByteBuffer.allocate(128);
        buf.put(chunk1);
        buf.flip();

        RespFrame frame = RespParser.parse(buf);
        assertEquals(null, frame, "Incomplete chunk returns null without corrupting position");
        assertEquals(0, buf.position(), "Buffer position restored to mark after incomplete read");

        // Second chunk arrives over TCP
        buf.compact();
        buf.put(chunk2);
        buf.flip();

        frame = RespParser.parse(buf);
        assertTrue(frame != null, "Frame parses successfully once remaining chunk arrives");
        assertTrue(frame instanceof RespFrame.Array, "Parsed frame is Array");
        assertEquals("SET", ((RespFrame.BulkString) ((RespFrame.Array) frame).elements().get(0)).asUtf8String(), "Fragmented command name");
        assertEquals("key", ((RespFrame.BulkString) ((RespFrame.Array) frame).elements().get(1)).asUtf8String(), "Fragmented argument");

        // Pipelining test: two commands concatenated in one buffer
        String pipelined = "*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n";
        buf = ByteBuffer.wrap(pipelined.getBytes(StandardCharsets.US_ASCII));
        RespFrame frame1 = RespParser.parse(buf);
        RespFrame frame2 = RespParser.parse(buf);
        RespFrame frame3 = RespParser.parse(buf);

        assertTrue(frame1 != null, "Pipelined Frame 1 parsed");
        assertTrue(frame2 != null, "Pipelined Frame 2 parsed");
        assertEquals(null, frame3, "Buffer empty after 2 pipelined frames");
    }

    // --- 3. STORAGE ENGINE & PASSIVE EVICTION ---
    private static void testDataStoreAndPassiveEviction() throws Exception {
        System.out.println("\n--- Testing DataStore Operations & Passive Eviction ---");

        DataStore ds = new DataStore();

        // String operations
        ds.set("alpha", RedisObject.ofString("val1"), null);
        assertEquals("val1", ds.get("alpha").asUtf8String(), "DataStore GET returns correct value");

        // INCR
        assertEquals(1L, ds.incr("counter"), "DataStore INCR uninitialized key");
        assertEquals(2L, ds.incr("counter"), "DataStore INCR increment key");

        // Hash operations
        ds.hset("user:100", "name", "Alice".getBytes(StandardCharsets.UTF_8));
        ds.hset("user:100", "role", "Architect".getBytes(StandardCharsets.UTF_8));
        assertEquals("Alice", new String(ds.hget("user:100", "name"), StandardCharsets.UTF_8), "DataStore HGET");
        assertEquals("Architect", new String(ds.hget("user:100", "role"), StandardCharsets.UTF_8), "DataStore HGET field 2");

        // List operations
        ds.lpush("tasks", "task1".getBytes(StandardCharsets.UTF_8), "task2".getBytes(StandardCharsets.UTF_8));
        assertEquals("task2", new String(ds.lpop("tasks"), StandardCharsets.UTF_8), "DataStore LPOP head");
        assertEquals("task1", new String(ds.lpop("tasks"), StandardCharsets.UTF_8), "DataStore LPOP next");
        assertEquals(null, ds.lpop("tasks"), "DataStore LPOP empty list returns null");

        // Passive (Lazy) TTL Eviction
        ds.set("tempKey", RedisObject.ofString("expiresFast"), 50L); // 50ms TTL
        assertTrue(ds.exists("tempKey"), "Key exists before TTL expiry");
        Thread.sleep(80);
        assertEquals(null, ds.get("tempKey"), "Passive eviction: GET returns null after TTL elapsed");
        assertTrue(!ds.exists("tempKey"), "Passive eviction: exists returns false after TTL elapsed");
    }

    // --- 4. ACTIVE EVICTION CYCLE ---
    private static void testActiveEvictionCycle() throws Exception {
        System.out.println("\n--- Testing Active Probabilistic Eviction Cycle ---");

        DataStore ds = new DataStore();
        // Insert 50 keys with a 40ms TTL
        for (int i = 0; i < 50; i++) {
            ds.set("activeExp:" + i, RedisObject.ofString("val" + i), 40L);
        }

        assertEquals(50, ds.keyCount(), "Inserted 50 temporary keys");
        Thread.sleep(60); // Allow TTL to expire

        // Run active probabilistic expiration sweep
        int evicted = ds.activeExpireCycle(20, 16);
        assertTrue(evicted > 0, "Active expiration pruned expired keys without reads (" + evicted + " keys pruned)");
        assertTrue(ds.keyCount() < 50, "Total key count reduced via active expiration sweep");
    }

    // --- 5. LRU CACHE EVICTION POLICY ---
    private static void testLruEvictionPolicy() {
        System.out.println("\n--- Testing O(1) LRU Cache Eviction Engine ---");

        // Create store with max 3 keys
        DataStore ds = new DataStore(3, LruEvictionPolicy.allKeysLru());
        ds.set("k1", RedisObject.ofString("v1"));
        ds.set("k2", RedisObject.ofString("v2"));
        ds.set("k3", RedisObject.ofString("v3"));
        assertEquals(3, ds.keyCount(), "Store reached max capacity of 3");

        // Access k1 -> order of recency becomes: k1 (MRU), k3, k2 (LRU)
        ds.get("k1");

        // Insert k4 -> should evict k2 (least recently used)
        ds.set("k4", RedisObject.ofString("v4"));
        assertEquals(3, ds.keyCount(), "Store maintains max capacity after eviction");
        assertTrue(ds.get("k2") == null, "LRU key k2 was evicted");
        assertTrue(ds.get("k1") != null, "Frequently accessed key k1 was preserved");
        assertTrue(ds.get("k3") != null, "Key k3 was preserved");
        assertTrue(ds.get("k4") != null, "Newly inserted key k4 is present");
    }

    // --- 6. TRANSACTIONS & OPTIMISTIC LOCKING (WATCH) ---
    private static void testTransactionsAndOptimisticWatch() throws Exception {
        System.out.println("\n--- Testing ACID/CAS Transactions (MULTI/EXEC/DISCARD/WATCH) ---");

        int txPort = 6393;
        ServerConfig config = new ServerConfig();
        config.setPort(txPort);
        config.setAofEnabled(false);
        config.setRdbEnabled(false);

        RedisServer server = new RedisServer(config);
        server.start();
        Thread.sleep(200);

        try {
            // Client 1: Basic MULTI / EXEC
            try (Socket socket = new Socket("127.0.0.1", txPort);
                 OutputStream out = socket.getOutputStream();
                 InputStream in = socket.getInputStream()) {

                out.write("*1\r\n$5\r\nMULTI\r\n".getBytes(StandardCharsets.US_ASCII));
                out.flush();
                assertEquals("+OK\r\n", readResponse(in), "MULTI begins transaction");

                out.write("*3\r\n$3\r\nSET\r\n$3\r\ntx1\r\n$2\r\n42\r\n".getBytes(StandardCharsets.US_ASCII));
                out.flush();
                assertEquals("+QUEUED\r\n", readResponse(in), "SET queued in transaction");

                out.write("*2\r\n$4\r\nINCR\r\n$3\r\ntx1\r\n".getBytes(StandardCharsets.US_ASCII));
                out.flush();
                assertEquals("+QUEUED\r\n", readResponse(in), "INCR queued in transaction");

                out.write("*1\r\n$4\r\nEXEC\r\n".getBytes(StandardCharsets.US_ASCII));
                out.flush();
                String execResponse = readResponse(in);
                assertTrue(execResponse.startsWith("*2\r\n"), "EXEC returns array of 2 results");
                assertTrue(execResponse.contains("+OK"), "EXEC array contains SET result");
                assertTrue(execResponse.contains(":43"), "EXEC array contains INCR result");
            }

            // Client 2: DISCARD test
            try (Socket socket = new Socket("127.0.0.1", txPort);
                 OutputStream out = socket.getOutputStream();
                 InputStream in = socket.getInputStream()) {

                out.write("*1\r\n$5\r\nMULTI\r\n".getBytes(StandardCharsets.US_ASCII));
                out.flush();
                readResponse(in);

                out.write("*3\r\n$3\r\nSET\r\n$9\r\ndiscardMe\r\n$4\r\nfail\r\n".getBytes(StandardCharsets.US_ASCII));
                out.flush();
                readResponse(in);

                out.write("*1\r\n$7\r\nDISCARD\r\n".getBytes(StandardCharsets.US_ASCII));
                out.flush();
                assertEquals("+OK\r\n", readResponse(in), "DISCARD cancels transaction");

                out.write("*2\r\n$3\r\nGET\r\n$9\r\ndiscardMe\r\n".getBytes(StandardCharsets.US_ASCII));
                out.flush();
                assertEquals("$-1\r\n", readResponse(in), "Discarded key was never committed");
            }

            // Client 3 & 4: WATCH with concurrent modification abort
            Socket client1 = new Socket("127.0.0.1", txPort);
            OutputStream out1 = client1.getOutputStream();
            InputStream in1 = client1.getInputStream();

            Socket client2 = new Socket("127.0.0.1", txPort);
            OutputStream out2 = client2.getOutputStream();
            InputStream in2 = client2.getInputStream();

            // Set initial value
            out1.write("*3\r\n$3\r\nSET\r\n$7\r\nbalance\r\n$3\r\n100\r\n".getBytes(StandardCharsets.US_ASCII));
            out1.flush();
            readResponse(in1);

            // Client 1 WATCH balance
            out1.write("*2\r\n$5\r\nWATCH\r\n$7\r\nbalance\r\n".getBytes(StandardCharsets.US_ASCII));
            out1.flush();
            assertEquals("+OK\r\n", readResponse(in1), "WATCH balance succeeded");

            // Client 1 MULTI
            out1.write("*1\r\n$5\r\nMULTI\r\n".getBytes(StandardCharsets.US_ASCII));
            out1.flush();
            readResponse(in1);

            out1.write("*3\r\n$3\r\nSET\r\n$7\r\nbalance\r\n$3\r\n200\r\n".getBytes(StandardCharsets.US_ASCII));
            out1.flush();
            readResponse(in1);

            // Client 2 intervenes and mutates balance
            out2.write("*2\r\n$4\r\nINCR\r\n$7\r\nbalance\r\n".getBytes(StandardCharsets.US_ASCII));
            out2.flush();
            assertEquals(":101\r\n", readResponse(in2), "Client 2 mutated watched balance");

            // Client 1 EXEC -> must abort!
            out1.write("*1\r\n$4\r\nEXEC\r\n".getBytes(StandardCharsets.US_ASCII));
            out1.flush();
            assertEquals("*-1\r\n", readResponse(in1), "EXEC aborted via WATCH optimistic lock (returns *-1)");

            client1.close();
            client2.close();
        } finally {
            server.stop();
        }
    }

    // --- 7. CRC16 & CLUSTER SHARDING SIMULATION ---
    private static void testCrc16AndClusterRouting() {
        System.out.println("\n--- Testing CRC16 Hashing & Cluster Sharding Router ---");

        int slot1 = Crc16.getSlot("user100");
        assertTrue(slot1 >= 0 && slot1 < 16384, "CRC16 slot within 0-16383 range");

        // Hash Tag test: {user100}:profile and {user100}:orders must map to identical slot
        int slotTag1 = Crc16.getSlot("{user100}:profile");
        int slotTag2 = Crc16.getSlot("{user100}:orders");
        assertEquals(slotTag1, slotTag2, "Redis Hash Tag curly brackets map to identical hash slot");

        // Cluster Router redirection test
        ClusterSlotRouter router = new ClusterSlotRouter("127.0.0.1", 7000);
        router.setClusterEnabled(true);
        router.setMySlotRange(0, 5000);
        router.addRemoteNode("node-2", "127.0.0.1", 7001, 5001, 16383);

        assertTrue(router.isMySlot(100), "Slot 100 is owned locally");
        assertTrue(!router.isMySlot(8000), "Slot 8000 is owned by remote node");

        RespFrame moved = router.createMovedError(8000);
        assertTrue(moved instanceof RespFrame.Error, "Redirection frame is Error");
        assertEquals("MOVED 8000 127.0.0.1:7001", ((RespFrame.Error) moved).message(), "Cluster -MOVED redirection error format");
    }

    // --- 8. LIVE TCP NETWORKING & PUBSUB ---
    private static void testLiveTcpCommandsAndPubSub() throws Exception {
        System.out.println("\n--- Testing Live TCP Networking, Commands & Pub/Sub ---");

        int testPort = 6389;
        ServerConfig config = new ServerConfig();
        config.setPort(testPort);
        config.setAofEnabled(false);
        config.setRdbEnabled(false);

        RedisServer server = new RedisServer(config);
        server.start();

        Thread.sleep(200); // Allow server to bind

        try {
            // Client 1: Standard commands
            try (Socket socket = new Socket("127.0.0.1", testPort);
                 OutputStream out = socket.getOutputStream();
                 InputStream in = socket.getInputStream()) {

                // Send PING
                out.write("*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII));
                out.flush();
                String response = readResponse(in);
                assertEquals("+PONG\r\n", response, "TCP PING returns +PONG");

                // Send SET key value
                out.write("*3\r\n$3\r\nSET\r\n$7\r\ncluster\r\n$10\r\nproduction\r\n".getBytes(StandardCharsets.US_ASCII));
                out.flush();
                response = readResponse(in);
                assertEquals("+OK\r\n", response, "TCP SET returns +OK");

                // Send GET key
                out.write("*2\r\n$3\r\nGET\r\n$7\r\ncluster\r\n".getBytes(StandardCharsets.US_ASCII));
                out.flush();
                response = readResponse(in);
                assertEquals("$10\r\nproduction\r\n", response, "TCP GET returns bulk string value");

                // Send DEL key
                out.write("*2\r\n$3\r\nDEL\r\n$7\r\ncluster\r\n".getBytes(StandardCharsets.US_ASCII));
                out.flush();
                response = readResponse(in);
                assertEquals(":1\r\n", response, "TCP DEL returns count of deleted keys");
            }

            // Test Real-Time Pub/Sub with two concurrent TCP clients
            Socket subSocket = new Socket("127.0.0.1", testPort);
            OutputStream subOut = subSocket.getOutputStream();
            InputStream subIn = subSocket.getInputStream();

            Socket pubSocket = new Socket("127.0.0.1", testPort);
            OutputStream pubOut = pubSocket.getOutputStream();
            InputStream pubIn = pubSocket.getInputStream();

            // Client 1: SUBSCRIBE news
            subOut.write("*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nnews\r\n".getBytes(StandardCharsets.US_ASCII));
            subOut.flush();
            String subConfirmation = readResponse(subIn);
            assertTrue(subConfirmation.contains("subscribe"), "Sub confirmation received");
            assertTrue(subConfirmation.contains("news"), "Sub confirmation contains channel name");

            // Client 2: PUBLISH news "hello world"
            pubOut.write("*3\r\n$7\r\nPUBLISH\r\n$4\r\nnews\r\n$11\r\nhello world\r\n".getBytes(StandardCharsets.US_ASCII));
            pubOut.flush();
            String pubResponse = readResponse(pubIn);
            assertEquals(":1\r\n", pubResponse, "PUBLISH returns 1 subscriber received message");

            // Verify Client 1 received push message
            String pushMsg = readResponse(subIn);
            assertTrue(pushMsg.contains("message"), "Push message contains 'message'");
            assertTrue(pushMsg.contains("news"), "Push message contains channel");
            assertTrue(pushMsg.contains("hello world"), "Push message contains payload");

            subSocket.close();
            pubSocket.close();

        } finally {
            server.stop();
        }
    }

    // --- 9. PERSISTENCE RECOVERY (AOF & RDB) ---
    private static void testPersistenceRecovery() throws Exception {
        System.out.println("\n--- Testing Persistence: AOF & RDB Snapshot Recovery ---");

        String aofPath = "test_data/test_appendonly.aof";
        String rdbPath = "test_data/test_dump.rdb";

        Files.createDirectories(Path.of("test_data"));
        Files.deleteIfExists(Path.of(aofPath));
        Files.deleteIfExists(Path.of(rdbPath));

        int port = 6390;
        ServerConfig config = new ServerConfig();
        config.setPort(port);
        config.setAofEnabled(true);
        config.setAofFilePath(aofPath);
        config.setAofFsync(AofManager.FsyncPolicy.ALWAYS);
        config.setRdbEnabled(true);
        config.setRdbFilePath(rdbPath);

        RedisServer server = new RedisServer(config);
        server.start();
        Thread.sleep(200);

        try (Socket socket = new Socket("127.0.0.1", port);
             OutputStream out = socket.getOutputStream();
             InputStream in = socket.getInputStream()) {

            // Mutate database via socket
            out.write("*3\r\n$3\r\nSET\r\n$4\r\nuser\r\n$7\r\nphantom\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            assertEquals("+OK\r\n", readResponse(in), "SET reply");

            out.write("*4\r\n$4\r\nHSET\r\n$4\r\ninfo\r\n$3\r\nage\r\n$2\r\n30\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            assertEquals(":1\r\n", readResponse(in), "HSET reply");

            out.write("*3\r\n$5\r\nLPUSH\r\n$7\r\nnumbers\r\n$3\r\n100\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            assertEquals(":1\r\n", readResponse(in), "LPUSH reply");

            // Trigger synchronous RDB snapshot
            out.write("*1\r\n$4\r\nSAVE\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            String saveReply = readResponse(in);
            assertEquals("+OK\r\n", saveReply, "SAVE command returns +OK");
        }

        // Stop server and flush to disk
        server.stop();
        Thread.sleep(200);

        assertTrue(Files.exists(Path.of(aofPath)), "AOF file exists on disk");
        assertTrue(Files.size(Path.of(aofPath)) > 0, "AOF file contains recorded bytes");
        assertTrue(Files.exists(Path.of(rdbPath)), "RDB snapshot file exists on disk");

        // Restart a new server instance reading the persisted AOF log
        System.out.println("Restarting new Redis instance to verify state reconstruction...");
        RedisServer recoveredServer = new RedisServer(config);
        recoveredServer.start();
        Thread.sleep(200);

        try (Socket socket = new Socket("127.0.0.1", port);
             OutputStream out = socket.getOutputStream();
             InputStream in = socket.getInputStream()) {

            // Verify 'user'
            out.write("*2\r\n$3\r\nGET\r\n$4\r\nuser\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            assertEquals("$7\r\nphantom\r\n", readResponse(in), "AOF Replay restored 'user'");

            // Verify 'info' HGET
            out.write("*3\r\n$4\r\nHGET\r\n$4\r\ninfo\r\n$3\r\nage\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            assertEquals("$2\r\n30\r\n", readResponse(in), "AOF Replay restored Hash 'info'");

            // Verify 'numbers' LPOP
            out.write("*2\r\n$4\r\nLPOP\r\n$7\r\nnumbers\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            assertEquals("$3\r\n100\r\n", readResponse(in), "AOF Replay restored List 'numbers'");
        } finally {
            recoveredServer.stop();
            // Cleanup test files safely
            try {
                Files.deleteIfExists(Path.of(aofPath));
                Files.deleteIfExists(Path.of(rdbPath));
                Files.deleteIfExists(Path.of("test_data"));
            } catch (Exception ignored) {}
        }
    }

    // --- 10. MASTER-REPLICA DISTRIBUTED REPLICATION ---
    private static void testMasterReplicaReplication() throws Exception {
        System.out.println("\n--- Testing Master-Replica Live Stream Replication ---");

        int masterPort = 6394;
        int replicaPort = 6395;

        // Start Master
        ServerConfig masterConfig = new ServerConfig();
        masterConfig.setPort(masterPort);
        masterConfig.setAofEnabled(false);
        masterConfig.setRdbEnabled(false);
        RedisServer masterServer = new RedisServer(masterConfig);
        masterServer.start();

        Thread.sleep(200);

        // Start Replica
        ServerConfig replicaConfig = new ServerConfig();
        replicaConfig.setPort(replicaPort);
        replicaConfig.setAofEnabled(false);
        replicaConfig.setRdbEnabled(false);
        replicaConfig.setReplicaOf("127.0.0.1", masterPort);
        RedisServer replicaServer = new RedisServer(replicaConfig);
        replicaServer.start();

        Thread.sleep(400); // Allow handshake (PING, REPLCONF, PSYNC) to settle

        try {
            // Write to Master
            try (Socket socket = new Socket("127.0.0.1", masterPort);
                 OutputStream out = socket.getOutputStream();
                 InputStream in = socket.getInputStream()) {

                out.write("*3\r\n$3\r\nSET\r\n$9\r\nsyncKey42\r\n$10\r\nsyncValueA\r\n".getBytes(StandardCharsets.US_ASCII));
                out.flush();
                assertEquals("+OK\r\n", readResponse(in), "Master accepts SET mutation");
            }

            // Wait for replication stream propagation
            Thread.sleep(300);

            // Read from Replica
            try (Socket socket = new Socket("127.0.0.1", replicaPort);
                 OutputStream out = socket.getOutputStream();
                 InputStream in = socket.getInputStream()) {

                out.write("*2\r\n$3\r\nGET\r\n$9\r\nsyncKey42\r\n".getBytes(StandardCharsets.US_ASCII));
                out.flush();
                String replicaResponse = readResponse(in);
                assertEquals("$10\r\nsyncValueA\r\n", replicaResponse, "Replica synchronized mutated key from Master stream");
            }
        } finally {
            replicaServer.stop();
            masterServer.stop();
        }
    }

    private static String readResponse(InputStream in) throws Exception {
        byte[] buffer = new byte[1024];
        int read = in.read(buffer);
        if (read <= 0) return "";
        return new String(buffer, 0, read, StandardCharsets.UTF_8);
    }
}
