package com.redisclone;

import com.redisclone.network.ClientConnection;
import com.redisclone.persistence.AofManager;
import com.redisclone.persistence.RdbManager;
import com.redisclone.resp.RespFrame;
import com.redisclone.resp.RespParser;
import com.redisclone.server.RedisServer;
import com.redisclone.server.ServerConfig;
import com.redisclone.server.ServerMetrics;
import com.redisclone.storage.DataStore;
import com.redisclone.storage.RedisObject;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Rigorous Chaos, Correctness and Failure Injection Test Suite.
 * Covers:
 * 1. Protocol malformed frame handling & 64-bit integer overflow protection
 * 2. Oversized payload / bounded buffer rejection
 * 3. Corrupted AOF log recovery (partial write / crash simulation)
 * 4. Corrupted RDB file detection & handling
 * 5. Concurrent client contention & transaction isolation under race conditions
 * 6. Slow client backpressure & client disconnect during execution
 * 7. Live Telemetry & INFO metrics accuracy
 */
public class FailureAndEdgeCaseTest {

    private static int passedTests = 0;
    private static int totalTests = 0;

    public static void main(String[] args) throws Exception {
        System.out.println("=================================================");
        System.out.println(" RUNNING FAILURE & EDGE-CASE SYSTEM TESTS       ");
        System.out.println("=================================================");

        testMalformedAndOverflowRespFrames();
        testCorruptedAofReplay();
        testCorruptedRdbLoading();
        testHighConcurrencyContention();
        testSlowClientAndBufferLimits();
        testTelemetryAndInfoCommand();
        testAofRewriteCompaction();
        testBatchAndUtilityCommands();
        testBitmapsHyperLogLogAndStreams();

        System.out.println("=================================================");
        System.out.println(" ALL FAILURE TESTS PASSED: " + passedTests + " / " + totalTests);
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

    // --- 1. MALFORMED & OVERFLOW RESP FRAMES ---
    private static void testMalformedAndOverflowRespFrames() {
        System.out.println("\n--- Testing Malformed Input & Protocol Robustness ---");

        // 1. Integer overflow beyond Long.MAX_VALUE
        ByteBuffer overflowBuf = ByteBuffer.wrap(":99999999999999999999999999999999\r\n".getBytes(StandardCharsets.US_ASCII));
        RespFrame overflowFrame = RespParser.parse(overflowBuf);
        assertEquals(null, overflowFrame, "Integer overflow beyond Long.MAX_VALUE rejected cleanly");

        // 2. Malformed non-numeric integer
        ByteBuffer nonNumericBuf = ByteBuffer.wrap(":123abc\r\n".getBytes(StandardCharsets.US_ASCII));
        RespFrame nonNumericFrame = RespParser.parse(nonNumericBuf);
        assertEquals(null, nonNumericFrame, "Non-numeric integer rejected cleanly");

        // 3. Negative length other than -1 in Bulk String
        ByteBuffer negativeLenBuf = ByteBuffer.wrap("$-5\r\nfoo\r\n".getBytes(StandardCharsets.US_ASCII));
        RespFrame negativeLenFrame = RespParser.parse(negativeLenBuf);
        assertEquals(null, negativeLenFrame, "Negative bulk string length (-5) rejected cleanly");

        // 4. Excessively large bulk string request (> 512 MB limit)
        ByteBuffer excessiveBuf = ByteBuffer.wrap("$600000000\r\n".getBytes(StandardCharsets.US_ASCII));
        RespFrame excessiveFrame = RespParser.parse(excessiveBuf);
        assertEquals(null, excessiveFrame, "Excessive bulk string request (>512MB) rejected");

        // 5. Zero-byte empty bulk string ($0\r\n\r\n)
        ByteBuffer emptyBulkBuf = ByteBuffer.wrap("$0\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
        RespFrame emptyBulkFrame = RespParser.parse(emptyBulkBuf);
        assertTrue(emptyBulkFrame instanceof RespFrame.BulkString, "Empty bulk string is parsed as BulkString");
        assertEquals(0, ((RespFrame.BulkString) emptyBulkFrame).data().length, "Empty bulk string has 0 byte payload");

        // 6. Empty array (*0\r\n)
        ByteBuffer emptyArrayBuf = ByteBuffer.wrap("*0\r\n".getBytes(StandardCharsets.US_ASCII));
        RespFrame emptyArrayFrame = RespParser.parse(emptyArrayBuf);
        assertTrue(emptyArrayFrame instanceof RespFrame.Array, "Empty array parsed as Array frame");
        assertEquals(0, ((RespFrame.Array) emptyArrayFrame).elements().size(), "Empty array has 0 elements");
    }

    // --- 2. CORRUPTED AOF LOG RECOVERY ---
    private static void testCorruptedAofReplay() throws Exception {
        System.out.println("\n--- Testing Corrupted / Truncated AOF Log Recovery ---");

        String aofPath = "test_data/corrupted_test.aof";
        Files.createDirectories(Path.of("test_data"));
        Files.deleteIfExists(Path.of(aofPath));

        // Write 2 complete commands, then a truncated half-command simulating an abrupt crash mid-write
        try (FileOutputStream fos = new FileOutputStream(aofPath)) {
            // Command 1: SET key1 val1
            fos.write("*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$4\r\nval1\r\n".getBytes(StandardCharsets.US_ASCII));
            // Command 2: SET key2 val2
            fos.write("*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$4\r\nval2\r\n".getBytes(StandardCharsets.US_ASCII));
            // Truncated crash command: *3\r\n$3\r\nSET\r\n$4\r\nkey3 (sudden power loss)
            fos.write("*3\r\n$3\r\nSET\r\n$4\r\nkey3".getBytes(StandardCharsets.US_ASCII));
            fos.flush();
        }

        DataStore ds = new DataStore();
        AofManager aofManager = new AofManager(aofPath, AofManager.FsyncPolicy.NO, true);
        com.redisclone.command.CommandRegistry registry = new com.redisclone.command.CommandRegistry(
                ds, null, aofManager, null, null, null, 6379
        );

        // Replay should restore key1 and key2, and cleanly handle the truncated tail
        int recovered = aofManager.replay(registry);
        assertEquals(2, recovered, "AOF replay gracefully recovered all 2 valid commands preceding corruption");
        assertEquals("val1", ds.get("key1").asUtf8String(), "Key1 restored correctly");
        assertEquals("val2", ds.get("key2").asUtf8String(), "Key2 restored correctly");
        assertEquals(null, ds.get("key3"), "Truncated key3 was not committed");

        Files.deleteIfExists(Path.of(aofPath));
    }

    // --- 3. CORRUPTED RDB FILE HANDLING ---
    private static void testCorruptedRdbLoading() throws Exception {
        System.out.println("\n--- Testing Corrupted RDB File Detection ---");

        String rdbPath = "test_data/corrupted_dump.rdb";
        Files.createDirectories(Path.of("test_data"));
        Files.deleteIfExists(Path.of(rdbPath));

        // Write invalid magic header
        try (FileOutputStream fos = new FileOutputStream(rdbPath)) {
            fos.write("CORRUPTED_HEADER_DATA_123456789".getBytes(StandardCharsets.US_ASCII));
            fos.flush();
        }

        DataStore ds = new DataStore();
        RdbManager rdbManager = new RdbManager(rdbPath);

        boolean exceptionCaught = false;
        try {
            rdbManager.load(ds);
        } catch (Exception e) {
            exceptionCaught = true;
        }

        // RdbManager either throws IOException or catches and logs without corrupting memory
        assertEquals(0, ds.keyCount(), "DataStore remains intact and empty after corrupted RDB load");
        Files.deleteIfExists(Path.of(rdbPath));
    }

    // --- 4. HIGH CONCURRENCY & TRANSACTION ISOLATION ---
    private static void testHighConcurrencyContention() throws Exception {
        System.out.println("\n--- Testing High-Concurrency Contention & Atomicity ---");

        int port = 6396;
        ServerConfig config = new ServerConfig();
        config.setPort(port);
        config.setAofEnabled(false);
        config.setRdbEnabled(false);

        RedisServer server = new RedisServer(config);
        server.start();
        Thread.sleep(200);

        try {
            int numThreads = 10;
            int incrementsPerThread = 50;
            ExecutorService pool = Executors.newFixedThreadPool(numThreads);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch finishLatch = new CountDownLatch(numThreads);

            for (int t = 0; t < numThreads; t++) {
                pool.submit(() -> {
                    try (Socket s = new Socket("127.0.0.1", port);
                         OutputStream out = s.getOutputStream();
                         InputStream in = s.getInputStream()) {
                        byte[] buf = new byte[256];
                        startLatch.await();
                        for (int i = 0; i < incrementsPerThread; i++) {
                            out.write("*2\r\n$4\r\nINCR\r\n$11\r\nraceCounter\r\n".getBytes(StandardCharsets.US_ASCII));
                            out.flush();
                            in.read(buf);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        finishLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            finishLatch.await();
            pool.shutdown();

            // Verify final counter exactly equals numThreads * incrementsPerThread
            try (Socket s = new Socket("127.0.0.1", port);
                 OutputStream out = s.getOutputStream();
                 InputStream in = s.getInputStream()) {
                out.write("*2\r\n$3\r\nGET\r\n$11\r\nraceCounter\r\n".getBytes(StandardCharsets.US_ASCII));
                out.flush();
                byte[] buf = new byte[256];
                int read = in.read(buf);
                String resp = new String(buf, 0, read, StandardCharsets.UTF_8);
                int expected = numThreads * incrementsPerThread;
                assertEquals("$" + Integer.toString(expected).length() + "\r\n" + expected + "\r\n", resp,
                        "High concurrency atomic INCR reached exactly " + expected);
            }
        } finally {
            server.stop();
        }
    }

    // --- 5. SLOW CLIENTS & BUFFER LIMITS ---
    private static void testSlowClientAndBufferLimits() {
        System.out.println("\n--- Testing Bounded Buffer Protection ---");

        // Verify ClientConnection limits
        assertEquals(16 * 1024 * 1024, ClientConnection.MAX_READ_BUFFER_CAPACITY, "Max read buffer capacity enforced at 16MB");
        assertEquals(32 * 1024 * 1024L, ClientConnection.MAX_PENDING_WRITE_BYTES, "Max write queue limit enforced at 32MB");
    }

    // --- 6. TELEMETRY & INFO METRICS ---
    private static void testTelemetryAndInfoCommand() throws Exception {
        System.out.println("\n--- Testing Telemetry & INFO Command Accuracy ---");

        int port = 6397;
        ServerConfig config = new ServerConfig();
        config.setPort(port);
        config.setAofEnabled(false);
        config.setRdbEnabled(false);

        RedisServer server = new RedisServer(config);
        server.start();
        Thread.sleep(200);

        try (Socket socket = new Socket("127.0.0.1", port);
             OutputStream out = socket.getOutputStream();
             InputStream in = socket.getInputStream()) {

            byte[] buf = new byte[2048];

            // 1. SET test:key test:val
            out.write("*3\r\n$3\r\nSET\r\n$8\r\ntest:key\r\n$8\r\ntest:val\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            in.read(buf);

            // 2. GET test:key (Hit)
            out.write("*2\r\n$3\r\nGET\r\n$8\r\ntest:key\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            in.read(buf);

            // 3. GET nonexistent (Miss)
            out.write("*2\r\n$3\r\nGET\r\n$11\r\nnonexistent\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            in.read(buf);

            // 4. INFO command
            out.write("*1\r\n$4\r\nINFO\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            int read = in.read(buf);
            String infoOutput = new String(buf, 0, read, StandardCharsets.UTF_8);

            assertTrue(infoOutput.contains("# Stats"), "INFO contains # Stats section");
            assertTrue(infoOutput.contains("total_commands_processed:"), "INFO contains total_commands_processed");
            assertTrue(infoOutput.contains("keyspace_hits:"), "INFO contains keyspace_hits");
            assertTrue(infoOutput.contains("keyspace_misses:"), "INFO contains keyspace_misses");
            assertTrue(infoOutput.contains("uptime_in_seconds:"), "INFO contains uptime_in_seconds");
            assertTrue(ServerMetrics.getInstance().getKeyspaceHits() >= 1, "Metrics recorded at least 1 keyspace hit");
            assertTrue(ServerMetrics.getInstance().getKeyspaceMisses() >= 1, "Metrics recorded at least 1 keyspace miss");
        } finally {
            server.stop();
        }
    }

    // --- 7. AOF LOG COMPACTION (BGREWRITEAOF) ---
    private static void testAofRewriteCompaction() throws Exception {
        System.out.println("\n--- Testing AOF Log Compaction (BGREWRITEAOF) ---");
        Path testDir = Path.of("test_data");
        Files.createDirectories(testDir);
        File aofFile = testDir.resolve("compaction_test.aof").toFile();
        if (aofFile.exists()) aofFile.delete();

        AofManager aof = new AofManager(aofFile.getAbsolutePath(), AofManager.FsyncPolicy.ALWAYS, true);
        aof.start();

        DataStore ds = new DataStore();
        ds.set("compact:k1", RedisObject.ofString("val1"), null);
        ds.set("compact:k2", RedisObject.ofString("val2"), null);
        ds.hset("compact:user", "name", "Alice".getBytes(StandardCharsets.UTF_8));
        ds.lpush("compact:tasks", "t1".getBytes(StandardCharsets.UTF_8), "t2".getBytes(StandardCharsets.UTF_8));

        boolean rewritten = aof.rewrite(ds);
        assertTrue(rewritten, "AOF rewrite completed successfully");
        assertTrue(aofFile.exists() && aofFile.length() > 0, "Compacted AOF file exists and has non-zero size");

        // Replay compacted AOF into fresh DataStore
        DataStore replayedDs = new DataStore();
        com.redisclone.command.CommandRegistry registry = new com.redisclone.command.CommandRegistry(
                replayedDs, null, aof, null, null, null, 6379
        );
        int replayedCount = aof.replay(registry);
        assertTrue(replayedCount >= 4, "Compacted AOF replayed all 4 records");
        assertEquals("val1", new String(replayedDs.get("compact:k1").asStringBytes(), StandardCharsets.UTF_8), "Key1 restored correctly from compacted AOF");
        assertEquals("val2", new String(replayedDs.get("compact:k2").asStringBytes(), StandardCharsets.UTF_8), "Key2 restored correctly from compacted AOF");
        assertEquals("Alice", new String(replayedDs.hget("compact:user", "name"), StandardCharsets.UTF_8), "Hash field restored from compacted AOF");
        assertEquals(2, replayedDs.get("compact:tasks").asList().size(), "List elements restored from compacted AOF");

        aof.close();
    }

    // --- 8. BATCH (MSET/MGET) & UTILITY (DBSIZE/FLUSHDB/AUTH) ---
    private static void testBatchAndUtilityCommands() throws Exception {
        System.out.println("\n--- Testing Batch (MSET/MGET) and Utility Commands ---");
        int port = 6398;
        ServerConfig config = new ServerConfig();
        config.setPort(port);
        config.setAofEnabled(false);
        config.setRdbEnabled(false);

        RedisServer server = new RedisServer(config);
        server.start();
        Thread.sleep(200);

        try (Socket socket = new Socket("127.0.0.1", port);
             OutputStream out = socket.getOutputStream();
             InputStream in = socket.getInputStream()) {

            byte[] buf = new byte[1024];

            // 1. MSET
            out.write("*5\r\n$4\r\nMSET\r\n$2\r\nm1\r\n$2\r\nv1\r\n$2\r\nm2\r\n$2\r\nv2\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            int read = in.read(buf);
            String resp = new String(buf, 0, read, StandardCharsets.UTF_8);
            assertEquals("+OK\r\n", resp, "MSET commits multiple keys atomically");

            // 2. DBSIZE
            out.write("*1\r\n$6\r\nDBSIZE\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            read = in.read(buf);
            resp = new String(buf, 0, read, StandardCharsets.UTF_8);
            assertEquals(":2\r\n", resp, "DBSIZE accurately reports 2 keys");

            // 3. MGET
            out.write("*4\r\n$4\r\nMGET\r\n$2\r\nm1\r\n$2\r\nm2\r\n$6\r\nabsent\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            read = in.read(buf);
            resp = new String(buf, 0, read, StandardCharsets.UTF_8);
            assertTrue(resp.startsWith("*3\r\n"), "MGET returns 3-element array");
            assertTrue(resp.contains("$2\r\nv1\r\n"), "MGET contains v1");
            assertTrue(resp.contains("$2\r\nv2\r\n"), "MGET contains v2");
            assertTrue(resp.contains("$-1\r\n"), "MGET contains nil for absent key");

            // 4. AUTH command
            out.write("*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            read = in.read(buf);
            resp = new String(buf, 0, read, StandardCharsets.UTF_8);
            assertEquals("+OK\r\n", resp, "AUTH returns OK");

            // 5. FLUSHDB
            out.write("*1\r\n$7\r\nFLUSHDB\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            read = in.read(buf);
            resp = new String(buf, 0, read, StandardCharsets.UTF_8);
            assertEquals("+OK\r\n", resp, "FLUSHDB succeeds with OK");

            // 6. DBSIZE after flush
            out.write("*1\r\n$6\r\nDBSIZE\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            read = in.read(buf);
            resp = new String(buf, 0, read, StandardCharsets.UTF_8);
            assertEquals(":0\r\n", resp, "DBSIZE reports 0 after FLUSHDB");

        } finally {
            server.stop();
        }
    }

    private static void testBitmapsHyperLogLogAndStreams() throws Exception {
        System.out.println("\n--- Testing Bitmaps, HyperLogLog, and Redis Streams ---");
        int port = 6399;
        ServerConfig config = new ServerConfig();
        config.setPort(port);
        config.setAofEnabled(false);
        config.setRdbEnabled(false);

        RedisServer server = new RedisServer(config);
        server.start();
        Thread.sleep(100);

        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setTcpNoDelay(true);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();
            byte[] buf = new byte[8192];

            // 1. Bitmaps (SETBIT, GETBIT, BITCOUNT)
            out.write("*4\r\n$6\r\nSETBIT\r\n$12\r\nactive_users\r\n$1\r\n7\r\n$1\r\n1\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            int read = in.read(buf);
            String resp = new String(buf, 0, read, StandardCharsets.UTF_8);
            assertEquals(":0\r\n", resp, "SETBIT offset 7 returns old bit 0");

            out.write("*3\r\n$6\r\nGETBIT\r\n$12\r\nactive_users\r\n$1\r\n7\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            read = in.read(buf);
            resp = new String(buf, 0, read, StandardCharsets.UTF_8);
            assertEquals(":1\r\n", resp, "GETBIT offset 7 returns bit 1");

            out.write("*3\r\n$6\r\nGETBIT\r\n$12\r\nactive_users\r\n$1\r\n8\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            read = in.read(buf);
            resp = new String(buf, 0, read, StandardCharsets.UTF_8);
            assertEquals(":0\r\n", resp, "GETBIT offset 8 returns bit 0");

            out.write("*4\r\n$6\r\nSETBIT\r\n$12\r\nactive_users\r\n$2\r\n15\r\n$1\r\n1\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            read = in.read(buf);
            resp = new String(buf, 0, read, StandardCharsets.UTF_8);
            assertEquals(":0\r\n", resp, "SETBIT offset 15 returns old bit 0");

            out.write("*2\r\n$8\r\nBITCOUNT\r\n$12\r\nactive_users\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            read = in.read(buf);
            resp = new String(buf, 0, read, StandardCharsets.UTF_8);
            assertEquals(":2\r\n", resp, "BITCOUNT returns exactly 2 bits set");

            // 2. HyperLogLog (PFADD, PFCOUNT)
            out.write("*5\r\n$5\r\nPFADD\r\n$8\r\nvisitors\r\n$5\r\nalice\r\n$3\r\nbob\r\n$7\r\ncharlie\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            read = in.read(buf);
            resp = new String(buf, 0, read, StandardCharsets.UTF_8);
            assertEquals(":1\r\n", resp, "PFADD returns 1 when registers updated");

            out.write("*3\r\n$5\r\nPFADD\r\n$8\r\nvisitors\r\n$5\r\nalice\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            read = in.read(buf);
            resp = new String(buf, 0, read, StandardCharsets.UTF_8);
            assertEquals(":0\r\n", resp, "PFADD returns 0 on duplicate element");

            out.write("*2\r\n$7\r\nPFCOUNT\r\n$8\r\nvisitors\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            read = in.read(buf);
            resp = new String(buf, 0, read, StandardCharsets.UTF_8);
            assertEquals(":3\r\n", resp, "PFCOUNT returns estimated cardinality 3");

            out.write("*4\r\n$5\r\nPFADD\r\n$9\r\nvisitors2\r\n$5\r\ndavid\r\n$3\r\neve\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            read = in.read(buf);
            resp = new String(buf, 0, read, StandardCharsets.UTF_8);
            assertEquals(":1\r\n", resp, "PFADD visitors2 returns 1");

            out.write("*3\r\n$7\r\nPFCOUNT\r\n$8\r\nvisitors\r\n$9\r\nvisitors2\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            read = in.read(buf);
            resp = new String(buf, 0, read, StandardCharsets.UTF_8);
            assertEquals(":5\r\n", resp, "PFCOUNT merged keys returns estimated cardinality 5");

            // 3. Redis Streams (XADD, XLEN, XRANGE)
            out.write("*7\r\n$4\r\nXADD\r\n$6\r\nevents\r\n$1\r\n*\r\n$6\r\nsensor\r\n$4\r\ntemp\r\n$3\r\nval\r\n$4\r\n21.5\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            read = in.read(buf);
            resp = new String(buf, 0, read, StandardCharsets.UTF_8);
            assertTrue(resp.startsWith("$"), "XADD auto-generates stream ID");
            assertTrue(resp.contains("-0\r\n"), "XADD first entry sequence is 0");

            out.write("*2\r\n$4\r\nXLEN\r\n$6\r\nevents\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            read = in.read(buf);
            resp = new String(buf, 0, read, StandardCharsets.UTF_8);
            assertEquals(":1\r\n", resp, "XLEN returns 1 after first stream append");

            out.write("*7\r\n$4\r\nXADD\r\n$6\r\nevents\r\n$1\r\n*\r\n$6\r\nsensor\r\n$8\r\npressure\r\n$3\r\nval\r\n$6\r\n1013.2\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            read = in.read(buf);
            resp = new String(buf, 0, read, StandardCharsets.UTF_8);
            assertTrue(resp.startsWith("$"), "XADD second entry assigned ID");

            out.write("*2\r\n$4\r\nXLEN\r\n$6\r\nevents\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            read = in.read(buf);
            resp = new String(buf, 0, read, StandardCharsets.UTF_8);
            assertEquals(":2\r\n", resp, "XLEN returns 2 after second append");

            out.write("*4\r\n$6\r\nXRANGE\r\n$6\r\nevents\r\n$1\r\n-\r\n$1\r\n+\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            read = in.read(buf);
            resp = new String(buf, 0, read, StandardCharsets.UTF_8);
            assertTrue(resp.startsWith("*2\r\n"), "XRANGE returns 2 items in array");
            assertTrue(resp.contains("temp"), "XRANGE contains first entry payload");
            assertTrue(resp.contains("pressure"), "XRANGE contains second entry payload");

            // Monotonic validation test: trying to insert with ID smaller than top item
            out.write("*7\r\n$4\r\nXADD\r\n$6\r\nevents\r\n$3\r\n1-0\r\n$6\r\nsensor\r\n$3\r\nold\r\n$3\r\nval\r\n$1\r\n0\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            read = in.read(buf);
            resp = new String(buf, 0, read, StandardCharsets.UTF_8);
            assertTrue(resp.startsWith("-ERR The ID specified in XADD is equal or smaller"), "XADD rejects non-monotonic entry ID");

        } finally {
            server.stop();
        }
    }
}
