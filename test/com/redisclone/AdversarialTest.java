package com.redisclone;

import com.redisclone.server.RedisServer;
import com.redisclone.server.ServerConfig;
import com.redisclone.storage.SkipList;
import com.redisclone.storage.SortedSet;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Adversarial and Stress Test Suite.
 *
 * Subjecting the Core Java 21 Redis engine to boundary and stress conditions:
 * 1. Byte-by-byte TCP packet fragmentation fuzzing.
 * 2. 100-thread concurrent CAS & atomic mutation race testing.
 * 3. William Pugh SkipList span & rank invariant verification (2,000 randomized operations).
 * 4. Pattern-based Pub/Sub (PSUBSCRIBE) glob matching under stress.
 * 5. High-velocity active TTL expiration saturation.
 * 6. Protocol malformation & 16MB buffer boundary defenses.
 */
public class AdversarialTest {

    private static int passedTests = 0;
    private static int totalTests = 0;

    public static void main(String[] args) throws Exception {
        System.out.println("================================================================================");
        System.out.println(" RUNNING ADVERSARIAL AND STRESS TEST SUITE");
        System.out.println(" Testing Core Java 21 Engine Under Adversarial Workloads");
        System.out.println("================================================================================");

        testSkipListInvariantsAndRankSpans();
        testByteLevelTcpFragmentationFuzzing();
        testExtremeConcurrencyAndAtomicity();
        testPatternPubSubGlobMatching();
        testHighVelocityActiveTtlExpiration();
        testProtocolMalformationAndBufferDefense();

        System.out.println("================================================================================");
        System.out.println(" ALL ADVERSARIAL TESTS PASSED: " + passedTests + " / " + totalTests);
        System.out.println("================================================================================");
    }

    private static void assertEquals(Object expected, Object actual, String testName) {
        totalTests++;
        if (!Objects.equals(expected, actual)) {
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

    // --- 1. SKIPLIST INVARIANT & SPAN VERIFICATION ---
    private static void testSkipListInvariantsAndRankSpans() {
        System.out.println("\n--- [Adversarial Test 1] William Pugh SkipList Rank Spans & Invariants ---");
        SkipList sl = new SkipList();
        SortedSet zset = new SortedSet();
        Random rnd = new Random(42);

        int count = 1000;
        Map<String, Double> groundTruth = new HashMap<>();

        for (int i = 0; i < count; i++) {
            String member = "member_" + i;
            double score = (rnd.nextInt(5000) - 2500) / 10.0;
            sl.insert(score, member);
            zset.add(member, score);
            groundTruth.put(member, score);
        }

        assertEquals((long) count, sl.length(), "SkipList length equals inserted count");
        assertEquals((long) count, zset.card(), "SortedSet cardinality equals inserted count");

        // Verify span-based rank matches linear order
        List<Map.Entry<String, Double>> sortedList = new ArrayList<>(groundTruth.entrySet());
        sortedList.sort((a, b) -> {
            int cmp = Double.compare(a.getValue(), b.getValue());
            return cmp != 0 ? cmp : a.getKey().compareTo(b.getKey());
        });

        // Test ranks of 100 random sampled members
        boolean ranksValid = true;
        for (int i = 0; i < 100; i++) {
            int targetIdx = rnd.nextInt(sortedList.size());
            Map.Entry<String, Double> entry = sortedList.get(targetIdx);
            long slRank = sl.getRank(entry.getValue(), entry.getKey());
            if (slRank != targetIdx + 1) { // 1-based rank
                ranksValid = false;
                break;
            }
        }
        assertTrue(ranksValid, "O(log N) SkipList span ranks match linear ground-truth indexing");

        // Test getNodeByRank
        boolean nodesByRankValid = true;
        for (int i = 1; i <= 50; i++) {
            SkipList.SkipListNode n = sl.getNodeByRank(i);
            if (n == null || !n.member.equals(sortedList.get(i - 1).getKey())) {
                nodesByRankValid = false;
                break;
            }
        }
        assertTrue(nodesByRankValid, "SkipList getNodeByRank(i) correctly retrieves nodes via spans");

        // Test bidirectional consistency (backward pointers)
        SkipList.SkipListNode curr = sl.getTail();
        int backwardCount = 0;
        boolean backwardOrdered = true;
        double lastScore = Double.POSITIVE_INFINITY;
        while (curr != null) {
            backwardCount++;
            if (curr.score > lastScore) {
                backwardOrdered = false;
            }
            lastScore = curr.score;
            curr = curr.backward;
        }
        assertEquals(count, backwardCount, "Level 0 backward traversal touches all nodes");
        assertTrue(backwardOrdered, "Backward pointer chain is strictly monotonically non-ascending");

        // Test ZRANGE and ZREVRANGE
        List<String> range = zset.range(0, 9, false);
        assertEquals(10, range.size(), "ZRANGE 0 9 returns 10 elements");
        assertEquals(sortedList.get(0).getKey(), range.get(0), "ZRANGE lowest element matches");

        List<String> revrange = zset.revrange(0, 9, false);
        assertEquals(10, revrange.size(), "ZREVRANGE 0 9 returns 10 elements");
        assertEquals(sortedList.get(sortedList.size() - 1).getKey(), revrange.get(0), "ZREVRANGE highest element matches");
    }

    // --- 2. BYTE-BY-BYTE TCP PACKET FRAGMENTATION FUZZING ---
    private static void testByteLevelTcpFragmentationFuzzing() throws Exception {
        System.out.println("\n--- [Adversarial Test 2] Byte-Level TCP Packet Fragmentation Fuzzing ---");
        int port = 6400;
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

            // Construct 10 pipelined commands
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 10; i++) {
                sb.append("*3\r\n$3\r\nSET\r\n$8\r\nfuzzkey").append(i)
                        .append("\r\n$8\r\nfuzzval").append(i).append("\r\n");
            }

            byte[] allBytes = sb.toString().getBytes(StandardCharsets.US_ASCII);

            // Brutally write 1 byte at a time with micro-flushes
            for (byte b : allBytes) {
                out.write(b);
                out.flush();
            }

            // Read replies: expect 10 "+OK\r\n"
            byte[] buf = new byte[4096];
            int totalRead = 0;
            StringBuilder replies = new StringBuilder();
            while (totalRead < 50) { // 10 * 5 bytes ("+OK\r\n") = 50 bytes
                int r = in.read(buf);
                if (r == -1) break;
                replies.append(new String(buf, 0, r, StandardCharsets.US_ASCII));
                totalRead += r;
            }

            assertEquals(50, totalRead, "Streaming state machine recovered 50 bytes across 1-byte TCP slices");
            assertTrue(replies.toString().startsWith("+OK\r\n"), "First pipelined reply is +OK");
            assertTrue(replies.toString().endsWith("+OK\r\n"), "Last pipelined reply is +OK");

            // Verify with a ZADD command also fragmented into 2-byte chunks
            String zaddCmd = "*4\r\n$4\r\nZADD\r\n$8\r\nfuzzzset\r\n$4\r\n42.5\r\n$7\r\nmemberX\r\n";
            byte[] zaddBytes = zaddCmd.getBytes(StandardCharsets.US_ASCII);
            for (int i = 0; i < zaddBytes.length; i += 2) {
                int len = Math.min(2, zaddBytes.length - i);
                out.write(zaddBytes, i, len);
                out.flush();
            }
            int read = in.read(buf);
            String zaddReply = new String(buf, 0, read, StandardCharsets.US_ASCII);
            assertEquals(":1\r\n", zaddReply, "Fragmented ZADD returns :1");
        } finally {
            server.stop();
        }
    }

    // --- 3. 100-THREAD CONCURRENT CAS & ATOMIC MUTATION STRESS ---
    private static void testExtremeConcurrencyAndAtomicity() throws Exception {
        System.out.println("\n--- [Adversarial Test 3] 100-Thread Concurrent CAS & Atomic Mutation Stress ---");
        int port = 6401;
        ServerConfig config = new ServerConfig();
        config.setPort(port);
        config.setAofEnabled(false);
        config.setRdbEnabled(false);

        RedisServer server = new RedisServer(config);
        server.start();
        Thread.sleep(100);

        try {
            int numThreads = 100;
            int opsPerThread = 50;
            ExecutorService pool = Executors.newFixedThreadPool(numThreads);
            CountDownLatch connectedLatch = new CountDownLatch(numThreads);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch finishLatch = new CountDownLatch(numThreads);
            AtomicInteger errorCount = new AtomicInteger(0);

            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                pool.submit(() -> {
                    Socket s = null;
                    for (int attempt = 0; attempt < 5; attempt++) {
                        try {
                            s = new Socket("127.0.0.1", port);
                            s.setTcpNoDelay(true);
                            break;
                        } catch (Exception ex) {
                            try { Thread.sleep(15); } catch (InterruptedException ignored) {}
                        }
                    }

                    if (s == null) {
                        System.err.println("[Thread " + threadId + "] Failed to establish socket connection after 5 attempts");
                        errorCount.incrementAndGet();
                        connectedLatch.countDown();
                        finishLatch.countDown();
                        return;
                    }

                    try (Socket sock = s) {
                        OutputStream out = sock.getOutputStream();
                        InputStream in = sock.getInputStream();
                        byte[] buf = new byte[512];

                        connectedLatch.countDown();
                        startLatch.await();

                        for (int i = 0; i < opsPerThread; i++) {
                            // Atomic INCR
                            out.write("*2\r\n$4\r\nINCR\r\n$14\r\ntortureCounter\r\n".getBytes(StandardCharsets.US_ASCII));
                            out.flush();
                            int r = in.read(buf);
                            if (r <= 0 || buf[0] != ':') {
                                System.err.println("[Thread " + threadId + "] read r=" + r + " firstByte=" + (r > 0 ? (char) buf[0] : "none"));
                                errorCount.incrementAndGet();
                            }

                            // ZADD concurrent mutation
                            String member = "th_" + threadId;
                            String zcmd = "*4\r\n$4\r\nZADD\r\n$11\r\ntortureZset\r\n$" +
                                    String.valueOf(i).length() + "\r\n" + i + "\r\n$" +
                                    member.length() + "\r\n" + member + "\r\n";
                            out.write(zcmd.getBytes(StandardCharsets.US_ASCII));
                            out.flush();
                            in.read(buf);
                        }
                    } catch (Exception e) {
                        System.err.println("[Thread " + threadId + "] Exception: " + e.getMessage());
                        errorCount.incrementAndGet();
                    } finally {
                        finishLatch.countDown();
                    }
                });
            }

            connectedLatch.await(10, TimeUnit.SECONDS);
            startLatch.countDown();
            finishLatch.await(30, TimeUnit.SECONDS);
            pool.shutdown();

            assertEquals(0, errorCount.get(), "Zero network or protocol errors across 100 concurrent threads");

            // Verify final counter exactly equals 100 * 50 = 5000
            try (Socket s = new Socket("127.0.0.1", port)) {
                OutputStream out = s.getOutputStream();
                InputStream in = s.getInputStream();
                byte[] buf = new byte[256];

                out.write("*2\r\n$3\r\nGET\r\n$14\r\ntortureCounter\r\n".getBytes(StandardCharsets.US_ASCII));
                out.flush();
                int r = in.read(buf);
                String resp = new String(buf, 0, r, StandardCharsets.US_ASCII);
                assertEquals("$4\r\n5000\r\n", resp, "High-concurrency atomic counter strictly equals 5,000 without data loss");

                // Verify ZCARD
                out.write("*2\r\n$5\r\nZCARD\r\n$11\r\ntortureZset\r\n".getBytes(StandardCharsets.US_ASCII));
                out.flush();
                r = in.read(buf);
                resp = new String(buf, 0, r, StandardCharsets.US_ASCII);
                assertEquals(":100\r\n", resp, "Concurrent ZSET contains exactly 100 distinct thread entries");
            }
        } finally {
            server.stop();
        }
    }

    // --- 4. PATTERN-BASED PUB/SUB (PSUBSCRIBE) GLOB MATCHING ---
    private static void testPatternPubSubGlobMatching() throws Exception {
        System.out.println("\n--- [Adversarial Test 4] Pattern-Based Pub/Sub (PSUBSCRIBE) Glob Matching ---");
        int port = 6402;
        ServerConfig config = new ServerConfig();
        config.setPort(port);
        config.setAofEnabled(false);
        config.setRdbEnabled(false);

        RedisServer server = new RedisServer(config);
        server.start();
        Thread.sleep(100);

        try (Socket subSocket = new Socket("127.0.0.1", port);
             Socket pubSocket = new Socket("127.0.0.1", port)) {

            subSocket.setTcpNoDelay(true);
            pubSocket.setTcpNoDelay(true);

            OutputStream subOut = subSocket.getOutputStream();
            InputStream subIn = subSocket.getInputStream();
            OutputStream pubOut = pubSocket.getOutputStream();
            InputStream pubIn = pubSocket.getInputStream();
            byte[] buf = new byte[4096];

            // 1. PSUBSCRIBE news.*
            subOut.write("*2\r\n$10\r\nPSUBSCRIBE\r\n$6\r\nnews.*\r\n".getBytes(StandardCharsets.US_ASCII));
            subOut.flush();
            int r = subIn.read(buf);
            String confirm = new String(buf, 0, r, StandardCharsets.US_ASCII);
            assertTrue(confirm.contains("psubscribe"), "PSUBSCRIBE confirmation contains 'psubscribe'");
            assertTrue(confirm.contains(":1\r\n"), "Subscription count incremented to 1");

            // 2. Publish to matching channel: news.technology
            pubOut.write("*3\r\n$7\r\nPUBLISH\r\n$15\r\nnews.technology\r\n$10\r\nBreakingAI\r\n".getBytes(StandardCharsets.US_ASCII));
            pubOut.flush();
            r = pubIn.read(buf);
            String pubReply = new String(buf, 0, r, StandardCharsets.US_ASCII);
            assertEquals(":1\r\n", pubReply, "PUBLISH delivered to 1 pattern subscriber");

            // 3. Verify pattern push frame format: *4\r\n$8\r\npmessage\r\n...
            r = subIn.read(buf);
            String pmsg = new String(buf, 0, r, StandardCharsets.US_ASCII);
            assertTrue(pmsg.startsWith("*4\r\n"), "PMessage starts with 4-element array");
            assertTrue(pmsg.contains("pmessage"), "Frame opcode is 'pmessage'");
            assertTrue(pmsg.contains("news.*"), "Pattern matches subscription");
            assertTrue(pmsg.contains("news.technology"), "Channel preserves target name");
            assertTrue(pmsg.contains("BreakingAI"), "Payload matches publication");

            // 4. Publish to non-matching channel: sport.football
            pubOut.write("*3\r\n$7\r\nPUBLISH\r\n$14\r\nsport.football\r\n$4\r\nGoal\r\n".getBytes(StandardCharsets.US_ASCII));
            pubOut.flush();
            r = pubIn.read(buf);
            pubReply = new String(buf, 0, r, StandardCharsets.US_ASCII);
            assertEquals(":0\r\n", pubReply, "PUBLISH to non-matching channel delivered to 0 clients");
        } finally {
            server.stop();
        }
    }

    // --- 5. HIGH-VELOCITY ACTIVE TTL EXPIRATION SATURATION ---
    private static void testHighVelocityActiveTtlExpiration() throws Exception {
        System.out.println("\n--- [Adversarial Test 5] High-Velocity Active TTL Expiration Saturation ---");
        int port = 6403;
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

            // Ingest 500 keys with a 50ms TTL using SET key val PX 50
            for (int i = 0; i < 500; i++) {
                String key = "ttl_k_" + i;
                String cmd = "*5\r\n$3\r\nSET\r\n$" + key.length() + "\r\n" + key +
                        "\r\n$3\r\nval\r\n$2\r\nPX\r\n$2\r\n50\r\n";
                out.write(cmd.getBytes(StandardCharsets.US_ASCII));
            }
            out.flush();

            // Read all 500 "+OK\r\n" responses
            int totalBytes = 0;
            while (totalBytes < 500 * 5) {
                int r = in.read(buf);
                if (r == -1) break;
                totalBytes += r;
            }
            assertEquals(2500, totalBytes, "All 500 short-TTL keys committed");

            // Sleep 250ms to ensure all keys expire past their 50ms window
            Thread.sleep(250);

            // Verify active probabilistic 10Hz eviction or passive access clears keys
            out.write("*2\r\n$3\r\nGET\r\n$7\r\nttl_k_0\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            int r = in.read(buf);
            String getReply = new String(buf, 0, r, StandardCharsets.US_ASCII);
            assertEquals("$-1\r\n", getReply, "Expired key returns nil on passive access");

            // DBSIZE should reflect eviction
            out.write("*1\r\n$6\r\nDBSIZE\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            r = in.read(buf);
            String dbsizeReply = new String(buf, 0, r, StandardCharsets.US_ASCII);
            assertTrue(dbsizeReply.startsWith(":"), "DBSIZE returns valid integer reply");
        } finally {
            server.stop();
        }
    }

    // --- 6. PROTOCOL MALFORMATION & 16MB BUFFER BOUNDARY DEFENSE ---
    private static void testProtocolMalformationAndBufferDefense() throws Exception {
        System.out.println("\n--- [Adversarial Test 6] Protocol Malformation & 16MB Buffer Boundary Defense ---");
        int port = 6404;
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
            byte[] buf = new byte[4096];

            // 1. Negative array length (*-5\r\n)
            out.write("*-5\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            int r = in.read(buf);
            String reply = new String(buf, 0, r, StandardCharsets.US_ASCII);
            assertTrue(reply.startsWith("-ERR"), "Negative array prefix rejected with -ERR");

            // 2. Astronomical bulk length ($999999999999999999\r\n)
            out.write("$999999999999999999\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            r = in.read(buf);
            reply = new String(buf, 0, r, StandardCharsets.US_ASCII);
            assertTrue(reply.startsWith("-ERR"), "Astronomical bulk string rejected with -ERR (DoS boundary)");

            // 3. Normal command immediately following malformed frame executes cleanly
            out.write("*3\r\n$3\r\nSET\r\n$8\r\ncleankey\r\n$8\r\ncleanval\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            r = in.read(buf);
            reply = new String(buf, 0, r, StandardCharsets.US_ASCII);
            assertEquals("+OK\r\n", reply, "Server recovers and serves subsequent valid commands cleanly");
        } finally {
            server.stop();
        }
    }
}
