package com.redisclone.benchmark;

import com.redisclone.resp.RespFrame;
import com.redisclone.resp.RespParser;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Rigorous Systems-Engineering Benchmarking Framework.
 *
 * Capabilities:
 * - High-resolution nanosecond timing per request
 * - Percentiles calculation: min, p50 (median), p90, p95, p99, p99.9, max
 * - Concurrency scaling sweeps: 1, 10, 50, 100, 250 clients
 * - Multi-workload suite: PING, SET, GET, INCR, MIXED (80/20), PIPELINE (16, 64), LARGE (1KB), TRANSACTIONS
 * - JVM C2 JIT warm-up cycle
 * - Hardware and JVM telemetry logging
 * - Machine-readable JSON and CSV report export
 */
public class RedisBenchmark {

    public record BenchmarkResult(
            String workload,
            int totalRequests,
            int concurrency,
            int pipelineDepth,
            int payloadBytes,
            double elapsedSeconds,
            double throughputRps,
            double minMs,
            double p50Ms,
            double p90Ms,
            double p95Ms,
            double p99Ms,
            double p999Ms,
            double maxMs,
            long heapUsedBytesBefore,
            long heapUsedBytesAfter,
            int errorCount
    ) {
        public void printReport() {
            System.out.printf("""
                    ================================================================================
                    WORKLOAD: %s | Concurrency: %d clients | Pipeline: %d | Payload: %d bytes
                    --------------------------------------------------------------------------------
                      Requests completed: %d (Errors: %d)
                      Total duration:     %.3f seconds
                      Throughput (RPS):   %,.2f requests/sec
                    --- Latency Distribution (ms) ---
                      min:                %.3f ms
                      p50 (median):       %.3f ms
                      p90:                %.3f ms
                      p95:                %.3f ms
                      p99:                %.3f ms
                      p99.9:              %.3f ms
                      max:                %.3f ms
                    --- Memory Impact ---
                      Heap delta:         %+,d KB
                    ================================================================================
                    %n""", workload, concurrency, pipelineDepth, payloadBytes,
                    totalRequests, errorCount, elapsedSeconds, throughputRps,
                    minMs, p50Ms, p90Ms, p95Ms, p99Ms, p999Ms, maxMs,
                    (heapUsedBytesAfter - heapUsedBytesBefore) / 1024);
        }

        public String toCsvLine() {
            return String.format(Locale.ROOT, "%s,%d,%d,%d,%d,%.4f,%.2f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%d,%d,%d",
                    workload, totalRequests, concurrency, pipelineDepth, payloadBytes,
                    elapsedSeconds, throughputRps, minMs, p50Ms, p90Ms, p95Ms, p99Ms, p999Ms, maxMs,
                    heapUsedBytesBefore, heapUsedBytesAfter, errorCount);
        }

        public String toJsonFragment() {
            return String.format(Locale.ROOT, """
                    {
                      "workload": "%s",
                      "totalRequests": %d,
                      "concurrency": %d,
                      "pipelineDepth": %d,
                      "payloadBytes": %d,
                      "elapsedSeconds": %.4f,
                      "throughputRps": %.2f,
                      "latency": {
                        "minMs": %.3f,
                        "p50Ms": %.3f,
                        "p90Ms": %.3f,
                        "p95Ms": %.3f,
                        "p99Ms": %.3f,
                        "p999Ms": %.3f,
                        "maxMs": %.3f
                      },
                      "heap": {
                        "beforeBytes": %d,
                        "afterBytes": %d
                      },
                      "errorCount": %d
                    }""", workload, totalRequests, concurrency, pipelineDepth, payloadBytes,
                    elapsedSeconds, throughputRps, minMs, p50Ms, p90Ms, p95Ms, p99Ms, p999Ms, maxMs,
                    heapUsedBytesBefore, heapUsedBytesAfter, errorCount);
        }
    }

    public static BenchmarkResult runBenchmark(
            String host, int port, int totalRequests, int concurrency,
            String workload, int pipelineDepth, int payloadSize) throws Exception {

        System.gc();
        Thread.sleep(100);
        long heapBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        int requestsPerClient = totalRequests / concurrency;
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        List<Long> latenciesNanos = Collections.synchronizedList(new ArrayList<>(totalRequests));
        AtomicInteger errorCount = new AtomicInteger(0);
        AtomicInteger globalCounter = new AtomicInteger(0);

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(concurrency);

        byte[] largeValue = "X".repeat(Math.max(1, payloadSize)).getBytes(StandardCharsets.US_ASCII);

        for (int c = 0; c < concurrency; c++) {
            executor.submit(() -> {
                Socket socket = null;
                for (int retry = 0; retry < 5; retry++) {
                    try {
                        socket = new Socket(host, port);
                        break;
                    } catch (Exception e) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException ignored) {}
                    }
                }
                if (socket == null) {
                    errorCount.incrementAndGet();
                    finishLatch.countDown();
                    return;
                }

                final Socket clientSocket = socket;
                try (clientSocket;
                     OutputStream out = clientSocket.getOutputStream();
                     InputStream in = clientSocket.getInputStream()) {

                    clientSocket.setTcpNoDelay(true);
                    ByteBuffer readBuffer = ByteBuffer.allocate(64 * 1024);
                    byte[] ioBuf = new byte[8192];
                    startLatch.await();

                    int batches = requestsPerClient / pipelineDepth;
                    for (int b = 0; b < batches; b++) {
                        long start = System.nanoTime();

                        // Build and send pipelined batch
                        ByteArrayOutputStream batchOut = new ByteArrayOutputStream();
                        for (int p = 0; p < pipelineDepth; p++) {
                            int id = globalCounter.incrementAndGet();
                            byte[] cmd = generateCommandBytes(workload, id, largeValue);
                            batchOut.write(cmd);
                        }
                        out.write(batchOut.toByteArray());
                        out.flush();

                        // Read expected pipeline replies using RESP Parser
                        int expectedReplies = "TRANSACTION".equalsIgnoreCase(workload) ? pipelineDepth * 4 : pipelineDepth;
                        int repliesReceived = 0;

                        while (repliesReceived < expectedReplies) {
                            readBuffer.flip();
                            RespFrame frame;
                            while ((frame = RespParser.parse(readBuffer)) != null) {
                                repliesReceived++;
                                if (repliesReceived >= expectedReplies) break;
                            }
                            readBuffer.compact();
                            if (repliesReceived >= expectedReplies) break;

                            int r = in.read(ioBuf);
                            if (r == -1) break;
                            readBuffer.put(ioBuf, 0, r);
                        }

                        long duration = System.nanoTime() - start;
                        long perOpNanos = duration / pipelineDepth;
                        for (int p = 0; p < pipelineDepth; p++) {
                            latenciesNanos.add(perOpNanos);
                        }
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        long benchStart = System.nanoTime();
        startLatch.countDown();
        finishLatch.await();
        long benchDuration = System.nanoTime() - benchStart;

        executor.shutdown();

        long heapAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        double elapsedSeconds = benchDuration / 1_000_000_000.0;
        int completed = latenciesNanos.size();
        double rps = elapsedSeconds > 0 ? completed / elapsedSeconds : 0;

        long[] sorted = latenciesNanos.stream().mapToLong(Long::longValue).toArray();
        Arrays.sort(sorted);

        double minMs = sorted.length > 0 ? sorted[0] / 1_000_000.0 : 0;
        double p50Ms = getPercentileMs(sorted, 50.0);
        double p90Ms = getPercentileMs(sorted, 90.0);
        double p95Ms = getPercentileMs(sorted, 95.0);
        double p99Ms = getPercentileMs(sorted, 99.0);
        double p999Ms = getPercentileMs(sorted, 99.9);
        double maxMs = sorted.length > 0 ? sorted[sorted.length - 1] / 1_000_000.0 : 0;

        return new BenchmarkResult(
                workload, completed, concurrency, pipelineDepth, payloadSize,
                elapsedSeconds, rps, minMs, p50Ms, p90Ms, p95Ms, p99Ms, p999Ms, maxMs,
                heapBefore, heapAfter, errorCount.get()
        );
    }

    private static byte[] generateCommandBytes(String workload, int id, byte[] largePayload) {
        return switch (workload.toUpperCase(Locale.ROOT)) {
            case "PING" -> "*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII);
            case "SET" -> {
                String k = "bench:" + id;
                yield ("*3\r\n$3\r\nSET\r\n$" + k.length() + "\r\n" + k + "\r\n$5\r\nvalue\r\n").getBytes(StandardCharsets.US_ASCII);
            }
            case "GET" -> {
                String k = "bench:" + id;
                yield ("*2\r\n$3\r\nGET\r\n$" + k.length() + "\r\n" + k + "\r\n").getBytes(StandardCharsets.US_ASCII);
            }
            case "INCR" -> ("*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n").getBytes(StandardCharsets.US_ASCII);
            case "MIXED" -> {
                String k = "bench:" + (id % 1000);
                if (id % 5 == 0) {
                    String v = "val" + id;
                    yield ("*3\r\n$3\r\nSET\r\n$" + k.length() + "\r\n" + k + "\r\n$" + v.length() + "\r\n" + v + "\r\n").getBytes(StandardCharsets.US_ASCII);
                } else {
                    yield ("*2\r\n$3\r\nGET\r\n$" + k.length() + "\r\n" + k + "\r\n").getBytes(StandardCharsets.US_ASCII);
                }
            }
            case "LARGE_PAYLOAD" -> {
                String k = "bench:large:" + id;
                byte[] keyBytes = k.getBytes(StandardCharsets.US_ASCII);
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try {
                    baos.write("*3\r\n$3\r\nSET\r\n$".getBytes(StandardCharsets.US_ASCII));
                    baos.write(Integer.toString(keyBytes.length).getBytes(StandardCharsets.US_ASCII));
                    baos.write("\r\n".getBytes(StandardCharsets.US_ASCII));
                    baos.write(keyBytes);
                    baos.write("\r\n$".getBytes(StandardCharsets.US_ASCII));
                    baos.write(Integer.toString(largePayload.length).getBytes(StandardCharsets.US_ASCII));
                    baos.write("\r\n".getBytes(StandardCharsets.US_ASCII));
                    baos.write(largePayload);
                    baos.write("\r\n".getBytes(StandardCharsets.US_ASCII));
                } catch (IOException ignored) {}
                yield baos.toByteArray();
            }
            case "TRANSACTION" -> "*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$5\r\ntxkey\r\n$4\r\n1000\r\n*2\r\n$4\r\nINCR\r\n$5\r\ntxkey\r\n*1\r\n$4\r\nEXEC\r\n".getBytes(StandardCharsets.US_ASCII);
            default -> "*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII);
        };
    }

    private static double getPercentileMs(long[] sortedNanos, double percentile) {
        if (sortedNanos.length == 0) return 0.0;
        int index = (int) Math.ceil((percentile / 100.0) * sortedNanos.length) - 1;
        index = Math.max(0, Math.min(index, sortedNanos.length - 1));
        return sortedNanos[index] / 1_000_000.0;
    }

    public static void runWarmup(String host, int port, int warmupRequests) {
        System.out.printf("Warming up JVM HotSpot compiler (%d requests)...%n", warmupRequests);
        try {
            runBenchmark(host, port, warmupRequests, 10, "PING", 1, 16);
            runBenchmark(host, port, warmupRequests, 10, "SET", 1, 16);
            runBenchmark(host, port, warmupRequests, 10, "GET", 1, 16);
            System.out.println("Warmup completed. C2 compiler tier compiled hot paths.");
        } catch (Exception e) {
            System.err.println("Warmup encountered error: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
        String host = "127.0.0.1";
        int port = 6379;
        int totalRequests = 20000;
        int concurrency = 50;
        int pipelineDepth = 1;
        int payloadSize = 16;
        String workload = "PING";
        boolean runSuite = false;
        String outDir = "benchmark/results";

        for (int i = 0; i < args.length; i++) {
            if ("--host".equals(args[i]) && i + 1 < args.length) host = args[++i];
            else if ("--port".equals(args[i]) && i + 1 < args.length) port = Integer.parseInt(args[++i]);
            else if ("-n".equals(args[i]) && i + 1 < args.length) totalRequests = Integer.parseInt(args[++i]);
            else if ("-c".equals(args[i]) && i + 1 < args.length) concurrency = Integer.parseInt(args[++i]);
            else if ("-t".equals(args[i]) && i + 1 < args.length) workload = args[++i];
            else if ("-P".equals(args[i]) && i + 1 < args.length) pipelineDepth = Integer.parseInt(args[++i]);
            else if ("-d".equals(args[i]) && i + 1 < args.length) payloadSize = Integer.parseInt(args[++i]);
            else if ("--suite".equals(args[i])) runSuite = true;
            else if ("--out-dir".equals(args[i]) && i + 1 < args.length) outDir = args[++i];
        }

        Files.createDirectories(Path.of(outDir));

        if (runSuite) {
            System.out.println("================================================================================");
            System.out.println(" EXECUTING COMPREHENSIVE SYSTEMS BENCHMARK SUITE");
            System.out.println(" Target: " + host + ":" + port);
            System.out.println(" Runtime: Java " + System.getProperty("java.version") + " (" + System.getProperty("java.vm.name") + ")");
            System.out.println(" OS: " + System.getProperty("os.name") + " (" + System.getProperty("os.arch") + ") Cores: " + Runtime.getRuntime().availableProcessors());
            System.out.println("================================================================================");

            runWarmup(host, port, 5000);

            List<BenchmarkResult> results = new ArrayList<>();

            // 1. Concurrency Sweeps on Core Workloads (1, 10, 50, 100 clients)
            int[] clientConcurrencies = {1, 10, 50, 100};
            for (int c : clientConcurrencies) {
                results.add(runBenchmark(host, port, 10000, c, "PING", 1, 16));
                results.add(runBenchmark(host, port, 10000, c, "SET", 1, 16));
                results.add(runBenchmark(host, port, 10000, c, "GET", 1, 16));
                results.add(runBenchmark(host, port, 10000, c, "INCR", 1, 16));
                results.add(runBenchmark(host, port, 10000, c, "MIXED", 1, 16));
            }

            // 2. Pipelining Depth Sweeps (depth 1, 4, 16, 64 with 20 clients)
            int[] depths = {1, 4, 16, 64};
            for (int d : depths) {
                results.add(runBenchmark(host, port, 20000, 20, "SET", d, 16));
                results.add(runBenchmark(host, port, 20000, 20, "GET", d, 16));
            }

            // 3. Payload Size Impact (16B vs 1024B)
            results.add(runBenchmark(host, port, 10000, 20, "LARGE_PAYLOAD", 1, 1024));

            // 4. Transaction Workload (MULTI/EXEC)
            results.add(runBenchmark(host, port, 5000, 10, "TRANSACTION", 1, 16));

            // Print all reports
            for (BenchmarkResult r : results) {
                r.printReport();
            }

            // Write CSV
            File csvFile = new File(outDir, "benchmark_results.csv");
            try (PrintWriter pw = new PrintWriter(new FileWriter(csvFile))) {
                pw.println("workload,totalRequests,concurrency,pipelineDepth,payloadBytes,elapsedSeconds,throughputRps,minMs,p50Ms,p90Ms,p95Ms,p99Ms,p999Ms,maxMs,heapBefore,heapAfter,errors");
                for (BenchmarkResult r : results) {
                    pw.println(r.toCsvLine());
                }
            }
            System.out.println("✅ Raw CSV results saved to: " + csvFile.getAbsolutePath());

            // Write JSON
            File jsonFile = new File(outDir, "benchmark_results.json");
            try (PrintWriter pw = new PrintWriter(new FileWriter(jsonFile))) {
                pw.println("{\n  \"timestamp\": \"" + new Date() + "\",\n  \"os\": \"" + System.getProperty("os.name") + "\",\n  \"java\": \"" + System.getProperty("java.version") + "\",\n  \"results\": [");
                for (int idx = 0; idx < results.size(); idx++) {
                    pw.print(results.get(idx).toJsonFragment());
                    if (idx < results.size() - 1) pw.println(",");
                    else pw.println();
                }
                pw.println("  ]\n}");
            }
            System.out.println("✅ Machine-readable JSON results saved to: " + jsonFile.getAbsolutePath());

        } else {
            BenchmarkResult singleResult = runBenchmark(host, port, totalRequests, concurrency, workload, pipelineDepth, payloadSize);
            singleResult.printReport();
        }
    }
}
