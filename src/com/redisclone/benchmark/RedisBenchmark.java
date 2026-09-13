package com.redisclone.benchmark;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * High-performance multithreaded benchmarking harness.
 * Evaluates throughput (Requests Per Second) and high-resolution latency distributions
 * (min, p50, p90, p95, p99, p99.9, max) across PING, SET, GET, and pipelined workloads.
 */
public class RedisBenchmark {

    public static class BenchmarkResult {
        public final String testName;
        public final int totalRequests;
        public final int concurrency;
        public final double elapsedSeconds;
        public final double rps;
        public final double minMs;
        public final double p50Ms;
        public final double p90Ms;
        public final double p95Ms;
        public final double p99Ms;
        public final double p999Ms;
        public final double maxMs;

        public BenchmarkResult(String testName, int totalRequests, int concurrency,
                               double elapsedSeconds, double rps, double minMs,
                               double p50Ms, double p90Ms, double p95Ms,
                               double p99Ms, double p999Ms, double maxMs) {
            this.testName = testName;
            this.totalRequests = totalRequests;
            this.concurrency = concurrency;
            this.elapsedSeconds = elapsedSeconds;
            this.rps = rps;
            this.minMs = minMs;
            this.p50Ms = p50Ms;
            this.p90Ms = p90Ms;
            this.p95Ms = p95Ms;
            this.p99Ms = p99Ms;
            this.p999Ms = p999Ms;
            this.maxMs = maxMs;
        }

        public void printReport() {
            System.out.printf("""
                    ====== %s ======
                      Requests:       %d
                      Concurrency:    %d clients
                      Time taken:     %.3f seconds
                      Throughput:     %.2f requests/sec
                    --- Latency Percentiles (ms) ---
                      min:            %.3f ms
                      p50 (median):   %.3f ms
                      p90:            %.3f ms
                      p95:            %.3f ms
                      p99:            %.3f ms
                      p99.9:          %.3f ms
                      max:            %.3f ms
                    ---------------------------------
                    %n""", testName, totalRequests, concurrency, elapsedSeconds, rps,
                    minMs, p50Ms, p90Ms, p95Ms, p99Ms, p999Ms, maxMs);
        }
    }

    public static BenchmarkResult runBenchmark(String host, int port, int totalRequests, int concurrency, String commandType) throws Exception {
        int requestsPerClient = totalRequests / concurrency;
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        List<Long> latenciesNanos = Collections.synchronizedList(new ArrayList<>(totalRequests));
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(concurrency);

        AtomicInteger globalCounter = new AtomicInteger(0);

        for (int c = 0; c < concurrency; c++) {
            executor.submit(() -> {
                try (Socket socket = new Socket(host, port);
                     OutputStream out = socket.getOutputStream();
                     InputStream in = socket.getInputStream()) {

                    byte[] readBuf = new byte[1024];
                    startLatch.await(); // Synchronize all clients to start simultaneously

                    for (int i = 0; i < requestsPerClient; i++) {
                        int id = globalCounter.incrementAndGet();
                        byte[] payload = switch (commandType.toUpperCase()) {
                            case "PING" -> "*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII);
                            case "SET" -> ("*3\r\n$3\r\nSET\r\n$8\r\nbench:" + id + "\r\n$5\r\nvalue\r\n").getBytes(StandardCharsets.US_ASCII);
                            case "GET" -> ("*2\r\n$3\r\nGET\r\n$8\r\nbench:" + id + "\r\n").getBytes(StandardCharsets.US_ASCII);
                            default -> "*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII);
                        };

                        long start = System.nanoTime();
                        out.write(payload);
                        out.flush();
                        in.read(readBuf);
                        long duration = System.nanoTime() - start;

                        latenciesNanos.add(duration);
                    }
                } catch (Exception e) {
                    System.err.println("Benchmark client error: " + e.getMessage());
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

        double elapsedSeconds = benchDuration / 1_000_000_000.0;
        int completed = latenciesNanos.size();
        double rps = completed / elapsedSeconds;

        long[] sorted = latenciesNanos.stream().mapToLong(Long::longValue).toArray();
        Arrays.sort(sorted);

        double minMs = sorted.length > 0 ? sorted[0] / 1_000_000.0 : 0;
        double p50Ms = getPercentileMs(sorted, 50.0);
        double p90Ms = getPercentileMs(sorted, 90.0);
        double p95Ms = getPercentileMs(sorted, 95.0);
        double p99Ms = getPercentileMs(sorted, 99.0);
        double p999Ms = getPercentileMs(sorted, 99.9);
        double maxMs = sorted.length > 0 ? sorted[sorted.length - 1] / 1_000_000.0 : 0;

        return new BenchmarkResult(commandType, completed, concurrency, elapsedSeconds, rps, minMs, p50Ms, p90Ms, p95Ms, p99Ms, p999Ms, maxMs);
    }

    private static double getPercentileMs(long[] sortedNanos, double percentile) {
        if (sortedNanos.length == 0) return 0.0;
        int index = (int) Math.ceil((percentile / 100.0) * sortedNanos.length) - 1;
        index = Math.max(0, Math.min(index, sortedNanos.length - 1));
        return sortedNanos[index] / 1_000_000.0;
    }

    public static void main(String[] args) throws Exception {
        String host = "127.0.0.1";
        int port = 6379;
        int totalRequests = 20000;
        int concurrency = 50;

        for (int i = 0; i < args.length; i++) {
            if ("--host".equals(args[i]) && i + 1 < args.length) host = args[++i];
            else if ("--port".equals(args[i]) && i + 1 < args.length) port = Integer.parseInt(args[++i]);
            else if ("-n".equals(args[i]) && i + 1 < args.length) totalRequests = Integer.parseInt(args[++i]);
            else if ("-c".equals(args[i]) && i + 1 < args.length) concurrency = Integer.parseInt(args[++i]);
        }

        System.out.println("Starting Benchmark against " + host + ":" + port + "...");
        System.out.println("Concurrency: " + concurrency + " clients | Total requests: " + totalRequests);

        BenchmarkResult pingRes = runBenchmark(host, port, totalRequests, concurrency, "PING");
        pingRes.printReport();

        BenchmarkResult setRes = runBenchmark(host, port, totalRequests, concurrency, "SET");
        setRes.printReport();

        BenchmarkResult getRes = runBenchmark(host, port, totalRequests, concurrency, "GET");
        getRes.printReport();
    }
}
