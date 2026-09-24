package com.redisclone.examples;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

/**
 * Real-World Reference Application: High-Performance AI Inference & Prompt Cache Gateway.
 *
 * Problem Statement:
 * Deep Learning & Large Language Model (LLM) inference carries severe computational cost,
 * high GPU latency (typically 150-400ms per inference), and monetary expense. In production
 * conversational AI and search engines, up to 40-70% of user queries share identical or
 * semantically normalized intent ("What is binary search?", "what is binary search", "WHAT IS BINARY SEARCH?").
 *
 * Solution:
 * This reference gateway sits between client requests and an expensive AI inference engine.
 * 1. Incoming user prompts undergo deterministic normalization (whitespace compaction, lowercase, punctuation strip).
 * 2. A cryptographic SHA-256 fingerprint produces a localized Redis key: "ai:prompt:<hash>".
 * 3. Cache Hit: Retrieved from Java 21 Redis clone in < 1ms.
 * 4. Cache Miss: Evaluates inference engine (mocked at 180ms latency without external paid APIs),
 *    caches the generated response in Redis with an expiration TTL (EX 3600), and returns the result.
 */
public class AiInferenceCache {

    private final String host;
    private final int port;
    private final int ttlSeconds;

    public AiInferenceCache(String host, int port, int ttlSeconds) {
        this.host = host;
        this.port = port;
        this.ttlSeconds = ttlSeconds;
    }

    /**
     * Deterministically normalizes input text and hashes to a hex key.
     */
    public String computePromptHash(String rawPrompt) {
        String normalized = rawPrompt.trim().toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "");
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(normalized.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : hash) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (Exception e) {
            return Integer.toHexString(normalized.hashCode());
        }
    }

    /**
     * Executes AI inference request through Redis-backed semantic cache.
     */
    public InferenceResult processPrompt(Socket socket, String prompt) throws Exception {
        OutputStream out = socket.getOutputStream();
        InputStream in = socket.getInputStream();

        String promptHash = computePromptHash(prompt);
        String redisKey = "ai:prompt:" + promptHash;

        long t0 = System.nanoTime();

        // 1. Cache Lookup via RESP GET
        String getCommand = "*2\r\n$3\r\nGET\r\n$" + redisKey.length() + "\r\n" + redisKey + "\r\n";
        out.write(getCommand.getBytes(StandardCharsets.US_ASCII));
        out.flush();

        String redisReply = readRespString(in);

        if (redisReply != null) {
            // Cache Hit: sub-millisecond retrieval
            long durationNs = System.nanoTime() - t0;
            return new InferenceResult(prompt, redisReply, true, durationNs / 1_000_000.0);
        }

        // Cache Miss: Simulate LLM inference computation (150ms - 220ms)
        long modelStart = System.currentTimeMillis();
        String generatedResponse = simulateExpensiveLlmInference(prompt);
        long modelDurationMs = System.currentTimeMillis() - modelStart;

        // Store result in Redis with TTL
        byte[] valBytes = generatedResponse.getBytes(StandardCharsets.UTF_8);
        String setCommand = "*5\r\n$3\r\nSET\r\n$" + redisKey.length() + "\r\n" + redisKey +
                "\r\n$" + valBytes.length + "\r\n" + generatedResponse +
                "\r\n$2\r\nEX\r\n$" + Integer.toString(ttlSeconds).length() + "\r\n" + ttlSeconds + "\r\n";
        out.write(setCommand.getBytes(StandardCharsets.US_ASCII));
        out.flush();
        readRespString(in); // Consume +OK

        long totalDurationNs = System.nanoTime() - t0;
        return new InferenceResult(prompt, generatedResponse, false, totalDurationNs / 1_000_000.0);
    }

    private String simulateExpensiveLlmInference(String prompt) {
        try {
            // Realistic simulated LLM model matrix multiplication & token generation latency
            Thread.sleep(180);
        } catch (InterruptedException ignored) {}
        return "[Model Response]: Detailed technical answer regarding '" + prompt.trim() + "'.";
    }

    private String readRespString(InputStream in) throws Exception {
        int b = in.read();
        if (b == -1) return null;

        if (b == '$') {
            // Read length line
            StringBuilder lenSb = new StringBuilder();
            int c;
            while ((c = in.read()) != '\r') {
                lenSb.append((char) c);
            }
            in.read(); // consume \n
            int length = Integer.parseInt(lenSb.toString());
            if (length == -1) return null; // Null bulk string (cache miss)

            byte[] data = new byte[length];
            int readTotal = 0;
            while (readTotal < length) {
                int r = in.read(data, readTotal, length - readTotal);
                if (r == -1) break;
                readTotal += r;
            }
            in.read(); // skip trailing \r
            in.read(); // skip trailing \n
            return new String(data, StandardCharsets.UTF_8);
        } else if (b == '+') {
            // Simple string
            StringBuilder sb = new StringBuilder();
            int c;
            while ((c = in.read()) != '\r') {
                sb.append((char) c);
            }
            in.read(); // consume \n
            return sb.toString();
        }
        return null;
    }

    public record InferenceResult(String prompt, String response, boolean cacheHit, double latencyMs) {}

    public static void main(String[] args) throws Exception {
        String host = "127.0.0.1";
        int port = 6379;
        int numRequests = 60;

        for (int i = 0; i < args.length; i++) {
            if ("--host".equals(args[i]) && i + 1 < args.length) host = args[++i];
            else if ("--port".equals(args[i]) && i + 1 < args.length) port = Integer.parseInt(args[++i]);
            else if ("--requests".equals(args[i]) && i + 1 < args.length) numRequests = Integer.parseInt(args[++i]);
        }

        System.out.println("================================================================================");
        System.out.println(" AI INFERENCE CACHE GATEWAY (Redis-Backed Semantic Prompt Cache)");
        System.out.println(" Target Database: " + host + ":" + port);
        System.out.println(" Simulated Inference Cost: ~180 ms per cache miss | Cache Hit: <1 ms");
        System.out.println("================================================================================");

        AiInferenceCache gateway = new AiInferenceCache(host, port, 3600);

        // Typical production prompt distribution: 10 queries repeated with varying formatting
        List<String> promptPool = List.of(
                "How does Quicksort partitioning work?",
                "how does quicksort partitioning work",
                "HOW DOES QUICKSORT PARTITIONING WORK?",
                "Explain Java 21 Virtual Threads",
                "explain java 21 virtual threads",
                "What is Redis Serialization Protocol RESP?",
                "What is Redis Serialization Protocol RESP?",
                "Compare Java NIO Selector vs Epoll",
                "compare java nio selector vs epoll",
                "How does AOF persistence guarantee durability?"
        );

        int hits = 0;
        int misses = 0;
        List<Double> hitLatencies = new ArrayList<>();
        List<Double> missLatencies = new ArrayList<>();

        try (Socket socket = new Socket(host, port)) {
            socket.setTcpNoDelay(true);

            System.out.printf("Executing %d realistic client AI prompt queries...%n", numRequests);

            for (int i = 0; i < numRequests; i++) {
                String prompt = promptPool.get(i % promptPool.size());
                InferenceResult result = gateway.processPrompt(socket, prompt);

                if (result.cacheHit()) {
                    hits++;
                    hitLatencies.add(result.latencyMs());
                } else {
                    misses++;
                    missLatencies.add(result.latencyMs());
                }

                if (i < 10 || i % 10 == 0) {
                    System.out.printf("  [Query %02d] %-10s | Latency: %7.2f ms | Prompt: \"%s\"%n",
                            i + 1, result.cacheHit() ? "CACHE HIT" : "CACHE MISS",
                            result.latencyMs(), prompt);
                }
            }
        }

        double avgHit = hitLatencies.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        double avgMiss = missLatencies.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        double hitRatio = (double) hits / numRequests;
        double latencySpeedup = avgHit > 0 ? avgMiss / avgHit : 0.0;
        double savedComputeSeconds = (misses > 0 ? hits * (avgMiss / 1000.0) : 0.0);

        System.out.println("\n================================================================================");
        System.out.println(" EXPERIMENTAL RESULTS: AI INFERENCE CACHE GATEWAY");
        System.out.println("================================================================================");
        System.out.printf("  Total Queries Processed:     %d%n", numRequests);
        System.out.printf("  Cache Hits:                  %d (%.1f%%)%n", hits, hitRatio * 100.0);
        System.out.printf("  Cache Misses:                %d (%.1f%%)%n", misses, (1.0 - hitRatio) * 100.0);
        System.out.printf("  Average Cache Miss Latency:  %.2f ms (Simulated Model Compute)%n", avgMiss);
        System.out.printf("  Average Cache Hit Latency:   %.2f ms (Redis In-Memory Lookup)%n", avgHit);
        System.out.printf("  Observed Latency Speedup:    %.1fx faster on cache hit%n", latencySpeedup);
        System.out.printf("  Total GPU Time Saved:        %.2f seconds of compute%n", savedComputeSeconds);
        System.out.println("================================================================================");
    }
}
