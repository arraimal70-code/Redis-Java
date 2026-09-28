package com.redisclone.benchmark;

import com.redisclone.cluster.Crc16;
import com.redisclone.resp.RespParser;
import com.redisclone.storage.DataStore;
import com.redisclone.storage.RedisObject;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * Component-Level Microbenchmark Suite.
 * Measures low-level computational overhead of hot-path internal mechanisms:
 * 1. Zero-Allocation ASCII Numeric Parsing vs Standard Library {@link Long#parseLong(String)}
 * 2. CRC16-CCITT Hash Slot Computation across varying key lengths and {hash_tag} patterns
 * 3. In-Memory Key-Value Lookup & LRU Access Timestamp Tracking
 */
public class MicrobenchmarkSuite {

    private static final int WARMUP_ITERATIONS = 50_000;
    private static final int MEASURE_ITERATIONS = 200_000;

    public static void main(String[] args) {
        System.out.println("================================================================================");
        System.out.println(" COMPONENT-LEVEL MICROBENCHMARK SUITE");
        System.out.println(" Runtime: Java " + System.getProperty("java.version") + " (" + System.getProperty("java.vm.name") + ")");
        System.out.println(" OS: " + System.getProperty("os.name") + " (" + System.getProperty("os.arch") + ")");
        System.out.println("================================================================================\n");

        benchmarkNumericParsing();
        benchmarkCrc16HashSlot();
        benchmarkDataStoreMemoryAccess();

        System.out.println("================================================================================");
        System.out.println(" ✅ All internal microbenchmarks completed successfully.");
        System.out.println("================================================================================");
    }

    // --- 1. Zero-Allocation ASCII Numeric Parsing vs JDK Allocation ---
    private static void benchmarkNumericParsing() {
        System.out.println("--- 1. RESP Numeric Parsing: Zero-Allocation vs String Allocation ---");

        byte[] sampleAscii = "1234567890".getBytes(StandardCharsets.US_ASCII);

        // Warmup JIT C2 compiler
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            long v1 = parseAsciiDirect(sampleAscii, 0, sampleAscii.length);
            long v2 = Long.parseLong(new String(sampleAscii, 0, sampleAscii.length, StandardCharsets.US_ASCII));
            if (v1 != v2) throw new AssertionError("Mismatch in numeric parsing");
        }

        // Measure Direct Zero-Allocation Parsing
        long startDirect = System.nanoTime();
        long sumDirect = 0;
        for (int i = 0; i < MEASURE_ITERATIONS; i++) {
            sumDirect += parseAsciiDirect(sampleAscii, 0, sampleAscii.length);
        }
        long durationDirectNanos = System.nanoTime() - startDirect;
        double directNsPerOp = (double) durationDirectNanos / MEASURE_ITERATIONS;
        double directOpsPerSec = (MEASURE_ITERATIONS / (durationDirectNanos / 1_000_000_000.0));

        // Measure Standard Library String.parseLong
        long startJdk = System.nanoTime();
        long sumJdk = 0;
        for (int i = 0; i < MEASURE_ITERATIONS; i++) {
            sumJdk += Long.parseLong(new String(sampleAscii, 0, sampleAscii.length, StandardCharsets.US_ASCII));
        }
        long durationJdkNanos = System.nanoTime() - startJdk;
        double jdkNsPerOp = (double) durationJdkNanos / MEASURE_ITERATIONS;
        double jdkOpsPerSec = (MEASURE_ITERATIONS / (durationJdkNanos / 1_000_000_000.0));

        System.out.printf(Locale.ROOT, "  [Zero-Allocation]: %,.1f ns/op | %,.0f ops/sec (Sum: %d)%n",
                directNsPerOp, directOpsPerSec, sumDirect);
        System.out.printf(Locale.ROOT, "  [JDK Allocation]:  %,.1f ns/op | %,.0f ops/sec (Sum: %d)%n",
                jdkNsPerOp, jdkOpsPerSec, sumJdk);
        System.out.printf(Locale.ROOT, "  🚀 Speedup:        %.2fx faster without intermediate heap allocations%n%n",
                jdkNsPerOp / directNsPerOp);
    }

    private static long parseAsciiDirect(byte[] bytes, int offset, int length) {
        long result = 0;
        for (int i = 0; i < length; i++) {
            byte b = bytes[offset + i];
            result = result * 10 + (b - '0');
        }
        return result;
    }

    // --- 2. CRC16 Hash Slot Calculation ---
    private static void benchmarkCrc16HashSlot() {
        System.out.println("--- 2. CRC16-CCITT Cluster Hash Slot Calculation ---");

        String standardKey = "user:session:1004829";
        String taggedKey = "{tenant:alpha}:user:profile:settings";

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            Crc16.getSlot(standardKey);
            Crc16.getSlot(taggedKey);
        }

        long start = System.nanoTime();
        int slotSum = 0;
        for (int i = 0; i < MEASURE_ITERATIONS; i++) {
            slotSum += Crc16.getSlot(standardKey);
            slotSum += Crc16.getSlot(taggedKey);
        }
        long durationNanos = System.nanoTime() - start;
        int totalOps = MEASURE_ITERATIONS * 2;
        double nsPerOp = (double) durationNanos / totalOps;
        double opsPerSec = totalOps / (durationNanos / 1_000_000_000.0);

        System.out.printf(Locale.ROOT, "  Operations:       %,d keys hashed%n", totalOps);
        System.out.printf(Locale.ROOT, "  Latency:          %,.2f ns/op%n", nsPerOp);
        System.out.printf(Locale.ROOT, "  Throughput:       %,.0f slots calculated/sec%n%n", opsPerSec);
    }

    // --- 3. In-Memory Store & LRU Access Overhead ---
    private static void benchmarkDataStoreMemoryAccess() {
        System.out.println("--- 3. In-Memory DataStore Read/Write & LRU Access Overhead ---");

        DataStore ds = new DataStore();
        int keyCount = 10_000;
        for (int i = 0; i < keyCount; i++) {
            ds.set("key:" + i, RedisObject.ofString("payload_" + i), null);
        }

        // Warmup reads
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            ds.get("key:" + (i % keyCount));
        }

        // Measure Reads
        long startRead = System.nanoTime();
        long hitCount = 0;
        for (int i = 0; i < MEASURE_ITERATIONS; i++) {
            RedisObject obj = ds.get("key:" + (i % keyCount));
            if (obj != null) hitCount++;
        }
        long durationReadNanos = System.nanoTime() - startRead;
        double readNsPerOp = (double) durationReadNanos / MEASURE_ITERATIONS;
        double readOpsPerSec = MEASURE_ITERATIONS / (durationReadNanos / 1_000_000_000.0);

        System.out.printf(Locale.ROOT, "  [DataStore GET]:  %,.2f ns/op | %,.0f reads/sec (Hits: %d)%n",
                readNsPerOp, readOpsPerSec, hitCount);

        // Measure Writes
        long startWrite = System.nanoTime();
        for (int i = 0; i < MEASURE_ITERATIONS; i++) {
            ds.set("bench:key:" + (i % 1000), RedisObject.ofString("v"), null);
        }
        long durationWriteNanos = System.nanoTime() - startWrite;
        double writeNsPerOp = (double) durationWriteNanos / MEASURE_ITERATIONS;
        double writeOpsPerSec = MEASURE_ITERATIONS / (durationWriteNanos / 1_000_000_000.0);

        System.out.printf(Locale.ROOT, "  [DataStore SET]:  %,.2f ns/op | %,.0f writes/sec%n%n",
                writeNsPerOp, writeOpsPerSec);
    }
}
