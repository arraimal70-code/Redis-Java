package com.redisclone.storage;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * HyperLogLog Probabilistic Cardinality Estimator.
 * Implements the Flajolet et al. algorithm using m = 64 registers (6-bit index).
 *
 * Provides O(1) time complexity and fixed 64-byte memory footprint per set,
 * with standard error ~ 1.04 / sqrt(64) ≈ 13%.
 */
public class HyperLogLog {

    public static final int REGISTER_COUNT = 64;
    private static final int BITS_FOR_INDEX = 6; // 2^6 = 64 registers
    private static final double ALPHA = 0.709;   // Flajolet alpha constant for m = 64

    private final byte[] registers;

    public HyperLogLog() {
        this.registers = new byte[REGISTER_COUNT];
    }

    public HyperLogLog(byte[] existingRegisters) {
        if (existingRegisters.length >= REGISTER_COUNT) {
            this.registers = Arrays.copyOf(existingRegisters, REGISTER_COUNT);
        } else {
            this.registers = new byte[REGISTER_COUNT];
            System.arraycopy(existingRegisters, 0, this.registers, 0, existingRegisters.length);
        }
    }

    /**
     * Adds an element into the HyperLogLog register array.
     * @return true if at least one register was updated (cardinality changed), false otherwise.
     */
    public boolean add(byte[] element) {
        long hash = hash64(element);
        int index = (int) (hash & (REGISTER_COUNT - 1));
        long w = hash >>> BITS_FOR_INDEX;
        byte leadingZeros = (byte) (Long.numberOfLeadingZeros(w) - BITS_FOR_INDEX + 1);
        if (leadingZeros <= 0) leadingZeros = 1;

        if (leadingZeros > registers[index]) {
            registers[index] = leadingZeros;
            return true;
        }
        return false;
    }

    /**
     * Estimates the distinct count of elements added to this HLL.
     */
    public long estimate() {
        double sum = 0.0;
        int zeroCount = 0;
        for (int i = 0; i < REGISTER_COUNT; i++) {
            sum += 1.0 / (1L << registers[i]);
            if (registers[i] == 0) {
                zeroCount++;
            }
        }

        double rawEstimate = ALPHA * REGISTER_COUNT * REGISTER_COUNT / sum;

        // Linear counting for small cardinalities (zero registers present)
        if (rawEstimate <= 2.5 * REGISTER_COUNT && zeroCount > 0) {
            return Math.round(REGISTER_COUNT * Math.log((double) REGISTER_COUNT / zeroCount));
        }

        return Math.round(rawEstimate);
    }

    /**
     * Merges another HLL into this one (taking pairwise register maximums).
     */
    public void merge(HyperLogLog other) {
        for (int i = 0; i < REGISTER_COUNT; i++) {
            if (other.registers[i] > this.registers[i]) {
                this.registers[i] = other.registers[i];
            }
        }
    }

    public byte[] getBytes() {
        return Arrays.copyOf(registers, REGISTER_COUNT);
    }

    /**
     * 64-bit Murmur/FNV-style avalanche hash function.
     */
    private static long hash64(byte[] data) {
        long h = 0xcbf29ce484222325L;
        for (byte b : data) {
            h ^= (b & 0xff);
            h *= 0x100000001b3L;
        }
        // Bit mixing
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;
        return h;
    }
}
