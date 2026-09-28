package com.redisclone.storage;

import java.util.Collections;
import java.util.List;

/**
 * Immutable entry inside an append-only Redis Stream.
 * Each entry has an ID formatted as "<millisecondsTimestamp>-<sequenceNumber>",
 * along with an ordered list of field-value byte pairs.
 */
public record StreamEntry(
        String id,
        long timestampMs,
        long sequence,
        List<byte[]> fields
) {
    public StreamEntry {
        fields = Collections.unmodifiableList(fields);
    }

    public static StreamEntry parse(String id, List<byte[]> fields) {
        String[] parts = id.split("-");
        long ts = Long.parseLong(parts[0]);
        long seq = parts.length > 1 ? Long.parseLong(parts[1]) : 0L;
        return new StreamEntry(id, ts, seq, fields);
    }
}
