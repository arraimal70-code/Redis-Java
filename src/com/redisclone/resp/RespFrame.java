package com.redisclone.resp;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable algebraic data representation of a RESP Frame.
 * Utilizes Java 21 sealed hierarchy for pattern matching and type safety.
 */
public sealed interface RespFrame {

    RespType type();

    record SimpleString(String value) implements RespFrame {
        public SimpleString {
            Objects.requireNonNull(value, "value cannot be null");
        }
        @Override
        public RespType type() { return RespType.SIMPLE_STRING; }
    }

    record Error(String message) implements RespFrame {
        public Error {
            Objects.requireNonNull(message, "message cannot be null");
        }
        @Override
        public RespType type() { return RespType.ERROR; }
    }

    record Integer(long value) implements RespFrame {
        @Override
        public RespType type() { return RespType.INTEGER; }
    }

    record BulkString(byte[] data) implements RespFrame {
        // null data represents RESP Null Bulk String ($-1\r\n)
        public boolean isNull() {
            return data == null;
        }

        public String asUtf8String() {
            return data == null ? null : new String(data, StandardCharsets.UTF_8);
        }

        @Override
        public RespType type() { return RespType.BULK_STRING; }
    }

    record Array(List<RespFrame> elements) implements RespFrame {
        public Array {
            if (elements != null) {
                elements = List.copyOf(elements);
            }
        }

        public boolean isNull() {
            return elements == null;
        }

        @Override
        public RespType type() { return RespType.ARRAY; }
    }

    record Null() implements RespFrame {
        @Override
        public RespType type() { return RespType.NULL; }
    }

    // Static factories
    static SimpleString ofSimpleString(String s) {
        return new SimpleString(s);
    }

    static Error ofError(String message) {
        return new Error(message);
    }

    static Integer ofInteger(long val) {
        return new Integer(val);
    }

    static BulkString ofBulkString(byte[] data) {
        return new BulkString(data);
    }

    static BulkString ofBulkString(String str) {
        if (str == null) return ofNullBulkString();
        return new BulkString(str.getBytes(StandardCharsets.UTF_8));
    }

    static BulkString ofNullBulkString() {
        return new BulkString(null);
    }

    static Array ofArray(List<RespFrame> elements) {
        return new Array(elements);
    }

    static Array ofNullArray() {
        return new Array(null);
    }

    static Null ofNull() {
        return new Null();
    }
}
