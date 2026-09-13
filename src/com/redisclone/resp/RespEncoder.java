package com.redisclone.resp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * High-performance serializer for RESP frames into byte buffers.
 * Includes pre-allocated byte constants for hot-path responses (+OK, $-1, :0, :1, etc.)
 * to minimize GC pressure during high-throughput workloads.
 */
public class RespEncoder {

    public static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.US_ASCII);

    // Pre-allocated common RESP responses (zero GC overhead in hot paths)
    public static final byte[] OK = "+OK\r\n".getBytes(StandardCharsets.US_ASCII);
    public static final byte[] PONG = "+PONG\r\n".getBytes(StandardCharsets.US_ASCII);
    public static final byte[] NULL_BULK = "$-1\r\n".getBytes(StandardCharsets.US_ASCII);
    public static final byte[] NULL_ARRAY = "*-1\r\n".getBytes(StandardCharsets.US_ASCII);
    public static final byte[] EMPTY_ARRAY = "*0\r\n".getBytes(StandardCharsets.US_ASCII);
    public static final byte[] ZERO_INT = ":0\r\n".getBytes(StandardCharsets.US_ASCII);
    public static final byte[] ONE_INT = ":1\r\n".getBytes(StandardCharsets.US_ASCII);

    public static byte[] encode(RespFrame frame) {
        if (frame instanceof RespFrame.SimpleString s) {
            return encodeSimpleString(s.value());
        } else if (frame instanceof RespFrame.Error e) {
            return encodeError(e.message());
        } else if (frame instanceof RespFrame.Integer i) {
            return encodeInteger(i.value());
        } else if (frame instanceof RespFrame.BulkString b) {
            return encodeBulkString(b.data());
        } else if (frame instanceof RespFrame.Array a) {
            return encodeArray(a.elements());
        } else if (frame instanceof RespFrame.Null) {
            return "_\r\n".getBytes(StandardCharsets.US_ASCII);
        }
        throw new IllegalArgumentException("Unsupported frame type: " + frame);
    }

    public static byte[] encodeSimpleString(String s) {
        if ("OK".equals(s)) return OK;
        if ("PONG".equals(s)) return PONG;
        byte[] strBytes = s.getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[1 + strBytes.length + 2];
        out[0] = '+';
        System.arraycopy(strBytes, 0, out, 1, strBytes.length);
        out[out.length - 2] = '\r';
        out[out.length - 1] = '\n';
        return out;
    }

    public static byte[] encodeError(String message) {
        byte[] msgBytes = message.getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[1 + msgBytes.length + 2];
        out[0] = '-';
        System.arraycopy(msgBytes, 0, out, 1, msgBytes.length);
        out[out.length - 2] = '\r';
        out[out.length - 1] = '\n';
        return out;
    }

    public static byte[] encodeInteger(long val) {
        if (val == 0) return ZERO_INT;
        if (val == 1) return ONE_INT;
        byte[] numBytes = Long.toString(val).getBytes(StandardCharsets.US_ASCII);
        byte[] out = new byte[1 + numBytes.length + 2];
        out[0] = ':';
        System.arraycopy(numBytes, 0, out, 1, numBytes.length);
        out[out.length - 2] = '\r';
        out[out.length - 1] = '\n';
        return out;
    }

    public static byte[] encodeBulkString(byte[] data) {
        if (data == null) {
            return NULL_BULK;
        }
        byte[] lenBytes = Integer.toString(data.length).getBytes(StandardCharsets.US_ASCII);
        int totalLen = 1 + lenBytes.length + 2 + data.length + 2;
        byte[] out = new byte[totalLen];
        int pos = 0;
        out[pos++] = '$';
        System.arraycopy(lenBytes, 0, out, pos, lenBytes.length);
        pos += lenBytes.length;
        out[pos++] = '\r';
        out[pos++] = '\n';
        System.arraycopy(data, 0, out, pos, data.length);
        pos += data.length;
        out[pos++] = '\r';
        out[pos++] = '\n';
        return out;
    }

    public static byte[] encodeBulkString(String str) {
        if (str == null) return NULL_BULK;
        return encodeBulkString(str.getBytes(StandardCharsets.UTF_8));
    }

    public static byte[] encodeArray(java.util.List<RespFrame> elements) {
        if (elements == null) {
            return NULL_ARRAY;
        }
        if (elements.isEmpty()) {
            return EMPTY_ARRAY;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            baos.write('*');
            baos.write(Integer.toString(elements.size()).getBytes(StandardCharsets.US_ASCII));
            baos.write(CRLF);
            for (RespFrame element : elements) {
                baos.write(encode(element));
            }
        } catch (IOException e) {
            throw new RuntimeException("ByteArrayOutputStream write failed", e);
        }
        return baos.toByteArray();
    }

    public static ByteBuffer toByteBuffer(RespFrame frame) {
        return ByteBuffer.wrap(encode(frame));
    }
}
