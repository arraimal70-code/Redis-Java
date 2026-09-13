package com.redisclone.resp;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * High-performance streaming state-machine parser for the Redis Serialization Protocol (RESP).
 * Designed for non-blocking Java NIO channels with zero-copy buffer slicing and automatic
 * TCP fragmentation recovery.
 *
 * Mechanical Sympathy:
 * - Uses ByteBuffer mark/reset so incomplete frames leave the buffer unmodified for subsequent socket reads.
 * - Parses integers directly from ASCII byte streams without String allocations.
 * - Fully handles pipelined frames and nested arrays.
 */
public class RespParser {

    private static final byte CR = '\r';
    private static final byte LF = '\n';

    /**
     * Attempts to parse one complete RespFrame from the provided ByteBuffer.
     * If insufficient bytes are present (TCP fragmentation), the buffer's position
     * is restored to its starting point and null is returned.
     *
     * @param buffer ByteBuffer containing incoming socket data in read-mode (flipped)
     * @return Fully parsed RespFrame, or null if more bytes are needed
     */
    public static RespFrame parse(ByteBuffer buffer) {
        if (!buffer.hasRemaining()) {
            return null;
        }

        int startPos = buffer.position();
        byte marker = buffer.get();

        RespFrame frame = switch (marker) {
            case '+' -> parseSimpleString(buffer);
            case '-' -> parseError(buffer);
            case ':' -> parseInteger(buffer);
            case '$' -> parseBulkString(buffer);
            case '*' -> parseArray(buffer);
            case '_' -> parseNull(buffer);
            default -> {
                buffer.position(startPos);
                yield parseInlineCommand(buffer);
            }
        };

        if (frame == null) {
            buffer.position(startPos);
        }
        return frame;
    }

    private static RespFrame.SimpleString parseSimpleString(ByteBuffer buffer) {
        byte[] line = readLine(buffer);
        if (line == null) return null;
        return new RespFrame.SimpleString(new String(line, StandardCharsets.UTF_8));
    }

    private static RespFrame.Error parseError(ByteBuffer buffer) {
        byte[] line = readLine(buffer);
        if (line == null) return null;
        return new RespFrame.Error(new String(line, StandardCharsets.UTF_8));
    }

    private static RespFrame.Integer parseInteger(ByteBuffer buffer) {
        byte[] line = readLine(buffer);
        if (line == null) return null;
        try {
            long val = parseAsciiLong(line, 0, line.length);
            return new RespFrame.Integer(val);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static RespFrame.BulkString parseBulkString(ByteBuffer buffer) {
        byte[] lengthLine = readLine(buffer);
        if (lengthLine == null) return null;

        int length;
        try {
            length = (int) parseAsciiLong(lengthLine, 0, lengthLine.length);
        } catch (NumberFormatException e) {
            return null;
        }

        if (length == -1) {
            return RespFrame.ofNullBulkString();
        }
        if (length < -1) {
            return null;
        }

        // Must have length bytes + 2 bytes for CRLF
        if (buffer.remaining() < length + 2) {
            return null;
        }

        byte[] payload = new byte[length];
        buffer.get(payload);

        // Verify trailing CRLF
        byte cr = buffer.get();
        byte lf = buffer.get();
        if (cr != CR || lf != LF) {
            return null;
        }

        return new RespFrame.BulkString(payload);
    }

    private static RespFrame.Array parseArray(ByteBuffer buffer) {
        byte[] countLine = readLine(buffer);
        if (countLine == null) return null;

        int count;
        try {
            count = (int) parseAsciiLong(countLine, 0, countLine.length);
        } catch (NumberFormatException e) {
            return null;
        }

        if (count == -1) {
            return RespFrame.ofNullArray();
        }
        if (count < -1) {
            return null;
        }

        List<RespFrame> elements = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            RespFrame element = parse(buffer);
            if (element == null) {
                return null; // Incomplete array frame
            }
            elements.add(element);
        }

        return new RespFrame.Array(elements);
    }

    private static RespFrame.Null parseNull(ByteBuffer buffer) {
        byte[] line = readLine(buffer);
        if (line == null) return null;
        return new RespFrame.Null();
    }

    /**
     * Parses legacy inline commands (e.g. "PING\r\n" or "SET foo bar\r\n" via netcat/telnet)
     * into a RESP Array of Bulk Strings.
     */
    private static RespFrame parseInlineCommand(ByteBuffer buffer) {
        byte[] line = readLine(buffer);
        if (line == null) return null;

        String lineStr = new String(line, StandardCharsets.UTF_8).trim();
        if (lineStr.isEmpty()) return null;

        String[] parts = lineStr.split("\\s+");
        List<RespFrame> elements = new ArrayList<>(parts.length);
        for (String part : parts) {
            elements.add(RespFrame.ofBulkString(part));
        }
        return RespFrame.ofArray(elements);
    }

    /**
     * Scans for CRLF delimiter and returns bytes preceding it without modifying
     * buffer position if CRLF is missing.
     */
    private static byte[] readLine(ByteBuffer buffer) {
        int startPos = buffer.position();
        int limit = buffer.limit();

        int crIndex = -1;
        for (int i = startPos; i < limit - 1; i++) {
            if (buffer.get(i) == CR && buffer.get(i + 1) == LF) {
                crIndex = i;
                break;
            }
        }

        if (crIndex == -1) {
            return null; // Incomplete line
        }

        int length = crIndex - startPos;
        byte[] line = new byte[length];
        buffer.get(line);
        buffer.get(); // skip CR
        buffer.get(); // skip LF
        return line;
    }

    /**
     * Fast zero-allocation ASCII byte[] to long parser.
     */
    public static long parseAsciiLong(byte[] bytes, int offset, int length) {
        if (bytes == null || length == 0) {
            throw new NumberFormatException("Empty byte slice");
        }
        int i = offset;
        boolean negative = false;
        if (bytes[i] == '-') {
            negative = true;
            i++;
            if (length == 1) throw new NumberFormatException("Invalid number: -");
        } else if (bytes[i] == '+') {
            i++;
        }

        long result = 0;
        int end = offset + length;
        while (i < end) {
            byte b = bytes[i++];
            if (b < '0' || b > '9') {
                throw new NumberFormatException("Invalid numeric byte: " + (char) b);
            }
            result = result * 10 + (b - '0');
        }
        return negative ? -result : result;
    }
}
