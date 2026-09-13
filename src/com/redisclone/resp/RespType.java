package com.redisclone.resp;

/**
 * RESP (Redis Serialization Protocol) marker definitions.
 * Represents prefix byte identifiers according to the RESP2 / RESP3 specification.
 */
public enum RespType {
    SIMPLE_STRING((byte) '+'),
    ERROR((byte) '-'),
    INTEGER((byte) ':'),
    BULK_STRING((byte) '$'),
    ARRAY((byte) '*'),
    
    // RESP3 extensions
    NULL((byte) '_'),
    BOOLEAN((byte) '#'),
    DOUBLE((byte) ','),
    MAP((byte) '%'),
    SET((byte) '~');

    private final byte prefix;

    RespType(byte prefix) {
        this.prefix = prefix;
    }

    public byte getPrefix() {
        return prefix;
    }

    public static RespType fromPrefix(byte b) {
        return switch (b) {
            case '+' -> SIMPLE_STRING;
            case '-' -> ERROR;
            case ':' -> INTEGER;
            case '$' -> BULK_STRING;
            case '*' -> ARRAY;
            case '_' -> NULL;
            case '#' -> BOOLEAN;
            case ',' -> DOUBLE;
            case '%' -> MAP;
            case '~' -> SET;
            default -> throw new IllegalArgumentException("Unknown RESP marker: " + (char) b + " (" + b + ")");
        };
    }
}
