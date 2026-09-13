package com.redisclone.command;

import com.redisclone.resp.RespFrame;

import java.util.List;

/**
 * Standard Redis command execution interface.
 */
public interface Command {

    /**
     * Executes the command logic.
     *
     * @param ctx  Execution context containing subsystem references
     * @param args Argument bytes (excluding the command name itself)
     * @return The RESP frame response to reply to the client
     */
    RespFrame execute(CommandContext ctx, List<byte[]> args);

    /**
     * Flags whether this command mutates state and must be logged to the AOF file.
     */
    default boolean isWriteCommand() {
        return false;
    }
}
