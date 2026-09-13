package com.redisclone.persistence;

import com.redisclone.command.CommandRegistry;
import com.redisclone.resp.RespEncoder;
import com.redisclone.resp.RespFrame;
import com.redisclone.resp.RespParser;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Append-Only File (AOF) persistence manager.
 * Serializes all state-mutating commands in standard RESP format to disk.
 * Supports configurable fsync policies: ALWAYS, EVERYSEC, and NO.
 */
public class AofManager {

    private static final Logger LOGGER = Logger.getLogger(AofManager.class.getName());

    public enum FsyncPolicy {
        ALWAYS,
        EVERYSEC,
        NO
    }

    private final File aofFile;
    private final FsyncPolicy fsyncPolicy;
    private FileOutputStream fos;
    private FileChannel fileChannel;
    private ScheduledExecutorService fsyncScheduler;
    private volatile boolean enabled;

    public AofManager(String filePath, FsyncPolicy fsyncPolicy, boolean enabled) {
        this.aofFile = new File(filePath);
        this.fsyncPolicy = fsyncPolicy;
        this.enabled = enabled;
    }

    public synchronized void start() throws IOException {
        if (!enabled) return;

        if (aofFile.getParentFile() != null) {
            aofFile.getParentFile().mkdirs();
        }

        this.fos = new FileOutputStream(aofFile, true);
        this.fileChannel = fos.getChannel();

        if (fsyncPolicy == FsyncPolicy.EVERYSEC) {
            this.fsyncScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "redis-aof-fsync-thread");
                t.setDaemon(true);
                return t;
            });
            fsyncScheduler.scheduleAtFixedRate(this::syncToDisk, 1, 1, TimeUnit.SECONDS);
        }

        LOGGER.info("AOF persistence initialized: " + aofFile.getAbsolutePath() + " [fsync: " + fsyncPolicy + "]");
    }

    /**
     * Appends a mutating command (represented as a RESP Array frame) to the AOF log.
     */
    public synchronized void append(RespFrame commandFrame) {
        if (!enabled || fileChannel == null || !fileChannel.isOpen()) {
            return;
        }

        try {
            byte[] encoded = RespEncoder.encode(commandFrame);
            fos.write(encoded);

            if (fsyncPolicy == FsyncPolicy.ALWAYS) {
                fileChannel.force(false);
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to append command to AOF", e);
        }
    }

    public synchronized void syncToDisk() {
        if (fileChannel != null && fileChannel.isOpen()) {
            try {
                fos.flush();
                fileChannel.force(false);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to fsync AOF to disk", e);
            }
        }
    }

    /**
     * Replays the AOF log during server startup to rebuild the in-memory data store.
     */
    public int replay(CommandRegistry commandRegistry) {
        if (!aofFile.exists() || aofFile.length() == 0) {
            return 0;
        }

        LOGGER.info("Replaying AOF log from " + aofFile.getAbsolutePath() + " (" + aofFile.length() + " bytes)...");
        int commandCount = 0;

        try (FileInputStream fis = new FileInputStream(aofFile)) {
            byte[] buffer = new byte[(int) aofFile.length()];
            int bytesRead = fis.read(buffer);
            if (bytesRead <= 0) return 0;

            ByteBuffer byteBuf = ByteBuffer.wrap(buffer, 0, bytesRead);
            RespFrame frame;
            while ((frame = RespParser.parse(byteBuf)) != null) {
                if (frame instanceof RespFrame.Array arrayFrame) {
                    // Dispatch command without re-appending to AOF or replying to client
                    commandRegistry.executeInternal(arrayFrame);
                    commandCount++;
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "AOF replay error", e);
        }

        LOGGER.info("AOF replay finished. Executed " + commandCount + " commands successfully.");
        return commandCount;
    }

    public synchronized void close() {
        if (fsyncScheduler != null) {
            fsyncScheduler.shutdown();
        }
        syncToDisk();
        try {
            if (fileChannel != null) fileChannel.close();
            if (fos != null) fos.close();
        } catch (IOException ignored) {}
    }

    public boolean isEnabled() {
        return enabled;
    }

    public File getAofFile() {
        return aofFile;
    }
}
