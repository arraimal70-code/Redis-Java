package com.redisclone.persistence;

import com.redisclone.command.CommandRegistry;
import com.redisclone.resp.RespEncoder;
import com.redisclone.resp.RespFrame;
import com.redisclone.resp.RespParser;

import com.redisclone.storage.DataStore;
import com.redisclone.storage.RedisObject;
import com.redisclone.storage.SortedSet;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
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

    /**
     * Compacts the current in-memory DataStore into a brand new AOF file
     * and atomically replaces the existing appendonly.aof file (BGREWRITEAOF).
     */
    public synchronized boolean rewrite(DataStore dataStore) {
        if (!enabled) return false;

        File tempFile = new File(aofFile.getAbsolutePath() + ".rewrite");
        try {
            if (tempFile.getParentFile() != null) {
                tempFile.getParentFile().mkdirs();
            }

            try (FileOutputStream tempFos = new FileOutputStream(tempFile)) {
                Map<String, RedisObject> snapshot = dataStore.getRawDbSnapshot();
                Map<String, Long> expires = dataStore.getRawExpiresSnapshot();
                long now = System.currentTimeMillis();

                for (Map.Entry<String, RedisObject> entry : snapshot.entrySet()) {
                    String key = entry.getKey();
                    RedisObject obj = entry.getValue();
                    Long expireAt = expires.get(key);

                    // Skip expired keys
                    if (expireAt != null && expireAt <= now) {
                        continue;
                    }

                    byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

                    switch (obj.getType()) {
                        case STRING -> {
                            byte[] val = obj.asStringBytes();
                            RespFrame frame;
                            if (expireAt != null) {
                                long ttlMs = expireAt - now;
                                frame = RespFrame.ofArray(List.of(
                                        RespFrame.ofBulkString("SET".getBytes(StandardCharsets.US_ASCII)),
                                        RespFrame.ofBulkString(keyBytes),
                                        RespFrame.ofBulkString(val),
                                        RespFrame.ofBulkString("PX".getBytes(StandardCharsets.US_ASCII)),
                                        RespFrame.ofBulkString(Long.toString(ttlMs).getBytes(StandardCharsets.US_ASCII))
                                ));
                            } else {
                                frame = RespFrame.ofArray(List.of(
                                        RespFrame.ofBulkString("SET".getBytes(StandardCharsets.US_ASCII)),
                                        RespFrame.ofBulkString(keyBytes),
                                        RespFrame.ofBulkString(val)
                                ));
                            }
                            tempFos.write(RespEncoder.encode(frame));
                        }
                        case HASH -> {
                            Map<String, byte[]> hash = obj.asHash();
                            if (!hash.isEmpty()) {
                                List<RespFrame> elements = new ArrayList<>();
                                elements.add(RespFrame.ofBulkString("HSET".getBytes(StandardCharsets.US_ASCII)));
                                elements.add(RespFrame.ofBulkString(keyBytes));
                                for (Map.Entry<String, byte[]> hEntry : hash.entrySet()) {
                                    elements.add(RespFrame.ofBulkString(hEntry.getKey().getBytes(StandardCharsets.UTF_8)));
                                    elements.add(RespFrame.ofBulkString(hEntry.getValue()));
                                }
                                tempFos.write(RespEncoder.encode(RespFrame.ofArray(elements)));
                            }
                        }
                        case LIST -> {
                            Deque<byte[]> list = obj.asList();
                            if (!list.isEmpty()) {
                                List<RespFrame> elements = new ArrayList<>();
                                elements.add(RespFrame.ofBulkString("RPUSH".getBytes(StandardCharsets.US_ASCII)));
                                elements.add(RespFrame.ofBulkString(keyBytes));
                                for (byte[] item : list) {
                                    elements.add(RespFrame.ofBulkString(item));
                                }
                                tempFos.write(RespEncoder.encode(RespFrame.ofArray(elements)));
                            }
                        }
                        case ZSET -> {
                            SortedSet zset = obj.asZSet();
                            Map<String, Double> dict = zset.getDictSnapshot();
                            if (!dict.isEmpty()) {
                                List<RespFrame> elements = new ArrayList<>();
                                elements.add(RespFrame.ofBulkString("ZADD".getBytes(StandardCharsets.US_ASCII)));
                                elements.add(RespFrame.ofBulkString(keyBytes));
                                for (Map.Entry<String, Double> zEntry : dict.entrySet()) {
                                    double score = zEntry.getValue();
                                    String scoreStr = (score == (long) score) ? String.valueOf((long) score) : String.valueOf(score);
                                    elements.add(RespFrame.ofBulkString(scoreStr.getBytes(StandardCharsets.US_ASCII)));
                                    elements.add(RespFrame.ofBulkString(zEntry.getKey().getBytes(StandardCharsets.UTF_8)));
                                }
                                tempFos.write(RespEncoder.encode(RespFrame.ofArray(elements)));
                            }
                        }
                    }
                }
                tempFos.flush();
                tempFos.getChannel().force(false);
            }

            // Close existing file channel and stream before replacing file
            if (fileChannel != null) fileChannel.close();
            if (fos != null) fos.close();

            // Atomic file swap
            try {
                java.nio.file.Files.move(
                        tempFile.toPath(),
                        aofFile.toPath(),
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE
                );
            } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                java.nio.file.Files.move(
                        tempFile.toPath(),
                        aofFile.toPath(),
                        StandardCopyOption.REPLACE_EXISTING
                );
            }

            // Reopen in append mode
            this.fos = new FileOutputStream(aofFile, true);
            this.fileChannel = fos.getChannel();

            LOGGER.info("AOF rewrite completed successfully. Compacted file size: " + aofFile.length() + " bytes");
            return true;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to rewrite AOF log", e);
            if (tempFile.exists()) tempFile.delete();
            try {
                if (fos == null || fileChannel == null || !fileChannel.isOpen()) {
                    this.fos = new FileOutputStream(aofFile, true);
                    this.fileChannel = fos.getChannel();
                }
            } catch (IOException ignored) {}
            return false;
        }
    }

    /**
     * Executes AOF compaction asynchronously in a background daemon thread.
     */
    public void bgrewrite(DataStore dataStore) {
        Thread t = new Thread(() -> rewrite(dataStore), "redis-bgrewriteaof-thread");
        t.setDaemon(true);
        t.start();
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
