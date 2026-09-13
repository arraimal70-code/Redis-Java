package com.redisclone.persistence;

import com.redisclone.storage.DataStore;
import com.redisclone.storage.RedisObject;
import com.redisclone.storage.RedisType;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Binary point-in-time snapshotting engine (RDB).
 * Serializes the complete database memory state into a compact binary format.
 * Features atomic file replacement (temp file -> rename) to avoid file corruption
 * during unexpected server terminations.
 */
public class RdbManager {

    private static final Logger LOGGER = Logger.getLogger(RdbManager.class.getName());

    private static final byte[] RDB_MAGIC = "REDIS0009".getBytes(StandardCharsets.US_ASCII);

    // RDB Opcodes
    private static final byte OP_EXPIRE_TIME_MS = (byte) 0xFC;
    private static final byte OP_SELECT_DB = (byte) 0xFE;
    private static final byte OP_EOF = (byte) 0xFF;

    // Type Identifiers
    private static final byte TYPE_STRING = 0x00;
    private static final byte TYPE_LIST = 0x01;
    private static final byte TYPE_HASH = 0x04;

    private final File rdbFile;
    private final ExecutorService bgSaveExecutor;

    public RdbManager(String filePath) {
        this.rdbFile = new File(filePath);
        this.bgSaveExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "redis-rdb-bgsave");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Synchronous snapshot save (SAVE command).
     */
    public synchronized boolean save(DataStore dataStore) {
        File tempFile = new File(rdbFile.getAbsolutePath() + ".tmp");
        if (rdbFile.getParentFile() != null) {
            rdbFile.getParentFile().mkdirs();
        }

        long startTime = System.currentTimeMillis();
        try {
            try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tempFile)))) {
                // 1. Magic Header
                dos.write(RDB_MAGIC);

                // 2. Select DB 0
                dos.writeByte(OP_SELECT_DB);
                dos.writeByte(0);

                Map<String, RedisObject> snapshot = dataStore.getRawDbSnapshot();
                Map<String, Long> expires = dataStore.getRawExpiresSnapshot();
                long now = System.currentTimeMillis();

                // 3. Serialize Keys
                for (Map.Entry<String, RedisObject> entry : snapshot.entrySet()) {
                    String key = entry.getKey();
                    RedisObject obj = entry.getValue();

                    // Skip already expired keys
                    Long expireTime = expires.get(key);
                    if (expireTime != null && now >= expireTime) {
                        continue;
                    }

                    // Write Expiry if present
                    if (expireTime != null) {
                        dos.writeByte(OP_EXPIRE_TIME_MS);
                        dos.writeLong(expireTime);
                    }

                    // Write Type & Key
                    switch (obj.getType()) {
                        case STRING -> {
                            dos.writeByte(TYPE_STRING);
                            writeString(dos, key);
                            byte[] data = obj.asStringBytes();
                            dos.writeInt(data.length);
                            dos.write(data);
                        }
                        case LIST -> {
                            dos.writeByte(TYPE_LIST);
                            writeString(dos, key);
                            Deque<byte[]> list = obj.asList();
                            dos.writeInt(list.size());
                            for (byte[] item : list) {
                                dos.writeInt(item.length);
                                dos.write(item);
                            }
                        }
                        case HASH -> {
                            dos.writeByte(TYPE_HASH);
                            writeString(dos, key);
                            Map<String, byte[]> hash = obj.asHash();
                            dos.writeInt(hash.size());
                            for (Map.Entry<String, byte[]> hashEntry : hash.entrySet()) {
                                writeString(dos, hashEntry.getKey());
                                byte[] valBytes = hashEntry.getValue();
                                dos.writeInt(valBytes.length);
                                dos.write(valBytes);
                            }
                        }
                    }
                }

                // 4. EOF & Checksum
                dos.writeByte(OP_EOF);
                dos.writeLong(0L); // 8-byte checksum placeholder
                dos.flush();
            }

            // Rename temp file to permanent snapshot with retry for Windows / sync client resilience
            boolean moved = false;
            for (int i = 0; i < 5; i++) {
                try {
                    Files.move(tempFile.toPath(), rdbFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                    moved = true;
                    break;
                } catch (IOException e) {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException ignored) {}
                }
            }

            if (!moved) {
                Files.copy(tempFile.toPath(), rdbFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                tempFile.delete();
            }

            LOGGER.info("DB saved on disk in " + (System.currentTimeMillis() - startTime) + "ms. File size: " + rdbFile.length() + " bytes.");
            return true;
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to persist RDB snapshot", e);
            if (tempFile.exists()) tempFile.delete();
            return false;
        }
    }

    /**
     * Asynchronous background snapshot (BGSAVE command).
     */
    public void bgSave(DataStore dataStore) {
        bgSaveExecutor.submit(() -> {
            LOGGER.info("Background saving started by pid thread " + Thread.currentThread().getName());
            boolean success = save(dataStore);
            if (success) {
                LOGGER.info("Background saving terminated with success.");
            } else {
                LOGGER.warning("Background saving failed!");
            }
        });
    }

    /**
     * Loads RDB snapshot file into the dataStore upon server startup.
     */
    public int load(DataStore dataStore) {
        if (!rdbFile.exists() || rdbFile.length() == 0) {
            return 0;
        }

        LOGGER.info("Loading RDB snapshot from " + rdbFile.getAbsolutePath() + "...");
        int loadedKeys = 0;

        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(rdbFile)))) {
            // Verify Magic
            byte[] magic = new byte[RDB_MAGIC.length];
            dis.readFully(magic);
            if (!java.util.Arrays.equals(magic, RDB_MAGIC)) {
                throw new IOException("Invalid RDB magic header: " + new String(magic, StandardCharsets.US_ASCII));
            }

            Long nextExpiry = null;

            while (true) {
                int opcode = dis.read();
                if (opcode == -1 || opcode == (OP_EOF & 0xFF)) {
                    break;
                }

                if (opcode == (OP_SELECT_DB & 0xFF)) {
                    dis.readByte(); // db number
                    continue;
                }

                if (opcode == (OP_EXPIRE_TIME_MS & 0xFF)) {
                    nextExpiry = dis.readLong();
                    opcode = dis.read(); // Followed immediately by value type opcode
                }

                String key = readString(dis);

                switch (opcode) {
                    case TYPE_STRING -> {
                        int len = dis.readInt();
                        byte[] val = new byte[len];
                        dis.readFully(val);
                        dataStore.restoreKey(key, RedisObject.ofString(val), nextExpiry);
                        loadedKeys++;
                    }
                    case TYPE_LIST -> {
                        int count = dis.readInt();
                        RedisObject listObj = RedisObject.ofList();
                        Deque<byte[]> list = listObj.asList();
                        for (int i = 0; i < count; i++) {
                            int itemLen = dis.readInt();
                            byte[] item = new byte[itemLen];
                            dis.readFully(item);
                            list.addLast(item);
                        }
                        dataStore.restoreKey(key, listObj, nextExpiry);
                        loadedKeys++;
                    }
                    case TYPE_HASH -> {
                        int count = dis.readInt();
                        RedisObject hashObj = RedisObject.ofHash();
                        Map<String, byte[]> hash = hashObj.asHash();
                        for (int i = 0; i < count; i++) {
                            String field = readString(dis);
                            int valLen = dis.readInt();
                            byte[] val = new byte[valLen];
                            dis.readFully(val);
                            hash.put(field, val);
                        }
                        dataStore.restoreKey(key, hashObj, nextExpiry);
                        loadedKeys++;
                    }
                    default -> throw new IOException("Unknown RDB value type: " + opcode);
                }

                nextExpiry = null;
            }
            LOGGER.info("RDB load completed. Restored " + loadedKeys + " keys.");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to load RDB file", e);
        }

        return loadedKeys;
    }

    private void writeString(DataOutputStream dos, String str) throws IOException {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        dos.writeInt(bytes.length);
        dos.write(bytes);
    }

    private String readString(DataInputStream dis) throws IOException {
        int length = dis.readInt();
        byte[] bytes = new byte[length];
        dis.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public File getRdbFile() {
        return rdbFile;
    }
}
