package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.RingBufferHandler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Упрощенный сервис для управления PostgreSQL checkpoint'ами
 * Работает только с offset и handler без промежуточных классов
 */
@Slf4j
public class PostgresCheckpointService {

    private static final int CHECKPOINT_SIZE = 64; // bytes
    private static final int MAGIC_NUMBER = 0x3F760124;
//    PGCP0001;
    private static final int VERSION = 1;

    private final Path baseCheckpointDirectory;
    private final ConcurrentHashMap<Integer, PostgresShardCheckpoint> shardCheckpoints = new ConcurrentHashMap<>();

    public PostgresCheckpointService(Path baseCheckpointDirectory) throws IOException {
        this.baseCheckpointDirectory = baseCheckpointDirectory;
        Files.createDirectories(baseCheckpointDirectory);

        log.info("PostgreSQL Checkpoint Service initialized: directory={}", baseCheckpointDirectory);
    }

    /**
     * Обновить checkpoint после успешной обработки batch
     * Принимает только handler и offset
     */
    public void updateCheckpoint(
        int shardId,
        RingBufferHandler handler,
        long batchOffset
    ) {
        try {
            // Получить или создать checkpoint для шарда
            PostgresShardCheckpoint checkpoint = shardCheckpoints.computeIfAbsent(
                shardId,
                id -> new PostgresShardCheckpoint(id)
            );

            // Извлекаем данные напрямую из handler
            long entriesCount = handler.batchElementsCount(batchOffset);
            long batchSize = handler.batchSize(batchOffset);
            long processedAt = System.currentTimeMillis();

            // Обновить статистику
            checkpoint.updateFromBatch(entriesCount, batchSize, processedAt);

            // Записать в файл
            writeCheckpointToFile(shardId, checkpoint);

            log.debug("Updated PostgreSQL checkpoint: shard={}, batches={}, entries={}",
                shardId, checkpoint.getTotalBatches(), checkpoint.getTotalEntries());

        } catch (Exception e) {
            log.error("Error updating PostgreSQL checkpoint: shard={}", shardId, e);
            // НЕ выбрасываем исключение - checkpoint не должен ломать основной процесс
        }
    }

    /**
     * Восстановить checkpoint состояние при старте
     */
    public PostgresShardCheckpoint recoverCheckpoint(int shardId) {
        try {
            Path checkpointFile = getCheckpointFilePath(shardId);

            if (Files.exists(checkpointFile)) {
                PostgresShardCheckpoint checkpoint = readCheckpointFromFile(shardId, checkpointFile);
                if (checkpoint != null) {
                    shardCheckpoints.put(shardId, checkpoint);
                    log.info("Recovered PostgreSQL checkpoint: shard={}, batches={}, entries={}",
                        shardId, checkpoint.getTotalBatches(), checkpoint.getTotalEntries());
                    return checkpoint;
                }
            }

            // Создать новый checkpoint если файл не существует или поврежден
            PostgresShardCheckpoint newCheckpoint = new PostgresShardCheckpoint(shardId);
            shardCheckpoints.put(shardId, newCheckpoint);

            log.info("Created new PostgreSQL checkpoint: shard={}", shardId);
            return newCheckpoint;

        } catch (Exception e) {
            log.error("Error recovering PostgreSQL checkpoint: shard={}", shardId, e);

            // Fallback - создать новый checkpoint
            PostgresShardCheckpoint fallbackCheckpoint = new PostgresShardCheckpoint(shardId);
            shardCheckpoints.put(shardId, fallbackCheckpoint);
            return fallbackCheckpoint;
        }
    }

    /**
     * Получить текущий checkpoint для шарда (для мониторинга)
     */
    public PostgresShardCheckpoint getCheckpoint(int shardId) {
        return shardCheckpoints.get(shardId);
    }

    /**
     * Принудительно сохранить все checkpoint'ы
     */
    public void flushAllCheckpoints() {
        log.info("Flushing all PostgreSQL checkpoints");

        for (PostgresShardCheckpoint checkpoint : shardCheckpoints.values()) {
            try {
                writeCheckpointToFile(checkpoint.getShardId(), checkpoint);
            } catch (Exception e) {
                log.error("Error flushing checkpoint for shard {}", checkpoint.getShardId(), e);
            }
        }

        log.info("All PostgreSQL checkpoints flushed");
    }

    // ===== INTERNAL METHODS =====

    private void writeCheckpointToFile(int shardId, PostgresShardCheckpoint checkpoint) throws IOException {
        Path checkpointFile = getCheckpointFilePath(shardId);

        try (FileChannel channel = FileChannel.open(checkpointFile,
            StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {

            ByteBuffer buffer = ByteBuffer.allocate(CHECKPOINT_SIZE);

            // Write checkpoint data
            buffer.putInt(MAGIC_NUMBER);
            buffer.putInt(VERSION);
            buffer.putLong(checkpoint.getLastBatchTimestamp());
            buffer.putLong(checkpoint.getTotalBatches());
            buffer.putLong(checkpoint.getTotalEntries());
            buffer.putLong(checkpoint.getTotalBytes());
            buffer.putLong(System.currentTimeMillis()); // Updated at

            // Calculate simple checksum (XOR of all longs)
            buffer.flip();
            long checksum = 0;
            for (int i = 8; i < buffer.remaining() - 8; i += 8) { // Skip magic+version and checksum itself
                checksum ^= buffer.getLong(i);
            }
            buffer.position(buffer.limit() - 8);
            buffer.putLong(checksum);

            buffer.flip();
            channel.write(buffer);
            channel.force(true);
        }
    }

    private PostgresShardCheckpoint readCheckpointFromFile(int shardId, Path checkpointFile) throws IOException {
        try (FileChannel channel = FileChannel.open(checkpointFile, StandardOpenOption.READ)) {
            ByteBuffer buffer = ByteBuffer.allocate(CHECKPOINT_SIZE);

            if (channel.read(buffer) != CHECKPOINT_SIZE) {
                log.warn("Invalid checkpoint file size: shard={}", shardId);
                return null;
            }

            buffer.flip();

            // Validate header
            int magic = buffer.getInt();
            int version = buffer.getInt();

            if (magic != MAGIC_NUMBER || version != VERSION) {
                log.warn("Invalid checkpoint file format: shard={}, magic={}, version={}",
                    shardId, Integer.toHexString(magic), version);
                return null;
            }

            // Read data
            long lastBatchTimestamp = buffer.getLong();
            long totalBatches = buffer.getLong();
            long totalEntries = buffer.getLong();
            long totalBytes = buffer.getLong();
            long updatedAt = buffer.getLong();
            long expectedChecksum = buffer.getLong();

            // Validate checksum
            long actualChecksum = lastBatchTimestamp ^ totalBatches ^ totalEntries ^ totalBytes ^ updatedAt;

            if (actualChecksum != expectedChecksum) {
                log.warn("Checkpoint checksum mismatch: shard={}, expected={}, actual={}",
                    shardId, expectedChecksum, actualChecksum);
                return null;
            }

            // Create checkpoint object
            PostgresShardCheckpoint checkpoint = new PostgresShardCheckpoint(shardId);
            checkpoint.setLastBatchTimestamp(lastBatchTimestamp);
            checkpoint.setTotalBatches(totalBatches);
            checkpoint.setTotalEntries(totalEntries);
            checkpoint.setTotalBytes(totalBytes);

            return checkpoint;
        }
    }

    private Path getCheckpointFilePath(int shardId) {
        return baseCheckpointDirectory.resolve(String.format("postgres_checkpoint_shard_%02d.dat", shardId));
    }

    // ===== CHECKPOINT DATA CLASS =====

    public static class PostgresShardCheckpoint {

        private final int shardId;
        private final AtomicLong totalBatches = new AtomicLong(0);
        private final AtomicLong totalEntries = new AtomicLong(0);
        private final AtomicLong totalBytes = new AtomicLong(0);
        private volatile long lastBatchTimestamp = 0;

        public PostgresShardCheckpoint(int shardId) {
            this.shardId = shardId;
        }

        public void updateFromBatch(long entriesCount, long batchSize, long processedAt) {
            totalBatches.incrementAndGet();
            totalEntries.addAndGet(entriesCount);
            totalBytes.addAndGet(batchSize);
            lastBatchTimestamp = processedAt;
        }

        // Getters
        public int getShardId() {
            return shardId;
        }

        public long getTotalBatches() {
            return totalBatches.get();
        }

        public long getTotalEntries() {
            return totalEntries.get();
        }

        public long getTotalBytes() {
            return totalBytes.get();
        }

        public long getLastBatchTimestamp() {
            return lastBatchTimestamp;
        }

        // Setters (for recovery)
        public void setTotalBatches(long value) {
            totalBatches.set(value);
        }

        public void setTotalEntries(long value) {
            totalEntries.set(value);
        }

        public void setTotalBytes(long value) {
            totalBytes.set(value);
        }

        public void setLastBatchTimestamp(long value) {
            lastBatchTimestamp = value;
        }

        public String getDiagnostics() {
            return String.format("PostgresCheckpoint[shard=%d, batches=%d, entries=%d, bytes=%dMB, last_batch=%d]",
                shardId, getTotalBatches(), getTotalEntries(),
                getTotalBytes() / (1024 * 1024), lastBatchTimestamp);
        }
    }
}