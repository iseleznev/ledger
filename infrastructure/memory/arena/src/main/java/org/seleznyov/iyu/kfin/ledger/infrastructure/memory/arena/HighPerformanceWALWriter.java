package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import com.sun.nio.file.ExtendedOpenOption;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicLong;

/**
 * High-performance WAL writer с immediate writes для финтех операций
 * Использует Direct I/O для predictable latency
 */
@Slf4j
public class HighPerformanceWALWriter {

    // ===== CONFIGURATION =====

    private static final int WRITE_BUFFER_SIZE = 64 * 1024; // 64KB aligned buffer
    private static final int ALIGNMENT_SIZE = 4096; // 4KB для SSD optimization
    private static final long MAX_FILE_SIZE = 1024 * 1024 * 1024L; // 1GB per file
    private static final String FILE_PREFIX = "wal_shard_";

    // ===== FILE MANAGEMENT =====

    private final Path walDirectory;
    private final int shardId;
    private final AtomicLong currentFileSequence = new AtomicLong(1);
    private final AtomicLong currentFileOffset = new AtomicLong(0);

    // ===== I/O RESOURCES =====

    private FileChannel currentChannel;
    private final ByteBuffer writeBuffer;
    private volatile boolean isOpen = true;

    // ===== METRICS =====

    private final AtomicLong totalBytesWritten = new AtomicLong(0);
    private final AtomicLong totalWrites = new AtomicLong(0);
    private final AtomicLong totalFlushes = new AtomicLong(0);

    public HighPerformanceWALWriter(Path walDirectory, int shardId) throws IOException {
        this.walDirectory = walDirectory;
        this.shardId = shardId;

        // Create aligned buffer для Direct I/O
        this.writeBuffer = ByteBuffer.allocateDirect(WRITE_BUFFER_SIZE);

        // Ensure directory exists
        Files.createDirectories(walDirectory);

        // Open first file
        openNewFile();

        log.info("WAL Writer initialized: shard={}, directory={}", shardId, walDirectory);
    }

    // ===== IMMEDIATE WRITE API =====

    /**
     * Немедленная запись WAL batch на диск
     * @param walBatch сериализованные данные batch'а
     * @return file offset где записан batch
     */
    public synchronized long writeWALBatchImmediate(WALBatchData walBatch) throws IOException {
        if (!isOpen) {
            throw new IOException("WAL Writer is closed");
        }

        byte[] serializedData = serializeWALBatch(walBatch);
        long writeOffset = currentFileOffset.get();

        // Check if need file rotation
        if (writeOffset + serializedData.length > MAX_FILE_SIZE) {
            rotateFile();
            writeOffset = 0;
        }

        // Write immediately с alignment
        long actualBytesWritten = writeAlignedData(serializedData);

        // Force to disk для durability
        currentChannel.force(false);

        // Update metrics и offset
        currentFileOffset.addAndGet(actualBytesWritten);
        totalBytesWritten.addAndGet(actualBytesWritten);
        totalWrites.incrementAndGet();
        totalFlushes.incrementAndGet();

        log.debug("WAL batch written: shard={}, offset={}, size={} bytes",
            shardId, writeOffset, actualBytesWritten);

        return writeOffset;
    }

    // ===== WAL BATCH SERIALIZATION =====

    private byte[] serializeWALBatch(WALBatchData walBatch) {
        // Calculate total size
        int headerSize = calculateHeaderSize();
        int dataSize = walBatch.getTotalDataSize();
        int totalSize = headerSize + dataSize + 8; // +8 для checksum

        // Align к 4KB boundary для optimal SSD performance
        int alignedSize = alignSize(totalSize, ALIGNMENT_SIZE);

        ByteBuffer buffer = ByteBuffer.allocate(alignedSize);

        // Write batch header
        writeBatchHeader(buffer, walBatch, totalSize);

        // Write account batches
        for (AccountBatch accountBatch : walBatch.getAccountBatches()) {
            writeAccountBatch(buffer, accountBatch);
        }

        // Write checksum
        long checksum = calculateChecksum(buffer, 0, totalSize - 8);
        buffer.putLong(checksum);

        // Pad к aligned size
        while (buffer.position() < alignedSize) {
            buffer.put((byte) 0);
        }

        return buffer.array();
    }

    private void writeBatchHeader(ByteBuffer buffer, WALBatchData walBatch, int totalSize) {
        // WAL Batch Header (64 bytes для cache line alignment)
        buffer.putLong(walBatch.getFileBatchId());           // 8 bytes: global batch ID
        buffer.putLong(System.currentTimeMillis());          // 8 bytes: timestamp
        buffer.putInt(walBatch.getAccountBatches().size());  // 4 bytes: number of account batches
        buffer.putInt(walBatch.getTotalOperations());        // 4 bytes: total operations
        buffer.putInt(totalSize);                            // 4 bytes: total batch size
        buffer.putInt(shardId);                              // 4 bytes: shard identifier

        // Padding to 64 bytes
        byte[] padding = new byte[64 - (8+8+4+4+4+4)];
        buffer.put(padding);
    }

    private void writeAccountBatch(ByteBuffer buffer, AccountBatch accountBatch) {
        // Account Batch Header (32 bytes)
        buffer.putLong(accountBatch.getWalSequenceId());     // 8 bytes: WAL sequence ID
        putUUID(buffer, accountBatch.getAccountId());        // 16 bytes: account UUID
        buffer.putInt(accountBatch.getOperationCount());     // 4 bytes: operation count
        buffer.putInt(accountBatch.getDataSize());           // 4 bytes: data size

        // Account Operations Data
        buffer.put(accountBatch.getSerializedOperations());
    }

    // ===== ALIGNED I/O OPERATIONS =====

    private long writeAlignedData(byte[] data) throws IOException {
        writeBuffer.clear();

        if (data.length <= WRITE_BUFFER_SIZE) {
            // Single write
            writeBuffer.put(data);
            writeBuffer.flip();

            long bytesWritten = 0;
            while (writeBuffer.hasRemaining()) {
                bytesWritten += currentChannel.write(writeBuffer);
            }
            return bytesWritten;

        } else {
            // Multiple writes для больших batches
            long totalWritten = 0;
            int offset = 0;

            while (offset < data.length) {
                writeBuffer.clear();
                int chunkSize = Math.min(WRITE_BUFFER_SIZE, data.length - offset);
                writeBuffer.put(data, offset, chunkSize);
                writeBuffer.flip();

                while (writeBuffer.hasRemaining()) {
                    totalWritten += currentChannel.write(writeBuffer);
                }

                offset += chunkSize;
            }

            return totalWritten;
        }
    }

    // ===== FILE MANAGEMENT =====

    private void openNewFile() throws IOException {
        closeCurrentFile();

        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        String fileName = String.format("%s%02d_%06d_%s.wal",
            FILE_PREFIX, shardId,
            currentFileSequence.get(), timestamp);

        Path filePath = walDirectory.resolve(fileName);

        // Open с Direct I/O flags
        currentChannel = FileChannel.open(filePath,
            StandardOpenOption.CREATE_NEW,
            StandardOpenOption.WRITE,
            ExtendedOpenOption.DIRECT); // Direct I/O

        // Pre-allocate file для better performance
        currentChannel.write(ByteBuffer.allocate(1), MAX_FILE_SIZE - 1);
        currentChannel.force(true);
        currentChannel.position(0);

        currentFileOffset.set(0);

        log.info("Opened new WAL file: shard={}, file={}", shardId, fileName);
    }

    private void rotateFile() throws IOException {
        log.info("Rotating WAL file: shard={}, currentSize={}",
            shardId, currentFileOffset.get());

        currentFileSequence.incrementAndGet();
        openNewFile();
    }

    private void closeCurrentFile() throws IOException {
        if (currentChannel != null && currentChannel.isOpen()) {
            currentChannel.force(true);
            currentChannel.close();
        }
    }

    // ===== UTILITY METHODS =====

    private int calculateHeaderSize() {
        return 64; // Fixed header size для alignment
    }

    private int alignSize(int size, int alignment) {
        return ((size + alignment - 1) / alignment) * alignment;
    }

    private void putUUID(ByteBuffer buffer, java.util.UUID uuid) {
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
    }

    private long calculateChecksum(ByteBuffer buffer, int offset, int length) {
        long checksum = 0;
        for (int i = offset; i < offset + length - 8; i += 8) {
            if (i + 8 <= buffer.capacity()) {
                checksum ^= buffer.getLong(i);
            }
        }
        return checksum;
    }

    // ===== LIFECYCLE MANAGEMENT =====

    public synchronized void close() throws IOException {
        if (!isOpen) return;

        isOpen = false;
        closeCurrentFile();

        log.info("WAL Writer closed: shard={}, totalBytes={}, totalWrites={}",
            shardId, totalBytesWritten.get(), totalWrites.get());
    }

    // ===== MONITORING =====

    public WALWriterMetrics getMetrics() {
        return new WALWriterMetrics(
            shardId,
            currentFileSequence.get(),
            currentFileOffset.get(),
            totalBytesWritten.get(),
            totalWrites.get(),
            totalFlushes.get()
        );
    }

    public static class WALWriterMetrics {
        public final int shardId;
        public final long currentFileSequence;
        public final long currentFileOffset;
        public final long totalBytesWritten;
        public final long totalWrites;
        public final long totalFlushes;

        public WALWriterMetrics(int shardId, long currentFileSequence, long currentFileOffset,
                                long totalBytesWritten, long totalWrites, long totalFlushes) {
            this.shardId = shardId;
            this.currentFileSequence = currentFileSequence;
            this.currentFileOffset = currentFileOffset;
            this.totalBytesWritten = totalBytesWritten;
            this.totalWrites = totalWrites;
            this.totalFlushes = totalFlushes;
        }
    }

    // ===== DATA CLASSES =====

    public static class WALBatchData {
        private final long fileBatchId;
        private final java.util.List<AccountBatch> accountBatches;
        private final int totalOperations;

        public WALBatchData(long fileBatchId, java.util.List<AccountBatch> accountBatches) {
            this.fileBatchId = fileBatchId;
            this.accountBatches = accountBatches;
            this.totalOperations = accountBatches.stream()
                .mapToInt(AccountBatch::getOperationCount)
                .sum();
        }

        public long getFileBatchId() { return fileBatchId; }
        public java.util.List<AccountBatch> getAccountBatches() { return accountBatches; }
        public int getTotalOperations() { return totalOperations; }

        public int getTotalDataSize() {
            return accountBatches.stream()
                .mapToInt(batch -> 32 + batch.getDataSize()) // 32 = account header
                .sum();
        }
    }

    public static class AccountBatch {
        private final long walSequenceId;
        private final java.util.UUID accountId;
        private final int operationCount;
        private final byte[] serializedOperations;

        public AccountBatch(long walSequenceId, java.util.UUID accountId,
                            int operationCount, byte[] serializedOperations) {
            this.walSequenceId = walSequenceId;
            this.accountId = accountId;
            this.operationCount = operationCount;
            this.serializedOperations = serializedOperations;
        }

        public long getWalSequenceId() { return walSequenceId; }
        public java.util.UUID getAccountId() { return accountId; }
        public int getOperationCount() { return operationCount; }
        public byte[] getSerializedOperations() { return serializedOperations; }
        public int getDataSize() { return serializedOperations.length; }
    }
}