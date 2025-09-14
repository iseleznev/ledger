package org.seleznyov.iyu.kfin.ledger.application.service.percore;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Batching Transaction Logger - append-only disk persistence
 */
@Slf4j
@Component
public class BatchingTransactionLogger implements AutoCloseable {

    private final Path logDirectory;
    private final ByteBuffer writeBuffer;
    private final DateTimeFormatter fileNameFormat = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm");

    // Current active log file
    private volatile FileChannel currentLogChannel;
    private volatile String currentLogFileName;
    private final AtomicLong currentLogSize = new AtomicLong(0);
    private final long maxLogFileSizeMB = 100; // 100MB per log file

    // Performance tracking
    private final AtomicLong totalBatches = new AtomicLong(0);
    private final AtomicLong totalTransactions = new AtomicLong(0);

    // Async writing
    private final ExecutorService writeExecutor = Executors.newSingleThreadExecutor(
        Thread.ofVirtual().name("transaction-log-writer-").factory());

    public BatchingTransactionLogger(Path logDirectory) throws IOException {
        this.logDirectory = logDirectory;
        Files.createDirectories(logDirectory);

        // Pre-allocate direct buffer for writing
        this.writeBuffer = ByteBuffer.allocateDirect(1024 * 1024); // 1MB buffer

        initializeCurrentLogFile();
        log.info("Initialized BatchingTransactionLogger at {}", logDirectory);
    }

    private void initializeCurrentLogFile() throws IOException {
        String fileName = "transactions_" + LocalDateTime.now().format(fileNameFormat) + ".log";
        Path logFilePath = logDirectory.resolve(fileName);

        this.currentLogChannel = FileChannel.open(logFilePath,
            StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        this.currentLogFileName = fileName;
        this.currentLogSize.set(currentLogChannel.size());

        log.info("Opened transaction log file: {}", fileName);
    }

    /**
     * Write batch of transactions to append-only log
     */
    public CompletableFuture<Void> writeBatch(List<TransactionEntry> transactions) {
        if (transactions.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        return CompletableFuture.runAsync(() -> {
            try {
                synchronized (writeBuffer) {
                    writeBuffer.clear();

                    // Check if we need to rotate log file
                    long estimatedSize = transactions.size() * TransactionEntry.BINARY_SIZE;
                    if (currentLogSize.get() + estimatedSize > maxLogFileSizeMB * 1024 * 1024) {
                        rotateLogFile();
                    }

                    // Serialize all transactions to buffer
                    for (TransactionEntry transaction : transactions) {
                        transaction.serializeTo(writeBuffer);
                    }

                    writeBuffer.flip();

                    // Single write operation to disk
                    long bytesWritten = currentLogChannel.write(writeBuffer);
                    currentLogChannel.force(false); // fsync data only

                    currentLogSize.addAndGet(bytesWritten);
                    totalBatches.incrementAndGet();
                    totalTransactions.addAndGet(transactions.size());

                    log.debug("Wrote batch of {} transactions ({} bytes) to {}",
                        transactions.size(), bytesWritten, currentLogFileName);
                }
            } catch (IOException e) {
                log.error("Failed to write transaction batch", e);
                throw new RuntimeException("Transaction log write failed", e);
            }
        }, writeExecutor);
    }

    private void rotateLogFile() throws IOException {
        // Close current file
        if (currentLogChannel != null) {
            currentLogChannel.force(true);
            currentLogChannel.close();
        }

        // Open new file
        initializeCurrentLogFile();
        log.info("Rotated transaction log to new file: {}", currentLogFileName);
    }

    /**
     * Read transactions from log file for recovery
     */
    public List<TransactionEntry> readLogFile(Path logFilePath) throws IOException {
        List<TransactionEntry> transactions = new ArrayList<>();

        try (FileChannel channel = FileChannel.open(logFilePath, StandardOpenOption.READ)) {
            ByteBuffer readBuffer = ByteBuffer.allocateDirect(1024 * 1024);

            while (channel.read(readBuffer) > 0) {
                readBuffer.flip();

                while (readBuffer.remaining() >= TransactionEntry.BINARY_SIZE) {
                    try {
                        TransactionEntry entry = TransactionEntry.deserializeFrom(readBuffer);

                        // Verify checksum
                        if (entry.getChecksum() == entry.calculateChecksum()) {
                            transactions.add(entry);
                        } else {
                            log.warn("Checksum mismatch for transaction {}, skipping", entry.getOperationId());
                        }
                    } catch (Exception e) {
                        log.warn("Failed to deserialize transaction, stopping read", e);
                        break;
                    }
                }

                // Handle partial entry at end of buffer
                if (readBuffer.hasRemaining()) {
                    readBuffer.compact();
                } else {
                    readBuffer.clear();
                }
            }
        }

        log.info("Read {} transactions from log file {}", transactions.size(), logFilePath.getFileName());
        return transactions;
    }

    /**
     * Get all log files sorted by creation time
     */
    public List<Path> getLogFiles() throws IOException {
        try (var stream = Files.list(logDirectory)) {
            return stream
                .filter(path -> path.getFileName().toString().startsWith("transactions_"))
                .filter(path -> path.getFileName().toString().endsWith(".log"))
                .sorted(Comparator.comparing(path -> {
                    try {
                        return Files.getLastModifiedTime(path);
                    } catch (IOException e) {
                        return null;
                    }
                }))
                .toList();
        }
    }

    public Map<String, Object> getStatistics() {
        return Map.of(
            "currentLogFile", currentLogFileName,
            "currentLogSizeMB", currentLogSize.get() / (1024 * 1024),
            "totalBatches", totalBatches.get(),
            "totalTransactions", totalTransactions.get(),
            "logDirectory", logDirectory.toString()
        );
    }

    @Override
    public void close() throws IOException {
        writeExecutor.shutdown();

        if (currentLogChannel != null) {
            currentLogChannel.force(true);
            currentLogChannel.close();
        }

        log.info("Closed BatchingTransactionLogger. Final stats: {}", getStatistics());
    }
}

