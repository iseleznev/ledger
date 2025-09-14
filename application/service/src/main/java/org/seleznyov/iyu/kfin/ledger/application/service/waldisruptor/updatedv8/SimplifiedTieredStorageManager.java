package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor.updatedv8;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simplified Tiered Storage Manager - только базовый функционал
 */
@Slf4j
public class SimplifiedTieredStorageManager implements AutoCloseable {

    private final Path dataDirectory;
    private final Path tierDirectory;
    private final ScheduledExecutorService tieringExecutor =
        Executors.newScheduledThreadPool(1, Thread.ofVirtual().name("tiering-").factory());

    // Statistics
    private final AtomicLong tieredOperations = new AtomicLong(0);
    private final AtomicLong tieredDataSize = new AtomicLong(0);

    public SimplifiedTieredStorageManager(Path dataDirectory) throws IOException {
        this.dataDirectory = dataDirectory;
        this.tierDirectory = dataDirectory.resolve("tier2");
        Files.createDirectories(tierDirectory);

        startTieringTasks();
        log.info("Initialized simplified tiered storage at {}", tierDirectory);
    }

    public void scheduleForTiering(long offset, byte[] data) {
        tieringExecutor.schedule(() -> {
            try {
                moveToTier2Storage(offset, data);
            } catch (Exception e) {
                log.error("Tiering failed for offset {}", offset, e);
            }
        }, 1, TimeUnit.HOURS);
    }

    private void moveToTier2Storage(long offset, byte[] data) throws IOException {
        // Group data by offset ranges for efficient storage
        long fileGroup = offset / 1_000_000; // 1M operations per file
        Path tier2File = tierDirectory.resolve("tier2-" + fileGroup + ".data");

        // Append to tier-2 file
        try (FileChannel channel = FileChannel.open(tier2File,
            StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {

            // Write with offset prefix for indexing
            ByteBuffer buffer = ByteBuffer.allocate(8 + 4 + data.length);
            buffer.putLong(offset);
            buffer.putInt(data.length);
            buffer.put(data);
            buffer.flip();

            channel.write(buffer);
            channel.force(true); // Ensure durability in tier-2
        }

        tieredOperations.incrementAndGet();
        tieredDataSize.addAndGet(data.length);

        log.debug("Moved offset {} to tier-2 storage (group {})", offset, fileGroup);
    }

    private void startTieringTasks() {
        // Background cleanup of old tier-2 files
        tieringExecutor.scheduleWithFixedDelay(() -> {
            try {
                cleanupOldTierFiles();
                compactTierFiles();
            } catch (Exception e) {
                log.error("Tier maintenance failed", e);
            }
        }, 6, 6, TimeUnit.HOURS); // Run every 6 hours

        log.info("Started tiering background tasks");
    }

    private void cleanupOldTierFiles() throws IOException {
        // Keep only last 90 days of tier-2 data
        long cutoffTime = System.currentTimeMillis() - (90L * 24 * 60 * 60 * 1000);

        try (var stream = Files.list(tierDirectory)) {
            List<Path> oldFiles = stream
                .filter(path -> path.getFileName().toString().startsWith("tier2-"))
                .filter(path -> {
                    try {
                        return Files.getLastModifiedTime(path).toMillis() < cutoffTime;
                    } catch (IOException e) {
                        return false;
                    }
                })
                .toList();

            for (Path oldFile : oldFiles) {
                try {
                    long fileSize = Files.size(oldFile);
                    Files.delete(oldFile);
                    tieredDataSize.addAndGet(-fileSize);
                    log.info("Deleted old tier-2 file: {} ({}MB freed)",
                        oldFile.getFileName(), fileSize / (1024 * 1024));
                } catch (IOException e) {
                    log.warn("Failed to delete tier-2 file: {}", oldFile, e);
                }
            }
        }
    }

    private void compactTierFiles() {
        try (var stream = Files.list(tierDirectory)) {
            stream.filter(path -> path.getFileName().toString().startsWith("tier2-"))
                .forEach(this::compactTierFileIfNeeded);
        } catch (IOException e) {
            log.warn("Failed to enumerate tier files for compaction", e);
        }
    }

    private void compactTierFileIfNeeded(Path tierFile) {
        try {
            long fileSize = Files.size(tierFile);

            if (fileSize < 10 * 1024 * 1024) {
                return;
            }

            Path compactedFile = tierFile.getParent().resolve(tierFile.getFileName() + ".compact");

            try (FileChannel source = FileChannel.open(tierFile, StandardOpenOption.READ);
                 FileChannel target = FileChannel.open(compactedFile,
                     StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {

                ByteBuffer readBuffer = ByteBuffer.allocate(64 * 1024);
                long totalCompacted = 0;

                while (source.read(readBuffer) > 0) {
                    readBuffer.flip();
                    target.write(readBuffer);
                    totalCompacted += readBuffer.remaining();
                    readBuffer.clear();
                }

                target.force(true);
                Files.move(compactedFile, tierFile);

                log.debug("Compacted tier file {} ({}MB)",
                    tierFile.getFileName(), totalCompacted / (1024 * 1024));
            }

        } catch (IOException e) {
            log.warn("Failed to compact tier file: {}", tierFile, e);
        }
    }

    public Map<String, Object> getStats() {
        return Map.of(
            "tieredOperations", tieredOperations.get(),
            "tieredDataSizeMB", tieredDataSize.get() / (1024 * 1024),
            "tier2Directory", tierDirectory.toString(),
            "tier2FileCount", countTierFiles()
        );
    }

    private int countTierFiles() {
        try (var stream = Files.list(tierDirectory)) {
            return (int) stream.filter(path -> path.getFileName().toString().startsWith("tier2-")).count();
        } catch (IOException e) {
            return 0;
        }
    }

    @Override
    public void close() {
        tieringExecutor.shutdown();
        try {
            if (!tieringExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                tieringExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            tieringExecutor.shutdownNow();
        }

        log.info("Closed tiered storage manager. Final stats: {} operations, {}MB tiered",
            tieredOperations.get(), tieredDataSize.get() / (1024 * 1024));
    }
}
