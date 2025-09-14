package org.seleznyov.iyu.kfin.ledger.application.service.percore;

import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Periodic Snapshot Service - создание снапшотов состояния счетов
 */
@Slf4j
@Component
public class PeriodicSnapshotService {

    private final Path snapshotDirectory;
    private final PhysicalCoreBalanceStorage balanceStorage;
    private final DateTimeFormatter fileNameFormat = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");

    // Snapshot management
    private final AtomicLong totalSnapshots = new AtomicLong(0);
    private final int maxSnapshotsToKeep = 10;

    // Async snapshot creation
    private final ExecutorService snapshotExecutor = Executors.newSingleThreadExecutor(
        Thread.ofVirtual().name("snapshot-creator-").factory());

    public PeriodicSnapshotService(Path snapshotDirectory, PhysicalCoreBalanceStorage balanceStorage)
        throws IOException {
        this.snapshotDirectory = snapshotDirectory;
        this.balanceStorage = balanceStorage;

        Files.createDirectories(snapshotDirectory);
        log.info("Initialized PeriodicSnapshotService at {}", snapshotDirectory);
    }

    /**
     * Create snapshot of all account balances
     */
    public CompletableFuture<String> createSnapshot() {
        return CompletableFuture.supplyAsync(() -> {
            String snapshotFileName = "snapshot_" + LocalDateTime.now().format(fileNameFormat) + ".dat";
            Path snapshotPath = snapshotDirectory.resolve(snapshotFileName);

            try {
                long startTime = System.currentTimeMillis();

                // Create snapshot using callback-based API
                CompletableFuture<Map<UUID, Long>> snapshotFuture = new CompletableFuture<>();

                balanceStorage.createSnapshotAsync(new PhysicalCoreBalanceStorage.SnapshotCallback() {
                    @Override
                    public void onSuccess(Map<UUID, Long> snapshot) {
                        snapshotFuture.complete(snapshot);
                    }

                    @Override
                    public void onError(Exception e) {
                        snapshotFuture.completeExceptionally(e);
                    }
                });

                Map<UUID, Long> balances = snapshotFuture.join();

                // Write snapshot to disk
                writeSnapshotToDisk(snapshotPath, balances);

                long duration = System.currentTimeMillis() - startTime;
                totalSnapshots.incrementAndGet();

                log.info("Created snapshot {} with {} accounts in {}ms",
                    snapshotFileName, balances.size(), duration);

                // Cleanup old snapshots
                cleanupOldSnapshots();

                return snapshotFileName;

            } catch (Exception e) {
                log.error("Failed to create snapshot {}", snapshotFileName, e);
                throw new RuntimeException("Snapshot creation failed", e);
            }
        }, snapshotExecutor);
    }

    private void writeSnapshotToDisk(Path snapshotPath, Map<UUID, Long> balances) throws IOException {
        try (FileChannel channel = FileChannel.open(snapshotPath,
            StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {

            ByteBuffer buffer = ByteBuffer.allocateDirect(1024 * 1024); // 1MB buffer

            // Write header: [VERSION:4][TIMESTAMP:8][ACCOUNT_COUNT:4]
            buffer.putInt(1); // Version
            buffer.putLong(System.currentTimeMillis());
            buffer.putInt(balances.size());

            // Write account entries: [ACCOUNT_ID:16][BALANCE:8] = 24 bytes each
            for (Map.Entry<UUID, Long> entry : balances.entrySet()) {
                // Check buffer space
                if (buffer.remaining() < 24) {
                    buffer.flip();
                    channel.write(buffer);
                    buffer.clear();
                }

                UUID accountId = entry.getKey();
                buffer.putLong(accountId.getMostSignificantBits());
                buffer.putLong(accountId.getLeastSignificantBits());
                buffer.putLong(entry.getValue());
            }

            // Write remaining data
            if (buffer.position() > 0) {
                buffer.flip();
                channel.write(buffer);
            }

            channel.force(true); // Ensure data is on disk
        }
    }

    /**
     * Load snapshot from disk for recovery
     */
    public Map<UUID, Long> loadSnapshot(Path snapshotPath) throws IOException {
        Map<UUID, Long> balances = new HashMap<>();

        try (FileChannel channel = FileChannel.open(snapshotPath, StandardOpenOption.READ)) {
            ByteBuffer buffer = ByteBuffer.allocateDirect(1024 * 1024);

            // Read header
            channel.read(buffer);
            buffer.flip();

            int version = buffer.getInt();
            long timestamp = buffer.getLong();
            int accountCount = buffer.getInt();

            log.info("Loading snapshot: version={}, timestamp={}, accounts={}",
                version, timestamp, accountCount);

            buffer.clear();

            // Read account entries
            int accountsRead = 0;
            while (accountsRead < accountCount && channel.read(buffer) > 0) {
                buffer.flip();

                while (buffer.remaining() >= 24 && accountsRead < accountCount) {
                    UUID accountId = new UUID(buffer.getLong(), buffer.getLong());
                    long balance = buffer.getLong();
                    balances.put(accountId, balance);
                    accountsRead++;
                }

                // Handle partial entry
                if (buffer.hasRemaining()) {
                    buffer.compact();
                } else {
                    buffer.clear();
                }
            }

            log.info("Loaded {} account balances from snapshot {}",
                balances.size(), snapshotPath.getFileName());
        }

        return balances;
    }

    /**
     * Find latest snapshot file
     */
    public Optional<Path> findLatestSnapshot() throws IOException {
        try (var stream = Files.list(snapshotDirectory)) {
            return stream
                .filter(path -> path.getFileName().toString().startsWith("snapshot_"))
                .filter(path -> path.getFileName().toString().endsWith(".dat"))
                .max(Comparator.comparing(path -> {
                    try {
                        return Files.getLastModifiedTime(path);
                    } catch (IOException e) {
                        return null;
                    }
                }));
        }
    }

    private void cleanupOldSnapshots() {
        try (var stream = Files.list(snapshotDirectory)) {
            List<Path> snapshots = stream
                .filter(path -> path.getFileName().toString().startsWith("snapshot_"))
                .filter(path -> path.getFileName().toString().endsWith(".dat"))
                .sorted(Comparator.comparing(path -> {
                    try {
                        return Files.getLastModifiedTime(path);
                    } catch (IOException e) {
                        return null;
                    }
                }, Comparator.reverseOrder()))
                .toList();

            // Keep only the latest N snapshots
            if (snapshots.size() > maxSnapshotsToKeep) {
                for (int i = maxSnapshotsToKeep; i < snapshots.size(); i++) {
                    try {
                        Files.delete(snapshots.get(i));
                        log.debug("Deleted old snapshot: {}", snapshots.get(i).getFileName());
                    } catch (IOException e) {
                        log.warn("Failed to delete old snapshot: {}", snapshots.get(i), e);
                    }
                }
            }
        } catch (IOException e) {
            log.warn("Failed to cleanup old snapshots", e);
        }
    }

    /**
     * Scheduled snapshot creation
     */
    @Scheduled(fixedDelay = 300000) // Every 5 minutes
    public void createScheduledSnapshot() {
        createSnapshot().whenComplete((fileName, error) -> {
            if (error != null) {
                log.error("Scheduled snapshot creation failed", error);
            } else {
                log.info("Scheduled snapshot created: {}", fileName);
            }
        });
    }

    public Map<String, Object> getStatistics() {
        return Map.of(
            "totalSnapshots", totalSnapshots.get(),
            "snapshotDirectory", snapshotDirectory.toString(),
            "maxSnapshotsToKeep", maxSnapshotsToKeep
        );
    }

    public void close() {
        snapshotExecutor.shutdown();
        log.info("Closed PeriodicSnapshotService. Final stats: {}", getStatistics());
    }
}
