package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration.LedgerConfiguration;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.EntryRecordBatchHandler;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.WalEntryRecordBatchRingBufferHandler;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.processor.EntryRecordBatchShardProcessor;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Segmented Arena approach for multiple EntryRecordLayout instances
 * Each hot account gets its own isolated memory segment within a shared Arena
 */
@Slf4j
public class HotAccountArenaManager {

    // Configuration constants
//    private static final long DEFAULT_SEGMENT_SIZE = 64 * 1024 * 1024; // 64MB per account
    private static final long IDLE_TIMEOUT_MS = 30 * 60 * 1000L; // 30 minutes
    private static final int CLEANUP_INTERVAL_SECONDS = 300; // 5 minutes

    // Arena and memory management
    private final Arena sharedArena;
    private final long entriesBatchSize;
    private final long maxBatchEntriesCount;
//    private final long maxSegments;
//    private final long totalArenaSize;

    // Account segments registry
    private final ConcurrentHashMap<Integer, EntryRecordBatchHandler> batchHandlers = new ConcurrentHashMap<>();
    private final ExecutorService[] stripeExecutors;
    private final ConcurrentHashMap<Integer, EntryRecordBatchShardProcessor> processors = new ConcurrentHashMap<>();

//    private final ReentrantReadWriteLock cleanupLock = new ReentrantReadWriteLock();

    // Metrics and monitoring
    private final AtomicLong totalSegmentsCreated = new AtomicLong(0);
    private final AtomicLong totalSegmentsReleased = new AtomicLong(0);
    private final AtomicLong lastCleanupTime = new AtomicLong(System.currentTimeMillis());

    public HotAccountArenaManager(
        LedgerConfiguration ledgerConfiguration
        //long totalArenaSizeGB, long segmentSizeMB, int stripeThreadsCount
    ) {
//        this.totalArenaSize =  //totalArenaSizeGB * 1024L * 1024L * 1024L;
//        this.entriesBatchSize = segmentSizeMB * 1024L * 1024L;
//        this.maxSegments = totalArenaSize / entriesBatchSize;

//        if (maxSegments <= 0) {
//            throw new IllegalArgumentException(
//                "Arena size too small: " + totalArenaSize + " bytes, segment size: " + entriesBatchSize + " bytes"
//            );
//        }
        this.maxBatchEntriesCount = ledgerConfiguration.entries().batchEntriesCount();
        this.entriesBatchSize = this.maxBatchEntriesCount * EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE;

        this.sharedArena = Arena.ofShared();

        this.stripeExecutors = new ExecutorService[ledgerConfiguration.hotAccounts().workerThreadsCount()];

//        log.info("Created SegmentedHotAccountArena: total_size={}GB, segment_size={}MB, max_segments={}",
//            totalArenaSizeGB, segmentSizeMB / (1024 * 1024), maxSegments);
    }

    /**
     * Gets or creates EntryRecordLayout for the specified account
     * Thread-safe method with lazy initialization
     */
    public EntryRecordBatchHandler getBatchHolder(UUID accountId, long totalAmount) {
        if (accountId == null) {
            throw new IllegalArgumentException("Account ID cannot be null");
        }

        // Fast path - segment already exists
        EntryRecordBatchHandler batchHandler = batchHandlers.get(accountId);
        if (batchHandler != null) {
            batchHandler.lastAccessTime(LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli());
            return batchHandler;
        }

        // Slow path - create new segment
//        cleanupLock.readLock().lock();
        try {
            // Double-checked locking pattern
//            batchHandler = activeSegments.get(accountId);
//            if (batchHandler != null) {
//                batchHandler.lastAccessTime(LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli());
//                return batchHandler;
//            }
            //TODO: получить сумму из базы данных
            return initializeWorkers();

        } catch (Exception e) {
            log.error("Failed to get or create segment for account {}", accountId, e);
            throw new RuntimeException("Segment creation failed for account " + accountId, e);
        }
//        finally {
//            cleanupLock.readLock().unlock();
//        }
    }

    /**
     * Creates a new memory segment and layout for the account
     */
    public EntryRecordBatchHandler initializeWorkers(LedgerConfiguration ledgerConfiguration) {
//        if (batchHandlers.size() >= maxSegments) {
        // Try cleanup first
//            performCleanup(accountId);

        // Check again after cleanup
//            if (batchHandlers.size() >= maxSegments) {
//                throw new RuntimeException(
//                    "Maximum segments reached: " + maxSegments +
//                        ". Cannot create segment for account: " + accountId
//                );
//            }
//        }

        try {
            // Allocate memory segment from shared arena
            final MemorySegment memorySegment = sharedArena.allocate(
                this.entriesBatchSize * stripeExecutors.length,
                64// 64-byte aligned
            );

            // Create segment wrapper
//            final AccountLayoutSegment accountSegment = new AccountLayoutSegment(
//                accountId, memorySegment, handler, System.currentTimeMillis()
//            );

            // Register the segment
            for (int i = 0; i < stripeExecutors.length; ++i) {
                final int workerIndex = i;

                this.stripeExecutors[workerIndex] = Executors.newSingleThreadExecutor(runnable -> {
                    Thread thread = Thread.ofPlatform()
                        .name("account-stripe-" + workerIndex)
                        .priority(Thread.NORM_PRIORITY + 1) // Повышенный приоритет
                        .unstarted(runnable);

                    // Настройки для финансового приложения
                    thread.setUncaughtExceptionHandler((t, exception) -> {
                            log.error("Uncaught exception in stripe {} thread: {}", workerIndex, t.getName(), exception);
                            // Здесь можно добавить алертинг
                        }
                    );

                    return thread;
                });

                final EntryRecordBatchHandler entriesBatchHandler = new EntryRecordBatchHandler(
                    memorySegment.asSlice(this.entriesBatchSize * workerIndex, this.entriesBatchSize),
                    walSequenceId,
                    ledgerConfiguration
                );

                processors.put(
                    workerIndex,
                    new EntryRecordBatchShardProcessor(
                        entriesBatchHandler,
                        this.stripeExecutors[workerIndex],

/*
        EntryRecordBatchHandler entryRecordBatchHandler,
        ExecutorService shardDedicatedExecutorService,
        PostgreSqlEntryRecordBatchRingBufferHandler pgEntryRecordBatchRingBufferHandler,
        EntriesSnapshotSharedBatchHandler snapshotSharedBatchHandler,
        PostgreSqlEntriesSnapshotBatchRingBufferHandler snapshotBatchRingBufferHandler

 */
                    )
                );

                batchHandlers.put(workerIndex, entriesBatchHandler);
                totalSegmentsCreated.incrementAndGet();
            }

            log.debug("Created new segment for account {}: segment_size={}MB, total_active={}",
                accountId, entriesBatchSize / (1024 * 1024), batchHandlers.size());

            return entriesBatchHandler;

        } catch (Exception e) {
            log.error("Failed to create segment for account {}", accountId, e);
            throw new RuntimeException("Segment creation failed for account " + accountId, e);
        }
    }

    /**
     * Manually releases a segment for the specified account
     * Useful for testing or explicit cleanup
     */
    public boolean releaseSegment(UUID accountId) {
        if (accountId == null) return false;

//        cleanupLock.writeLock().lock();
        try {
            final EntryRecordBatchHandler segment = batchHandlers.remove(accountId);
            if (segment != null) {
                totalSegmentsReleased.incrementAndGet();
                log.debug("Manually released segment for account {}: total_active={}",
                    accountId, batchHandlers.size());
                return true;
            }
            return false;
        } catch (Exception e) {
            log.error("Failed to release segment for account {}", accountId, e);
            return false;
        }
//        } finally {
//            cleanupLock.writeLock().unlock();
//        }
    }

    /**
     * Cleanup idle segments that haven't been accessed recently
     * Called automatically and can be called manually
     */
    public void performCleanup(UUID accountId) {
        final long currentTime = System.currentTimeMillis();

        // Rate limiting - don't cleanup too frequently
        if (currentTime - lastCleanupTime.get() < CLEANUP_INTERVAL_SECONDS * 1000L) {
            return;
        }

//        cleanupLock.writeLock().lock();
        try {
            int cleanedCount = 0;
            long cutoffTime = currentTime - IDLE_TIMEOUT_MS;

            EntryRecordBatchHandler batchHandler = batchHandlers.remove(accountId);

            cleanedCount++;
            totalSegmentsReleased.incrementAndGet();

            log.debug("Cleaned up idle segment for account {}: idle_time={}ms",
                accountId, currentTime - batchHandler.lastAccessTime());
            lastCleanupTime.set(currentTime);

            log.info("Cleanup completed: removed {} idle segments, active_segments={}",
                cleanedCount, batchHandlers.size());
        } catch (Exception e) {
            log.error("Failed to cleanup idle segment for account {}", accountId, e);
        }
//        } finally {
//            cleanupLock.writeLock().unlock();
//        }
    }

    /**
     * Checks if account has an active segment
     */
    public boolean hasActiveSegment(UUID accountId) {
        return batchHandlers.containsKey(accountId);
    }

    /**
     * Gets current metrics for monitoring
     */
    public ArenaMetrics getMetrics() {
//        cleanupLock.readLock().lock();
        try {
            return ArenaMetrics.builder()
                .totalArenaSize(totalArenaSize)
                .segmentSize(entriesBatchSize)
                .maxSegments(maxSegments)
                .activeSegments(batchHandlers.size())
                .totalSegmentsCreated(totalSegmentsCreated.get())
                .totalSegmentsReleased(totalSegmentsReleased.get())
                .memoryUtilization(calculateMemoryUtilization())
                .lastCleanupTime(lastCleanupTime.get())
                .build();
        } finally {
//            cleanupLock.readLock().unlock();
        }
    }

    private double calculateMemoryUtilization() {
        return (double) (batchHandlers.size() * entriesBatchSize) / totalArenaSize * 100.0;
    }

    /**
     * Diagnostic information for monitoring and debugging
     */
    public String getDiagnostics() {
        ArenaMetrics metrics = getMetrics();
        return String.format(
            "SegmentedHotAccountArena[active_segments=%d/%d, utilization=%.1f%%, " +
                "created=%d, released=%d, segment_size=%dMB]",
            metrics.activeSegments, metrics.maxSegments, metrics.memoryUtilization,
            metrics.totalSegmentsCreated, metrics.totalSegmentsReleased,
            metrics.segmentSize / (1024 * 1024)
        );
    }

    /**
     * Graceful shutdown - cleanup all resources
     */
    public void shutdown() {
        log.info("Shutting down SegmentedHotAccountArena with {} active segments",
            batchHandlers.size());

//        cleanupLock.writeLock().lock();
        try {
            // Clear all active segments
            int segmentCount = batchHandlers.size();
            batchHandlers.clear();
            totalSegmentsReleased.addAndGet(segmentCount);

            // Close the shared arena
            if (sharedArena.scope().isAlive()) {
                sharedArena.close();
            }

            log.info("SegmentedHotAccountArena shutdown completed. Final metrics: {}",
                getDiagnostics());

        } catch (Exception e) {
            log.error("Error during arena shutdown", e);
        } finally {
//            cleanupLock.writeLock().unlock();
        }
    }

    // ===== INNER CLASSES =====


    /**
     * Metrics class for monitoring arena usage
     */
    public static class ArenaMetrics {

        public final long totalArenaSize;
        public final long segmentSize;
        public final long maxSegments;
        public final int activeSegments;
        public final long totalSegmentsCreated;
        public final long totalSegmentsReleased;
        public final double memoryUtilization;
        public final long lastCleanupTime;

        private ArenaMetrics(long totalArenaSize, long segmentSize, long maxSegments,
                             int activeSegments, long totalSegmentsCreated, long totalSegmentsReleased,
                             double memoryUtilization, long lastCleanupTime) {
            this.totalArenaSize = totalArenaSize;
            this.segmentSize = segmentSize;
            this.maxSegments = maxSegments;
            this.activeSegments = activeSegments;
            this.totalSegmentsCreated = totalSegmentsCreated;
            this.totalSegmentsReleased = totalSegmentsReleased;
            this.memoryUtilization = memoryUtilization;
            this.lastCleanupTime = lastCleanupTime;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {

            private long totalArenaSize;
            private long segmentSize;
            private long maxSegments;
            private int activeSegments;
            private long totalSegmentsCreated;
            private long totalSegmentsReleased;
            private double memoryUtilization;
            private long lastCleanupTime;

            public Builder totalArenaSize(long totalArenaSize) {
                this.totalArenaSize = totalArenaSize;
                return this;
            }

            public Builder segmentSize(long segmentSize) {
                this.segmentSize = segmentSize;
                return this;
            }

            public Builder maxSegments(long maxSegments) {
                this.maxSegments = maxSegments;
                return this;
            }

            public Builder activeSegments(int activeSegments) {
                this.activeSegments = activeSegments;
                return this;
            }

            public Builder totalSegmentsCreated(long totalSegmentsCreated) {
                this.totalSegmentsCreated = totalSegmentsCreated;
                return this;
            }

            public Builder totalSegmentsReleased(long totalSegmentsReleased) {
                this.totalSegmentsReleased = totalSegmentsReleased;
                return this;
            }

            public Builder memoryUtilization(double memoryUtilization) {
                this.memoryUtilization = memoryUtilization;
                return this;
            }

            public Builder lastCleanupTime(long lastCleanupTime) {
                this.lastCleanupTime = lastCleanupTime;
                return this;
            }

            public ArenaMetrics build() {
                return new ArenaMetrics(totalArenaSize, segmentSize, maxSegments, activeSegments,
                    totalSegmentsCreated, totalSegmentsReleased, memoryUtilization, lastCleanupTime);
            }
        }
    }
}