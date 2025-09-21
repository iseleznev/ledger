package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration.LedgerConfiguration;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.EntryRecordBatchHandler;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Segmented Arena approach for multiple EntryRecordLayout instances
 * Each hot account gets its own isolated memory segment within a shared Arena
 */
@Slf4j
public class HotAccountEntryRecordWorkersManager {

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
    private final ConcurrentHashMap<Integer, HotAccountEntryRecordWorker> workersMap;
//    private final ExecutorService[] stripeExecutors;
//    private final ConcurrentHashMap<Integer, HotAccountEntryRecordWorker> processors = new ConcurrentHashMap<>();

//    private final ReentrantReadWriteLock cleanupLock = new ReentrantReadWriteLock();

    // Metrics and monitoring
    private final AtomicLong totalSegmentsCreated = new AtomicLong(0);
    private final AtomicLong totalSegmentsReleased = new AtomicLong(0);
    private final AtomicLong lastCleanupTime = new AtomicLong(System.currentTimeMillis());
    private final StripeExceptionHandler exceptionHandler;

    public HotAccountEntryRecordWorkersManager(
        LedgerConfiguration ledgerConfiguration,
        StripeExceptionHandler exceptionHandler
        //long totalArenaSizeGB, long segmentSizeMB, int stripeThreadsCount
    ) {
        this.exceptionHandler = exceptionHandler;
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

//        this.stripeExecutors = new ExecutorService[ledgerConfiguration.hotAccounts().workerThreadsCount()];
        this.workersMap = new ConcurrentHashMap<>(ledgerConfiguration.hotAccounts().workerThreadsCount());

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
        EntryRecordBatchHandler batchHandler = workersMap.get(accountId);
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
            return initWorkers();

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
    public EntryRecordBatchHandler initWorkers(
        LedgerConfiguration ledgerConfiguration,
        Map<Integer, Long> walSequenceIdMap,
        HotAccountPostgresWorkersManager postgresWorkersManager
    ) {
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
                this.entriesBatchSize * workersMap.size(),
                64// 64-byte aligned
            );

            // Create segment wrapper
//            final AccountLayoutSegment accountSegment = new AccountLayoutSegment(
//                accountId, memorySegment, handler, System.currentTimeMillis()
//            );

            // В вашем классе
//            private static final int QUEUE_SIZE = 1000;

// Инициализация
            for (int workerIndex = 0; workerIndex < workersMap.size(); workerIndex++) {
                workersMap.put(
                    workerIndex,
                    new HotAccountEntryRecordWorker(
                        workerIndex,
                        walSequenceIdMap.get(workerIndex),
                        ledgerConfiguration,
                        postgresWorkersManager,

                        /*
                                int workerId,
        EntriesSnapshotSharedBatchHandler snapshotSharedBatchHandler,
        PostgreSqlEntriesSnapshotBatchRingBufferHandler snapshotBatchRingBufferHandler,
        WalEntryRecordBatchRingBufferHandler walEntryRecordBatchRingBufferHandler

                         */
                    )
                );

//                final BlockingQueue<Runnable> boundedQueue = new ArrayBlockingQueue<>(ledgerConfiguration.entries().workerBoundedQueueSize());
//
//                final ThreadFactory threadFactory = runnable -> {
//                    Thread thread = Thread.ofPlatform()
//                        .name("account-stripe-" + currentWorkerIndex)
//                        .priority(Thread.NORM_PRIORITY + 1) // Повышенный приоритет
//                        .unstarted(runnable);
//
//                    // Настройки для финансового приложения
//                    thread.setUncaughtExceptionHandler(
//                        exceptionHandler.createHandler(currentWorkerIndex)
//                    );
//
//                    return thread;
//                };
//
//                StripeQueueRejectionHandler rejectionHandler =
//                    new StripeQueueRejectionHandler(currentWorkerIndex, alertingService, stripeMetrics);
//
//                // ThreadPoolExecutor с bounded queue
//                this.stripeExecutors[currentWorkerIndex] = new ThreadPoolExecutor(
//                    1, 1,                          // Ровно один поток
//                    0L, TimeUnit.MILLISECONDS,     // Без keep-alive
//                    boundedQueue,                  // Bounded queue на 1000 элементов
//                    threadFactory,                 // Ваша ThreadFactory
//                    rejectionHandler              // Custom rejection handler
//                );
//
////                this.stripeExecutors[workerIndex] = new ThreadPoolExecutor(
////                    1, 1,
////                    0L, TimeUnit.MILLISECONDS,
////                    new ArrayBlockingQueue<>(ledgerConfiguration.entries().workerBoundedQueueSize()),
////                    runnable -> {
////                        Thread thread = Thread.ofPlatform()
////                            .name("account-stripe-" + currentWorkerIndex)
////                            .priority(Thread.NORM_PRIORITY + 1)
////                            .unstarted(runnable);
////
////                        thread.setUncaughtExceptionHandler(
////                            exceptionHandler.createHandler(currentWorkerIndex)
////                        );
////
////                        return thread;
////                    },
////                    (runnable, executor) -> {
////                        log.warn(
////                            "Stripe worker-{} queue overflow, executing in caller thread",
////                            currentWorkerIndex
////                        );
////
////                        if (!executor.isShutdown()) {
////                            runnable.run(); // CallerRuns behavior
////                        }
////                    }
////                );
////                StripeQueueRejectionHandler rejectionHandler =
////                    new StripeQueueRejectionHandler(currentWorkerIndex, alertingService, stripeMetrics);
////
////                // ThreadPoolExecutor с bounded queue
////                this.stripeExecutors[currentWorkerIndex] = new ThreadPoolExecutor(
////                    1, 1,                          // Ровно один поток
////                    0L, TimeUnit.MILLISECONDS,     // Без keep-alive
////                    boundedQueue,                  // Bounded queue на 1000 элементов
////                    threadFactory,                 // Ваша ThreadFactory
////                    rejectionHandler              // Custom rejection handler
////                );
//            }


        } catch (Exception ) {
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
            final EntryRecordBatchHandler segment = workersMap.remove(accountId);
            if (segment != null) {
                totalSegmentsReleased.incrementAndGet();
                log.debug("Manually released segment for account {}: total_active={}",
                    accountId, workersMap.size());
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

            EntryRecordBatchHandler batchHandler = workersMap.remove(accountId);

            cleanedCount++;
            totalSegmentsReleased.incrementAndGet();

            log.debug("Cleaned up idle segment for account {}: idle_time={}ms",
                accountId, currentTime - batchHandler.lastAccessTime());
            lastCleanupTime.set(currentTime);

            log.info("Cleanup completed: removed {} idle segments, active_segments={}",
                cleanedCount, workersMap.size());
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
        return workersMap.containsKey(accountId);
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
                .activeSegments(workersMap.size())
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
        return (double) (workersMap.size() * entriesBatchSize) / totalArenaSize * 100.0;
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
            workersMap.size());

//        cleanupLock.writeLock().lock();
        try {
            // Clear all active segments
            int segmentCount = workersMap.size();
            workersMap.clear();
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