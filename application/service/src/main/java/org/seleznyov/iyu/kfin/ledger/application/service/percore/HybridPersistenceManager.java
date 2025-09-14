package org.seleznyov.iyu.kfin.ledger.application.service.percore;

import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Updated Hybrid Persistence Manager для работы с PerCoreInMemoryWAL
 */
@Slf4j
@Component
public class HybridPersistenceManager {
    private final BatchingTransactionLogger transactionLogger;
    private final PeriodicSnapshotService snapshotService;
    private final PerCoreInMemoryWAL memoryWAL;

    private final int physicalCores;

    // Async flush processing per core
    private final ExecutorService flushExecutor =
        Executors.newVirtualThreadPerTaskExecutor();

    public HybridPersistenceManager(
        BatchingTransactionLogger transactionLogger,
        PeriodicSnapshotService snapshotService,
        PerCoreInMemoryWAL memoryWAL) {

        this.transactionLogger = transactionLogger;
        this.snapshotService = snapshotService;
        this.memoryWAL = memoryWAL;
        this.physicalCores = extractPhysicalCoreCount(memoryWAL);

        log.info("Initialized HybridPersistenceManager with {} physical cores", physicalCores);
    }

    private int extractPhysicalCoreCount(PerCoreInMemoryWAL wal) {
        @SuppressWarnings("unchecked")
        Map<String, Object> stats = wal.getStatistics();
        return (Integer) stats.get("physicalCores");
    }

    public void logOperation(UUID accountId, long delta, UUID operationId) {
        // 1. Immediate: add to per-core in-memory WAL (routed automatically by account)
        memoryWAL.append(accountId, delta, operationId);
    }

    /**
     * Flush pending transactions from all cores to disk
     */
    @Scheduled(fixedDelay = 2000) // Every 2 seconds
    public void flushAllCores() {
        for (int coreId = 0; coreId < physicalCores; coreId++) {
            flushCore(coreId);
        }
    }

    /**
     * Flush specific core's pending transactions
     */
    public void flushCore(int coreId) {
        memoryWAL.getUnflushedEntries(coreId, 1000, entries -> {
            if (!entries.isEmpty()) {
                transactionLogger.writeBatch(entries)
                    .whenComplete((result, error) -> {
                        if (error != null) {
                            log.error("Failed to flush core {} entries", coreId, error);
                        } else {
                            // Mark as flushed after successful write
                            memoryWAL.markFlushed(coreId, entries);
                            log.debug("Flushed {} entries from core {}", entries.size(), coreId);
                        }
                    });
            }
        });
    }

    /**
     * Emergency flush when memory WAL is getting full
     */
    public void triggerEmergencyFlush() {
        log.warn("Emergency flush triggered - WAL buffers near capacity");

        flushExecutor.submit(() -> {
            for (int coreId = 0; coreId < physicalCores; coreId++) {
                flushCore(coreId);
            }
        });
    }

    /**
     * Recovery operations using per-core WAL
     */
    public void performRecovery(RecoveryCallback callback) {
        // Get all unflushed entries from all cores
        memoryWAL.getAllUnflushedEntries(unflushedEntries -> {
            try {
                // Process unflushed entries in timestamp order
                unflushedEntries.forEach(entry -> {
                    // Apply entry to balance storage or queue for processing
                    log.debug("Recovering operation: {} delta={} for account {}",
                        entry.getOperationId(), entry.getDelta(), entry.getAccountId());
                });

                log.info("Recovery completed: {} unflushed entries processed",
                    unflushedEntries.size());

                callback.onSuccess(unflushedEntries.size());

            } catch (Exception e) {
                log.error("Recovery failed", e);
                callback.onError(e);
            }
        });
    }

    /**
     * Get comprehensive statistics from all components
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new HashMap<>();

        // Per-core WAL statistics
        stats.put("perCoreWAL", memoryWAL.getStatistics());

        // Transaction logger statistics
        stats.put("transactionLogger", transactionLogger.getStatistics());

        // Snapshot service statistics
        stats.put("snapshotService", snapshotService.getStatistics());

        return stats;
    }

    /**
     * Check if any core WAL buffer is getting full
     */
    public boolean isAnyCoreLowOnCapacity() {
        @SuppressWarnings("unchecked")
        Map<String, Object> walStats = memoryWAL.getStatistics();
        @SuppressWarnings("unchecked")
        Map<String, Object> coreStats = (Map<String, Object>) walStats.get("coreStatistics");

        for (int i = 0; i < physicalCores; i++) {
            @SuppressWarnings("unchecked")
            Map<String, Object> coreStat = (Map<String, Object>) coreStats.get("core" + i);
            Double utilization = (Double) coreStat.get("utilizationPercent");

            if (utilization > 80.0) { // 80% threshold
                return true;
            }
        }

        return false;
    }

    /**
     * Force flush specific core if it's getting full
     */
    @Scheduled(fixedDelay = 500) // Check every 500ms
    public void checkAndFlushFullCores() {
        @SuppressWarnings("unchecked")
        Map<String, Object> walStats = memoryWAL.getStatistics();
        @SuppressWarnings("unchecked")
        Map<String, Object> coreStats = (Map<String, Object>) walStats.get("coreStatistics");

        for (int i = 0; i < physicalCores; i++) {
            @SuppressWarnings("unchecked")
            Map<String, Object> coreStat = (Map<String, Object>) coreStats.get("core" + i);
            Double utilization = (Double) coreStat.get("utilizationPercent");

            if (utilization > 75.0) { // Force flush at 75%
                log.info("Core {} utilization at {:.1f}%, forcing flush", i, utilization);
                flushCore(i);
            }
        }
    }

    public void close() {
        log.info("Shutting down HybridPersistenceManager...");

        // Final flush of all cores
        for (int coreId = 0; coreId < physicalCores; coreId++) {
            flushCore(coreId);
        }

        flushExecutor.close();

        Map<String, Object> finalStats = getStatistics();
        log.info("HybridPersistenceManager shutdown completed. Final stats: {}", finalStats);
    }

    // Callback interface for recovery
    @FunctionalInterface
    public interface RecoveryCallback {
        void onSuccess(int recoveredEntries);
        default void onError(Exception e) {
            log.error("Recovery callback error", e);
        }
    }
}