package org.seleznyov.iyu.kfin.ledger.application.service.containeroptimized;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fully Integrated Container-Optimized Ledger Service
 * Combines high-performance in-memory operations with database persistence
 */
@Service
@Slf4j
public class IntegratedContainerOptimizedLedgerService implements AutoCloseable {

    private final ContainerOptimizedBalanceStorage balanceStorage;
    private final IntegratedContainerOptimizedWAL walStorage;
    private final ImmutablePersistenceTransactionManager persistenceManager;
    private final ScheduledExecutorService scheduler;

    // Performance tracking
    private final AtomicLong totalOperations = new AtomicLong(0);
    private final AtomicLong successfulOperations = new AtomicLong(0);
    private final AtomicLong failedOperations = new AtomicLong(0);

    // Configuration
    private static final int RECOVERY_BATCH_SIZE = 1000;
    private static final int RECOVERY_INTERVAL_SECONDS = 30;
    private static final int STATS_LOG_INTERVAL_SECONDS = 60;

    public IntegratedContainerOptimizedLedgerService(
        ImmutablePersistenceTransactionManager persistenceManager,
        IntegratedContainerOptimizedWAL walStorage) {

        this.persistenceManager = persistenceManager;
        this.walStorage = walStorage;
        this.balanceStorage = new ContainerOptimizedBalanceStorage();

        this.scheduler = Executors.newScheduledThreadPool(3,
            Thread.ofVirtual().name("integrated-ledger-scheduler-").factory());

        // Initialize sequence numbers from database
        persistenceManager.initializeSequenceGenerator();

        startBackgroundTasks();

        log.info("IntegratedContainerOptimizedLedgerService started with database persistence");
    }

    private void startBackgroundTasks() {
        // Periodic WAL recovery from database
        scheduler.scheduleWithFixedDelay(this::performWalRecovery,
            RECOVERY_INTERVAL_SECONDS, RECOVERY_INTERVAL_SECONDS, TimeUnit.SECONDS);

        // Periodic statistics logging
        scheduler.scheduleWithFixedDelay(this::logStatistics,
            STATS_LOG_INTERVAL_SECONDS, STATS_LOG_INTERVAL_SECONDS, TimeUnit.SECONDS);

        // Health monitoring
        scheduler.scheduleWithFixedDelay(this::performHealthCheck, 15, 15, TimeUnit.SECONDS);
    }

    /**
     * Process double-entry transaction with full persistence integration
     */
    public CompletableFuture<LedgerOperationResult> processDoubleEntry(
        UUID debitAccountId, UUID creditAccountId, long amount,
        String currencyCode, DurabilityLevel durabilityLevel) {

        if (amount <= 0) {
            return CompletableFuture.completedFuture(
                LedgerOperationResult.failure("Amount must be positive", "INVALID_AMOUNT"));
        }

        if (debitAccountId.equals(creditAccountId)) {
            return CompletableFuture.completedFuture(
                LedgerOperationResult.failure("Debit and credit accounts cannot be the same", "SAME_ACCOUNT"));
        }

        UUID transactionId = UUID.randomUUID();
        UUID idempotencyKey = UUID.randomUUID();
        long startTime = System.nanoTime();

        return processDoubleEntryInternal(debitAccountId, creditAccountId, amount,
            currencyCode, durabilityLevel, transactionId, idempotencyKey, startTime)
            .exceptionally(error -> {
                failedOperations.incrementAndGet();
                log.error("Double-entry operation failed: debit={}, credit={}, amount={}, tx={}",
                    debitAccountId, creditAccountId, amount, transactionId, error);

                return LedgerOperationResult.failure(
                    "Operation processing failed: " + error.getMessage(),
                    "PROCESSING_ERROR");
            });
    }

    private CompletableFuture<LedgerOperationResult> processDoubleEntryInternal(
        UUID debitAccountId, UUID creditAccountId, long amount,
        String currencyCode, DurabilityLevel durabilityLevel,
        UUID transactionId, UUID idempotencyKey, long startTime) {

        // Step 1: Write to persistence layer (WAL + Entry Records)
        CompletableFuture<ImmutablePersistenceTransactionManager.DoubleEntryResult> persistenceFuture =
            persistenceManager.processDoubleEntry(debitAccountId, creditAccountId, amount,
                currencyCode, durabilityLevel, transactionId, idempotencyKey);

        // Step 2: Update in-memory balances in parallel for performance
        CompletableFuture<Long> debitFuture = balanceStorage.addToBalance(debitAccountId, -amount);
        CompletableFuture<Long> creditFuture = balanceStorage.addToBalance(creditAccountId, amount);

        // Step 3: Combine results
        return CompletableFuture.allOf(persistenceFuture, debitFuture, creditFuture)
            .thenApply(ignored -> {
                ImmutablePersistenceTransactionManager.DoubleEntryResult persistenceResult = persistenceFuture.join();

                if (!persistenceResult.isSuccess()) {
                    // Persistence failed - need to rollback in-memory changes
                    // In production, this would be more sophisticated
                    log.error("Persistence failed for transaction {}, reverting in-memory changes", transactionId);
                    balanceStorage.addToBalance(debitAccountId, amount);  // Reverse debit
                    balanceStorage.addToBalance(creditAccountId, -amount); // Reverse credit

                    failedOperations.incrementAndGet();
                    return LedgerOperationResult.failure(persistenceResult.getErrorMessage(), "PERSISTENCE_ERROR");
                }

                long debitBalance = debitFuture.join();
                long creditBalance = creditFuture.join();
                long sequenceNumber = totalOperations.incrementAndGet();
                long processingTime = System.nanoTime() - startTime;

                successfulOperations.incrementAndGet();

                return LedgerOperationResult.builder()
                    .success(true)
                    .sequenceNumber(sequenceNumber)
                    .transactionId(transactionId)
                    .processingTimeNanos(processingTime)
                    .debitAccountBalanceAfter(debitBalance)
                    .creditAccountBalanceAfter(creditBalance)
                    .walSequenceNumber(persistenceResult.getSequenceNumber())
                    .build();
            });
    }

    /**
     * Get balance - prioritizes in-memory for speed, falls back to database for accuracy
     */
    public CompletableFuture<OptionalLong> getBalance(UUID accountId) {
        // First try in-memory for speed
        return balanceStorage.getBalance(accountId)
            .thenCompose(memoryBalance -> {
                if (memoryBalance.isPresent()) {
                    return CompletableFuture.completedFuture(memoryBalance);
                } else {
                    // Fall back to database calculation
                    return persistenceManager.getCurrentBalance(accountId)
                        .thenApply(dbResult -> {
                            if (dbResult.hasError()) {
                                log.warn("Database balance query failed for account {}: {}",
                                    accountId, dbResult.getErrorMessage());
                                return OptionalLong.empty();
                            }

                            // Update in-memory cache
                            balanceStorage.updateBalance(accountId, dbResult.getBalance());

                            return OptionalLong.of(dbResult.getBalance());
                        });
                }
            });
    }

    /**
     * Get balances for multiple accounts
     */
    public CompletableFuture<Map<UUID, Long>> getBalances(Set<UUID> accountIds) {
        return balanceStorage.getBalances(accountIds)
            .thenCompose(memoryBalances -> {
                // Find accounts missing from memory
                Set<UUID> missingAccounts = new HashSet<>(accountIds);
                missingAccounts.removeAll(memoryBalances.keySet());

                if (missingAccounts.isEmpty()) {
                    return CompletableFuture.completedFuture(memoryBalances);
                }

                // Fetch missing balances from database
                return persistenceManager.getCurrentBalances(missingAccounts)
                    .thenApply(dbResults -> {
                        Map<UUID, Long> allBalances = new HashMap<>(memoryBalances);

                        dbResults.forEach((accountId, result) -> {
                            if (!result.hasError()) {
                                allBalances.put(accountId, result.getBalance());
                                // Update in-memory cache
                                balanceStorage.updateBalance(accountId, result.getBalance());
                            }
                        });

                        return allBalances;
                    });
            });
    }

    /**
     * Perform WAL recovery from database
     */
    private void performWalRecovery() {
        try {
            persistenceManager.recoverFromWal(RECOVERY_BATCH_SIZE)
                .thenAccept(result -> {
                    if (result.isSuccess() && result.getSuccessCount() > 0) {
                        log.info("WAL recovery completed: {} successful, {} failed",
                            result.getSuccessCount(), result.getFailureCount());

                        if (!result.getErrors().isEmpty()) {
                            log.warn("WAL recovery errors: {}", result.getErrors());
                        }
                    }
                })
                .exceptionally(error -> {
                    log.error("WAL recovery task failed", error);
                    return null;
                });

        } catch (Exception e) {
            log.error("WAL recovery scheduler error", e);
        }
    }

    /**
     * Create system snapshot including both in-memory and persistent state
     */
    public CompletableFuture<Map<String, Object>> createFullSystemSnapshot() {
        log.info("Creating full integrated system snapshot...");

        CompletableFuture<Map<UUID, Long>> memorySnapshot = balanceStorage.createSnapshot();
        CompletableFuture<Map<UUID, ImmutablePersistenceTransactionManager.BalanceResult>> dbSnapshot =
            getAllAccountBalancesFromDatabase();

        return CompletableFuture.allOf(memorySnapshot, dbSnapshot)
            .thenApply(ignored -> {
                Map<String, Object> fullSnapshot = new HashMap<>();
                fullSnapshot.put("timestamp", System.currentTimeMillis());
                fullSnapshot.put("memoryBalances", memorySnapshot.join());
                fullSnapshot.put("databaseBalances", convertDbSnapshot(dbSnapshot.join()));
                fullSnapshot.put("walStatistics", walStorage.getStatistics());
                fullSnapshot.put("balanceStatistics", balanceStorage.getStatistics());
                fullSnapshot.put("systemStatistics", getSystemStatistics());

                log.info("Full integrated system snapshot created");
                return fullSnapshot;
            });
    }

    /**
     * Get all account balances from database (for reconciliation)
     */
    private CompletableFuture<Map<UUID, ImmutablePersistenceTransactionManager.BalanceResult>> getAllAccountBalancesFromDatabase() {
        // This would need to be implemented to get all accounts - simplified for now
        return CompletableFuture.completedFuture(Collections.emptyMap());
    }

    private Map<UUID, Long> convertDbSnapshot(Map<UUID, ImmutablePersistenceTransactionManager.BalanceResult> dbResults) {
        Map<UUID, Long> converted = new HashMap<>();
        dbResults.forEach((accountId, result) -> {
            if (!result.hasError()) {
                converted.put(accountId, result.getBalance());
            }
        });
        return converted;
    }

    /**
     * Reconcile in-memory balances with database
     */
    public CompletableFuture<ReconciliationResult> reconcileBalances(Set<UUID> accountIds) {
        return getBalances(accountIds)
            .thenCompose(memoryBalances -> {
                return persistenceManager.getCurrentBalances(accountIds)
                    .thenApply(dbResults -> {
                        List<BalanceDiscrepancy> discrepancies = new ArrayList<>();

                        for (UUID accountId : accountIds) {
                            Long memoryBalance = memoryBalances.get(accountId);
                            ImmutablePersistenceTransactionManager.BalanceResult dbResult = dbResults.get(accountId);

                            if (memoryBalance != null && dbResult != null && !dbResult.hasError()) {
                                long dbBalance = dbResult.getBalance();
                                if (!memoryBalance.equals(dbBalance)) {
                                    discrepancies.add(new BalanceDiscrepancy(
                                        accountId, memoryBalance, dbBalance, memoryBalance - dbBalance));
                                }
                            }
                        }

                        return new ReconciliationResult(accountIds.size(), discrepancies);
                    });
            });
    }

    private void logStatistics() {
        try {
            Map<String, Object> stats = getSystemStatistics();
            log.info("Integrated Ledger Performance: total_ops={}, success_rate={:.2f}%, stats={}",
                totalOperations.get(), calculateSuccessRate(), stats);
        } catch (Exception e) {
            log.error("Failed to log statistics", e);
        }
    }

    private void performHealthCheck() {
        try {
            boolean balanceStorageHealthy = balanceStorage.isHealthy();
            boolean walStorageHealthy = walStorage.isHealthy();

            if (!balanceStorageHealthy || !walStorageHealthy) {
                log.warn("Health check failed: balance_storage={}, wal_storage={}",
                    balanceStorageHealthy, walStorageHealthy);
            } else {
                log.debug("Health check passed for all components");
            }
        } catch (Exception e) {
            log.error("Health check failed", e);
        }
    }

    private double calculateSuccessRate() {
        long total = totalOperations.get();
        long successful = successfulOperations.get();
        return total > 0 ? (successful * 100.0) / total : 100.0;
    }

    /**
     * Enhanced system statistics combining all components
     */
    public Map<String, Object> getSystemStatistics() {
        Map<String, Object> stats = new HashMap<>();

        // Operations statistics
        stats.put("totalOperations", totalOperations.get());
        stats.put("successfulOperations", successfulOperations.get());
        stats.put("failedOperations", failedOperations.get());
        stats.put("successRatePercent", String.format("%.2f", calculateSuccessRate()));

        // Component statistics
        stats.put("balanceStorage", balanceStorage.getStatistics());
        stats.put("walStorage", walStorage.getStatistics());

        // Integration health
        stats.put("integrationHealth", Map.of(
            "balanceStorageHealthy", balanceStorage.isHealthy(),
            "walStorageHealthy", walStorage.isHealthy(),
            "persistenceHealthy", isPersistenceHealthy(),
            "overallHealthy", isSystemHealthy()
        ));

        return stats;
    }

    private boolean isPersistenceHealthy() {
        try {
            // Simple persistence health check - try to get max sequence
            persistenceManager.initializeSequenceGenerator();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Overall system health check
     */
    public boolean isSystemHealthy() {
        try {
            return balanceStorage.isHealthy() &&
                walStorage.isHealthy() &&
                isPersistenceHealthy() &&
                calculateSuccessRate() > 95.0;
        } catch (Exception e) {
            log.error("Error checking system health", e);
            return false;
        }
    }

    @Override
    public void close() {
        log.info("Shutting down IntegratedContainerOptimizedLedgerService...");

        // Stop background tasks
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(15, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Close components
        balanceStorage.close();
        walStorage.close();

        log.info("IntegratedContainerOptimizedLedgerService closed. Final stats: {}", getSystemStatistics());
    }

    // Result classes for reconciliation
    public static class ReconciliationResult {
        private final int totalAccounts;
        private final List<BalanceDiscrepancy> discrepancies;

        public ReconciliationResult(int totalAccounts, List<BalanceDiscrepancy> discrepancies) {
            this.totalAccounts = totalAccounts;
            this.discrepancies = discrepancies;
        }

        public int getTotalAccounts() { return totalAccounts; }
        public List<BalanceDiscrepancy> getDiscrepancies() { return discrepancies; }
        public int getDiscrepancyCount() { return discrepancies.size(); }
        public boolean hasDiscrepancies() { return !discrepancies.isEmpty(); }

        public double getAccuracyRate() {
            return totalAccounts > 0 ? ((totalAccounts - discrepancies.size()) * 100.0) / totalAccounts : 100.0;
        }
    }

    public static class BalanceDiscrepancy {
        private final UUID accountId;
        private final long memoryBalance;
        private final long databaseBalance;
        private final long difference;

        public BalanceDiscrepancy(UUID accountId, long memoryBalance, long databaseBalance, long difference) {
            this.accountId = accountId;
            this.memoryBalance = memoryBalance;
            this.databaseBalance = databaseBalance;
            this.difference = difference;
        }

        public UUID getAccountId() { return accountId; }
        public long getMemoryBalance() { return memoryBalance; }
        public long getDatabaseBalance() { return databaseBalance; }
        public long getDifference() { return difference; }

        @Override
        public String toString() {
            return String.format("Account %s: memory=%d, db=%d, diff=%d",
                accountId, memoryBalance, databaseBalance, difference);
        }
    }
}