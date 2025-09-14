package org.seleznyov.iyu.kfin.ledger.application.service.containeroptimized;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Main Container-Optimized Ledger Service
 */
@Service
@Slf4j
public class ContainerOptimizedLedgerService implements AutoCloseable {

    private final ContainerOptimizedBalanceStorage balanceStorage;
    private final ContainerOptimizedWAL walStorage;
    private final ScheduledExecutorService scheduler;

    // Performance tracking
    private final AtomicLong totalOperations = new AtomicLong(0);

    public ContainerOptimizedLedgerService() {
        this.balanceStorage = new ContainerOptimizedBalanceStorage();
        this.walStorage = new ContainerOptimizedWAL();
        this.scheduler = Executors.newScheduledThreadPool(2,
            Thread.ofVirtual().name("ledger-scheduler-").factory());

        startBackgroundTasks();
        log.info("ContainerOptimizedLedgerService started");
    }

    private void startBackgroundTasks() {
        // Periodic WAL flush
        scheduler.scheduleWithFixedDelay(this::flushWAL, 2, 2, TimeUnit.SECONDS);

        // Periodic statistics logging
        scheduler.scheduleWithFixedDelay(this::logStatistics, 30, 30, TimeUnit.SECONDS);
    }

    /**
     * Main API: Process double-entry operation
     */
    public CompletableFuture<LedgerOperationResult> processDoubleEntry(
        UUID debitAccountId, UUID creditAccountId, long amount,
        String currencyCode, DurabilityLevel durabilityLevel) {

        UUID operationId = UUID.randomUUID();

        // Log to WAL first
        CompletableFuture<Void> walFuture = walStorage.append(debitAccountId, -amount, operationId)
            .thenCompose(ignored -> walStorage.append(creditAccountId, amount, operationId));

        // Update balances
        CompletableFuture<Long> debitFuture = balanceStorage.addToBalance(debitAccountId, -amount);
        CompletableFuture<Long> creditFuture = balanceStorage.addToBalance(creditAccountId, amount);

        return CompletableFuture.allOf(walFuture, debitFuture, creditFuture)
            .thenApply(ignored -> {
                totalOperations.incrementAndGet();

                return LedgerOperationResult.builder()
                    .success(true)
                    .sequenceNumber(totalOperations.get())
                    .transactionId(operationId)
                    .processingTimeNanos(System.nanoTime()) // Simplified timing
                    .build();
            })
            .exceptionally(error -> {
                log.error("Operation failed", error);
                return LedgerOperationResult.builder()
                    .success(false)
                    .errorMessage(error.getMessage())
                    .build();
            });
    }

    public CompletableFuture<OptionalLong> getBalance(UUID accountId) {
        return balanceStorage.getBalance(accountId);
    }

    public CompletableFuture<Map<UUID, Long>> getBalances(Set<UUID> accountIds) {
        return balanceStorage.getBalances(accountIds);
    }

    private void flushWAL() {
        walStorage.getUnflushedEntries(1000)
            .whenComplete((entries, error) -> {
                if (error != null) {
                    log.error("WAL flush failed", error);
                } else if (!entries.isEmpty()) {
                    log.debug("Flushed {} WAL entries", entries.size());
                }
            });
    }

    private void logStatistics() {
        Map<String, Object> balanceStats = balanceStorage.getStatistics();
        Map<String, Object> walStats = walStorage.getStatistics();

        log.info("Ledger stats: {} total ops, balance={}, wal={}",
            totalOperations.get(), balanceStats, walStats);
    }

    public Map<String, Object> getSystemStatistics() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalOperations", totalOperations.get());
        stats.put("balanceStorage", balanceStorage.getStatistics());
        stats.put("walStorage", walStorage.getStatistics());
        return stats;
    }

    @Override
    public void close() {
        log.info("Shutting down ContainerOptimizedLedgerService...");
        scheduler.shutdown();
        balanceStorage.close();
        log.info("ContainerOptimizedLedgerService closed");
    }
}
