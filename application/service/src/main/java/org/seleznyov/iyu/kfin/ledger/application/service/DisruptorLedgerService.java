package org.seleznyov.iyu.kfin.ledger.application.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.domain.model.ProcessingStrategy;
import org.seleznyov.iyu.kfin.ledger.infrastructure.disruptor.DisruptorManager;
import org.seleznyov.iyu.kfin.ledger.infrastructure.disruptor.LedgerEventHandler;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class DisruptorLedgerService {

    private final DisruptorManager disruptorManager;
    private final AdaptiveStrategyService adaptiveStrategyService;
    private final LedgerEventHandler eventHandler;

    // Timeout for async operations (configurable)
    private static final long OPERATION_TIMEOUT_MS = 5000; // 5 seconds
    private static final long BALANCE_QUERY_TIMEOUT_MS = 1000; // 1 second for balance queries

    public void processTransfer(
        UUID debitAccountId,
        UUID creditAccountId,
        long amount,
        String currencyCode,
        LocalDate operationDate,
        UUID transactionId,
        UUID idempotencyKey
    ) {
        log.debug("Processing disruptor transfer: {} -> {}, amount: {}, tx: {}",
            debitAccountId, creditAccountId, amount, transactionId);

        // Ensure both accounts are managed by disruptor
        disruptorManager.addManagedAccount(debitAccountId);
        disruptorManager.addManagedAccount(creditAccountId);

        // Record transactions for ML learning and validation
        adaptiveStrategyService.recordTransaction(debitAccountId, amount, true);  // debit
        adaptiveStrategyService.recordTransaction(creditAccountId, amount, false); // credit

        try {
            // Publish to disruptor ring buffer
            CompletableFuture<DisruptorManager.TransferResult> resultFuture = disruptorManager.publishTransfer(
                debitAccountId,
                creditAccountId,
                amount,
                currencyCode,
                operationDate,
                transactionId,
                idempotencyKey
            );

            // Wait for processing completion with timeout
            DisruptorManager.TransferResult result = resultFuture.get(OPERATION_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            if (!result.success()) {
                log.warn("Disruptor transfer failed: tx={}, message={}", transactionId, result.message());
                handleTransferFailure(debitAccountId, result.message(), transactionId);
                throw new TransferProcessingException(result.message());
            }

            log.info("Disruptor transfer completed successfully: {} -> {}, amount: {}, tx: {}, time: {}ms",
                debitAccountId, creditAccountId, amount, transactionId, result.processingTimeMs());

        } catch (java.util.concurrent.TimeoutException e) {
            log.error("Disruptor transfer timeout: tx={}", transactionId);
            handleTransferTimeout(debitAccountId, transactionId);
            throw new TransferTimeoutException("Transfer processing timeout: " + transactionId);

        } catch (java.util.concurrent.ExecutionException e) {
            log.error("Disruptor transfer execution error: tx={}, error={}",
                transactionId, e.getCause().getMessage());
            handleExecutionError(debitAccountId, e.getCause().getMessage(), transactionId);
            throw new TransferProcessingException("Transfer processing failed: " + e.getCause().getMessage());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Disruptor transfer interrupted: tx={}", transactionId);
            throw new TransferProcessingException("Transfer processing interrupted");

        } catch (TransferProcessingException | TransferTimeoutException e) {
            throw e; // Re-throw known exceptions

        } catch (Exception e) {
            log.error("Unexpected error in disruptor transfer: tx={}, error={}",
                transactionId, e.getMessage(), e);
            throw new TransferProcessingException("Unexpected transfer error: " + e.getMessage());
        }
    }

    public long getBalance(UUID accountId, LocalDate operationDate) {
        log.debug("Getting balance for hot account: {}, date: {}", accountId, operationDate);

        try {
            // Create balance inquiry event
            UUID queryId = UUID.randomUUID();

            // Publish balance inquiry to disruptor
            CompletableFuture<DisruptorManager.TransferResult> balanceQuery =
                publishBalanceInquiry(accountId, operationDate, queryId);

            // Wait for result with shorter timeout (balance queries should be fast)
            DisruptorManager.TransferResult result = balanceQuery.get(BALANCE_QUERY_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            if (!result.success()) {
                log.warn("Balance query failed: account={}, error={}", accountId, result.message());
                throw new BalanceQueryException("Failed to get balance: " + result.message());
            }

            // Parse balance from result message (this is a simplified approach)
            // In real implementation, we'd have a specific balance result type
            long balance = parseBalanceFromResult(result);

            log.debug("Balance retrieved for hot account {}: {}", accountId, balance);
            return balance;

        } catch (java.util.concurrent.TimeoutException e) {
            log.error("Balance query timeout: account={}", accountId);
            throw new BalanceQueryException("Balance query timeout for account: " + accountId);

        } catch (java.util.concurrent.ExecutionException e) {
            log.error("Balance query execution error: account={}, error={}", accountId, e.getCause().getMessage());
            throw new BalanceQueryException("Balance query failed: " + e.getCause().getMessage());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Balance query interrupted: account={}", accountId);
            throw new BalanceQueryException("Balance query interrupted");

        } catch (BalanceQueryException e) {
            throw e; // Re-throw known exceptions

        } catch (Exception e) {
            log.error("Unexpected error in balance query: account={}, error={}", accountId, e.getMessage(), e);
            throw new BalanceQueryException("Unexpected balance query error: " + e.getMessage());
        }
    }

    public boolean canProcessAccount(UUID accountId) {
        try {
            // Delegate to adaptive strategy service for ML-based decision
            ProcessingStrategy strategy = adaptiveStrategyService.selectStrategyForBalance(accountId);
            boolean canProcess = (strategy == ProcessingStrategy.DISRUPTOR);

            log.debug("Account {} can be processed by disruptor: {}", accountId, canProcess);
            return canProcess;

        } catch (Exception e) {
            log.warn("Error checking if account can be processed by disruptor: {}", e.getMessage());
            return false; // Fail safe
        }
    }

    public void validateAccountPerformance(UUID accountId, String observedPerformance) {
        log.debug("Validating performance for hot account: {}, performance: {}",
            accountId, observedPerformance);

        // Only report as false positive if there's clear evidence of poor fit
        if (isPerformancePoor(observedPerformance)) {
            log.info("Reporting false positive for account {} due to poor performance: {}",
                accountId, observedPerformance);
            adaptiveStrategyService.reportFalsePositive(accountId, observedPerformance);
        }
    }

    public ProcessingStats getProcessingStats() {
        var disruptorStats = disruptorManager.getStats();
        var handlerStats = eventHandler.getProcessingStats();

        return new ProcessingStats(
            "DISRUPTOR",
            handlerStats.processedEvents(),
            0, // failed count - would need separate tracking
            0.0, // average processing time - would need metrics collection
            disruptorStats.bufferSize(),
            disruptorStats.usedCapacity(),
            disruptorStats.pendingOperations(),
            disruptorStats.managedAccounts()
        );
    }

    public void shutdown() {
        log.info("Shutting down DisruptorLedgerService");
        // DisruptorManager handles shutdown in its @PreDestroy method
    }

    private CompletableFuture<DisruptorManager.TransferResult> publishBalanceInquiry(
        UUID accountId, LocalDate operationDate, UUID queryId) {

        // This is a simplified implementation - in reality we'd need a specific balance query event
        // For now, we'll create a mock balance inquiry
        CompletableFuture<DisruptorManager.TransferResult> mockResult = new CompletableFuture<>();

        // In real implementation, this would publish a BALANCE_INQUIRY event to disruptor
        // and the event handler would process it and return cached balance
        CompletableFuture.runAsync(() -> {
            try {
                // Simulate getting balance from event handler's cache
                // This is where we'd actually query the in-memory balance state
                mockResult.complete(new DisruptorManager.TransferResult(
                    queryId, true, "Balance: 0", System.currentTimeMillis()
                ));
            } catch (Exception e) {
                mockResult.completeExceptionally(e);
            }
        });

        return mockResult;
    }

    private long parseBalanceFromResult(DisruptorManager.TransferResult result) {
        try {
            // Parse balance from result message
            // This is simplified - in real implementation we'd have structured result
            String message = result.message();
            if (message.startsWith("Balance: ")) {
                return Long.parseLong(message.substring(9));
            }
            throw new BalanceQueryException("Invalid balance result format: " + message);
        } catch (NumberFormatException e) {
            throw new BalanceQueryException("Failed to parse balance from result");
        }
    }

    private void handleTransferFailure(UUID debitAccountId, String errorMessage, UUID transactionId) {
        // Only report false positives for specific error types that indicate account shouldn't be hot
        if (errorMessage.contains("Insufficient balance")) {
            // This is a legitimate business error, not a false positive
            log.debug("Transfer failed due to insufficient balance - not reporting false positive");
            return;
        }

        if (errorMessage.contains("timeout") || errorMessage.contains("deadlock") ||
            errorMessage.contains("contention")) {
            log.warn("Transfer failed due to performance issue - potential false positive: account={}, tx={}",
                debitAccountId, transactionId);
            adaptiveStrategyService.reportFalsePositive(debitAccountId,
                "Transfer failed: " + errorMessage);
        }
    }

    private void handleTransferTimeout(UUID debitAccountId, UUID transactionId) {
        log.warn("Transfer timeout - potential false positive: account={}, tx={}", debitAccountId, transactionId);
        adaptiveStrategyService.reportFalsePositive(debitAccountId, "Disruptor processing timeout");
    }

    private void handleExecutionError(UUID debitAccountId, String errorMessage, UUID transactionId) {
        // Analyze error type before reporting false positive
        if (errorMessage.contains("balance") || errorMessage.contains("validation")) {
            // Business logic errors, not performance issues
            return;
        }

        if (errorMessage.contains("lock") || errorMessage.contains("contention") ||
            errorMessage.contains("resource")) {
            log.warn("Transfer execution error - potential false positive: account={}, tx={}, error={}",
                debitAccountId, transactionId, errorMessage);
            adaptiveStrategyService.reportFalsePositive(debitAccountId,
                "Execution error: " + errorMessage);
        }
    }

    private boolean isPerformancePoor(String observedPerformance) {
        return observedPerformance.contains("slow") ||
            observedPerformance.contains("timeout") ||
            observedPerformance.contains("low throughput") ||
            observedPerformance.contains("high latency") ||
            observedPerformance.contains("contention");
    }

    public static class TransferProcessingException extends RuntimeException {
        public TransferProcessingException(String message) {
            super(message);
        }
    }

    public static class TransferTimeoutException extends RuntimeException {
        public TransferTimeoutException(String message) {
            super(message);
        }
    }

    public static class BalanceQueryException extends RuntimeException {
        public BalanceQueryException(String message) {
            super(message);
        }
    }

    public record ProcessingStats(
        String strategy,
        long processedCount,
        long failedCount,
        double averageProcessingTimeMs,
        long ringBufferSize,
        long ringBufferUsed,
        long pendingOperations,
        long managedAccounts
    ) {}
}