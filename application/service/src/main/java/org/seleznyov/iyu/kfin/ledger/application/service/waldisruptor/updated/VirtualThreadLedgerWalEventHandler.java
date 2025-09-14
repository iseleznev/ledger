package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor.updated;

import com.lmax.disruptor.EventHandler;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Production-ready WAL Event Handler with comprehensive error handling
 */
@Slf4j
@RequiredArgsConstructor
public final class VirtualThreadLedgerWalEventHandler implements EventHandler<LedgerWalEvent> {

    private final NamedParameterJdbcTemplate namedJdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final LedgerWalConfiguration config;
    private final LedgerWalMetrics metrics;
    private final ParameterMapPool parameterMapPool;

    private final WalOperationBatch batch;
    @Getter
    private volatile long lastFlushTime;
    @Getter
    private final AtomicLong processedCount;
    private final ExecutorService virtualExecutor;

    // Circuit breaker state
    private final AtomicLong consecutiveFailures = new AtomicLong(0);
    private volatile boolean circuitOpen = false;
    private volatile long circuitOpenTime = 0;

    public VirtualThreadLedgerWalEventHandler(
        NamedParameterJdbcTemplate namedJdbcTemplate,
        TransactionTemplate transactionTemplate,
        LedgerWalConfiguration config,
        LedgerWalMetrics metrics,
        ParameterMapPool parameterMapPool) {

        this.namedJdbcTemplate = namedJdbcTemplate;
        this.transactionTemplate = transactionTemplate;
        this.config = config;
        this.metrics = metrics;
        this.parameterMapPool = parameterMapPool;

        this.batch = new WalOperationBatch(config.getBatchSize());
        this.lastFlushTime = System.currentTimeMillis();
        this.processedCount = new AtomicLong(0);
        this.virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @Override
    public void onEvent(LedgerWalEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (isCircuitOpen()) {
            log.warn("Circuit breaker is open, dropping event: {}", event.getSequenceNumber());
            metrics.recordError("circuit_open");
            return;
        }

        batch.add(event);

        long currentTime = System.currentTimeMillis();
        boolean shouldFlushByTime = (currentTime - lastFlushTime) >= config.getFlushIntervalMs();
        boolean shouldFlushBySize = batch.isFull();

        if (shouldFlushBySize || shouldFlushByTime || endOfBatch) {
            if (!batch.isEmpty()) {
                var operationsToFlush = batch.getOperations();
                batch.clear();
                lastFlushTime = currentTime;

                virtualExecutor.submit(() -> flushBatchToWalWithRetry(operationsToFlush));
            }
        }
    }

    private void flushBatchToWalWithRetry(List<LedgerWalEvent> operations) {
        var startTime = Instant.now();

        for (int attempt = 1; attempt <= config.getMaxRetryAttempts(); attempt++) {
            try {
                flushBatchToWal(operations);

                // Reset circuit breaker on success
                consecutiveFailures.set(0);
                circuitOpen = false;

                var duration = Duration.between(startTime, Instant.now());
                metrics.recordFlush(operations.size(), duration);
                return;

            } catch (Exception e) {
                log.warn("WAL flush attempt {} failed: {}", attempt, e.getMessage());
                metrics.recordError("flush_failure");

                long failures = consecutiveFailures.incrementAndGet();

                if (attempt == config.getMaxRetryAttempts()) {
                    if (failures >= config.getCircuitBreakerFailureThreshold()) {
                        circuitOpen = true;
                        circuitOpenTime = System.currentTimeMillis();
                        log.error("Circuit breaker opened after {} consecutive failures", failures);
                        metrics.recordError("circuit_opened");
                    }

                    log.error("Failed to flush WAL batch after {} attempts", config.getMaxRetryAttempts(), e);
                    throw new RuntimeException("WAL flush failed after retries", e);
                }

                try {
                    Thread.sleep(config.getRetryBaseDelayMs() * (1L << (attempt - 1)));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during retry backoff", ie);
                }
            }
        }
    }

    private boolean isCircuitOpen() {
        if (!circuitOpen) return false;

        if (System.currentTimeMillis() - circuitOpenTime > config.getCircuitBreakerTimeoutMs()) {
            circuitOpen = false;
            consecutiveFailures.set(0);
            log.info("Circuit breaker reset after timeout");
            return false;
        }

        return true;
    }

    private void flushBatchToWal(List<LedgerWalEvent> operations) {
        transactionTemplate.execute(status -> {
            String sql = """
                INSERT INTO ledger.wal_entries 
                (sequence_number, debit_account_id, credit_account_id, amount, 
                 currency_code, operation_date, transaction_id, idempotency_key, status)
                VALUES (:sequenceNumber, :debitAccountId, :creditAccountId, :amount,
                        :currencyCode, :operationDate, :transactionId, :idempotencyKey, :status::VARCHAR)
                ON CONFLICT (idempotency_key) DO NOTHING
                """;

            // Use object pooling for parameters
            Map<String, Object>[] batchParams = operations.stream()
                .map(this::createParameterMapFromPool)
                .toArray(Map[]::new);

            try {
                int[] updateCounts = namedJdbcTemplate.batchUpdate(sql, batchParams);
                int actualInserts = Arrays.stream(updateCounts).sum();
                long totalProcessed = processedCount.addAndGet(actualInserts);

                log.debug("WAL [VT-{}]: Flushed batch of {} operations ({} actual inserts). Total: {}",
                    Thread.currentThread().threadId(), operations.size(), actualInserts, totalProcessed);

            } finally {
                // Return maps to pool
                for (Map<String, Object> params : batchParams) {
                    parameterMapPool.release(params);
                }
            }

            return null;
        });
    }

    private Map<String, Object> createParameterMapFromPool(LedgerWalEvent op) {
        Map<String, Object> params = parameterMapPool.acquire();
        params.put("sequenceNumber", op.getSequenceNumber());
        params.put("debitAccountId", op.getDebitAccountId());
        params.put("creditAccountId", op.getCreditAccountId());
        params.put("amount", op.getAmount());
        params.put("currencyCode", op.getCurrencyCode());
        params.put("operationDate", op.getOperationDate());
        params.put("transactionId", op.getTransactionId());
        params.put("idempotencyKey", op.getIdempotencyKey());
        params.put("status", op.getStatus().name());
        return params;
    }

    public long getProcessedCount() {
        return processedCount.get();
    }

    public boolean isCircuitBreakerOpen() {
        return circuitOpen;
    }

    public long getConsecutiveFailures() {
        return consecutiveFailures.get();
    }

    public void shutdown() {
        try {
            virtualExecutor.close();
            log.info("Virtual Thread WAL Event Handler shutdown completed");
        } catch (Exception e) {
            log.error("Error shutting down Virtual Thread WAL Event Handler", e);
        }
    }
}
