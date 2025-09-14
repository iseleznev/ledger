package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor;

import com.lmax.disruptor.EventHandler;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Virtual Thread optimized WAL Event Handler
 * Uses Virtual Threads for non-blocking I/O to PostgreSQL
 */
@Slf4j
@RequiredArgsConstructor
public final class VirtualThreadLedgerWalEventHandler implements EventHandler<LedgerWalEvent> {

    private final NamedParameterJdbcTemplate namedJdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final int batchSize;
    private final long flushIntervalMs;

    // Batch management
    private final WalOperationBatch batch;
    @Getter
    private volatile long lastFlushTime;
    @Getter
    private final AtomicLong processedCount;

    // Virtual Thread Executor for async I/O
    private final ExecutorService virtualExecutor;

    public VirtualThreadLedgerWalEventHandler(
        NamedParameterJdbcTemplate namedJdbcTemplate,
        TransactionTemplate transactionTemplate,
        int batchSize,
        long flushIntervalMs) {

        this.namedJdbcTemplate = namedJdbcTemplate;
        this.transactionTemplate = transactionTemplate;
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;

        this.batch = new WalOperationBatch(batchSize);
        this.lastFlushTime = System.currentTimeMillis();
        this.processedCount = new AtomicLong(0);

        // Create Virtual Thread Executor for I/O operations
        this.virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @Override
    public void onEvent(LedgerWalEvent event, long sequence, boolean endOfBatch) throws Exception {
        // Add operation to batch
        batch.add(event);

        long currentTime = System.currentTimeMillis();
        boolean shouldFlushByTime = (currentTime - lastFlushTime) >= flushIntervalMs;
        boolean shouldFlushBySize = batch.isFull();

        // Flush if needed - delegate to Virtual Thread for non-blocking I/O
        if (shouldFlushBySize || shouldFlushByTime || endOfBatch) {
            if (!batch.isEmpty()) {
                // Create snapshot for async processing
                var operationsToFlush = batch.getOperations();
                batch.clear();
                lastFlushTime = currentTime;

                // Submit flush operation to Virtual Thread - don't block Disruptor
                virtualExecutor.submit(() -> flushBatchToWal(operationsToFlush));
            }
        }
    }

    /**
     * Flush batch to WAL table using Virtual Thread for I/O
     * This method runs in Virtual Thread context - can block on I/O
     * <p>
     * TODO: Add retry logic for failed database operations
     * TODO: Implement dead letter queue for permanently failed operations
     * TODO: Add circuit breaker pattern for database connectivity issues
     * TODO: Implement backpressure handling when database is slow
     */
    private void flushBatchToWal(List<LedgerWalEvent> operations) {
        try {
            transactionTemplate.execute(status -> {
                String sql = """
                    INSERT INTO ledger.wal_entries 
                    (sequence_number, debit_account_id, credit_account_id, amount, 
                     currency_code, operation_date, transaction_id, idempotency_key, status)
                    VALUES (:sequenceNumber, :debitAccountId, :creditAccountId, :amount,
                            :currencyCode, :operationDate, :transactionId, :idempotencyKey, :status::VARCHAR)
                    """;

                // Prepare batch parameters - optimized for large batches
                Map<String, Object>[] batchParams = operations.stream()
                    .map(this::createParameterMap)
                    .toArray(Map[]::new);

                // Execute batch insert
                namedJdbcTemplate.batchUpdate(sql, batchParams);

                int batchSize = operations.size();
                long totalProcessed = processedCount.addAndGet(batchSize);

                log.debug("WAL [VT-{}]: Flushed batch of {} operations. Total: {}",
                    Thread.currentThread().threadId(), batchSize, totalProcessed);

                return null; // TransactionCallback requires return
            });

        } catch (Exception e) {
            log.error("ERROR [VT-{}]: Failed to flush WAL batch: {}",
                Thread.currentThread().threadId(), e.getMessage(), e);

            // TODO: Implement proper error handling strategy
            // - Retry logic with exponential backoff
            // - Dead letter queue for failed operations
            // - Circuit breaker for database issues
            // - Alerting system integration
            throw new RuntimeException("WAL flush failed", e);
        }
    }

    /**
     * TODO: Optimize parameter map creation to reduce allocations
     */
    private Map<String, Object> createParameterMap(LedgerWalEvent op) {
        Map<String, Object> params = new HashMap<>(9);
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

    public void shutdown() {
        try {
            virtualExecutor.close(); // Java 21 AutoCloseable
            log.info("Virtual Thread WAL Event Handler shutdown completed");
        } catch (Exception e) {
            log.error("Error shutting down Virtual Thread WAL Event Handler", e);
        }
    }
}
