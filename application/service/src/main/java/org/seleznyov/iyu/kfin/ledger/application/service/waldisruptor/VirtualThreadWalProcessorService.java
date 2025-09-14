package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * Virtual Thread optimized WAL Processor Service
 * Processes PENDING WAL entries into ledger.entry_records
 */
@Slf4j
@RequiredArgsConstructor
public final class VirtualThreadWalProcessorService implements AutoCloseable {

    private final NamedParameterJdbcTemplate namedJdbcTemplate;
    private final TransactionTemplate transactionTemplate;

    // Virtual Thread executors
    private final ExecutorService virtualExecutor;
    private final ScheduledExecutorService scheduledExecutor;

    public VirtualThreadWalProcessorService(
        NamedParameterJdbcTemplate namedJdbcTemplate,
        TransactionTemplate transactionTemplate) {

        this.namedJdbcTemplate = namedJdbcTemplate;
        this.transactionTemplate = transactionTemplate;

        // Virtual Thread executor for parallel processing
        this.virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();

        // Scheduled executor with Virtual Thread factory
        this.scheduledExecutor = Executors.newScheduledThreadPool(1,
            Thread.ofVirtual().name("wal-processor-scheduler-").factory());
    }

    /**
     * Start automatic WAL processing with specified interval
     * <p>
     * TODO: Add configuration for retry intervals and error handling
     * TODO: Implement health check mechanism for scheduled processing
     */
    public void startScheduledProcessing(long intervalMs, int batchSize) {
        scheduledExecutor.scheduleWithFixedDelay(() -> {
            try {
                // Process in Virtual Thread for non-blocking I/O
                virtualExecutor.submit(() -> processPendingWalEntries(batchSize));
            } catch (Exception e) {
                log.error("ERROR in scheduled WAL processing", e);
                // TODO: Implement alerting mechanism for repeated failures
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);

        log.info("Started scheduled WAL processing every {}ms with batch size {}", intervalMs, batchSize);
    }

    /**
     * Process PENDING WAL entries into entry_records
     * Optimized for Virtual Threads and high throughput
     * <p>
     * TODO: Add duplicate detection mechanism
     * TODO: Implement partial batch processing on errors
     * TODO: Add processing metrics and monitoring
     */
    public int processPendingWalEntries(int batchSize) {
        try {
            return transactionTemplate.execute(status -> {
                // Get PENDING entries with row-level locking
                var pendingEntries = getPendingWalEntries(batchSize);

                if (pendingEntries.isEmpty()) {
                    return 0;
                }

                log.debug("Processing {} WAL entries in VT-{}",
                    pendingEntries.size(), Thread.currentThread().threadId());

                // Mark as PROCESSING
                markAsProcessing(pendingEntries.stream().map(WalEntryRecord::id).toList());

                try {
                    // Create double-entry records
                    insertEntryRecords(pendingEntries);

                    // Mark as PROCESSED
                    markAsProcessed(pendingEntries.stream().map(WalEntryRecord::id).toList());

                    log.debug("Processed {} WAL entries into entry_records [VT-{}]",
                        pendingEntries.size(), Thread.currentThread().threadId());

                    return pendingEntries.size();

                } catch (Exception e) {
                    // Mark as FAILED on error
                    markAsFailed(pendingEntries.stream().map(WalEntryRecord::id).toList(),
                        e.getMessage());
                    throw e;
                }
            });
        } catch (Exception e) {
            log.error("ERROR processing WAL entries", e);
            // TODO: Implement retry mechanism with exponential backoff
            return 0;
        }
    }

    /**
     * Parallel processing using multiple Virtual Threads
     * <p>
     * TODO: Add adaptive parallelism based on system load
     * TODO: Implement load balancing between Virtual Threads
     */
    public CompletableFuture<Integer> processInParallel(int parallelism, int batchSize) {
        var futures = new ArrayList<CompletableFuture<Integer>>(parallelism);

        for (int i = 0; i < parallelism; i++) {
            futures.add(CompletableFuture.supplyAsync(() ->
                processPendingWalEntries(batchSize), virtualExecutor));
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(v -> futures.stream().mapToInt(CompletableFuture::join).sum());
    }

    /**
     * TODO: Add query optimization and caching for better performance
     * TODO: Implement pagination for very large backlogs
     */
    private List<WalEntryRecord> getPendingWalEntries(int batchSize) {
        String sql = """
            SELECT id, sequence_number, debit_account_id, credit_account_id, 
                   amount, currency_code, operation_date, transaction_id, idempotency_key
            FROM ledger.wal_entries 
            WHERE status = 'PENDING'
            ORDER BY sequence_number
            LIMIT :batchSize
            FOR UPDATE SKIP LOCKED
            """;

        Map<String, Object> params = Map.of("batchSize", batchSize);

        return namedJdbcTemplate.query(sql, params, (rs, rowNum) ->
            new WalEntryRecord(
                UUID.fromString(rs.getString("id")),
                rs.getLong("sequence_number"),
                UUID.fromString(rs.getString("debit_account_id")),
                UUID.fromString(rs.getString("credit_account_id")),
                rs.getLong("amount"),
                rs.getString("currency_code"),
                rs.getDate("operation_date").toLocalDate(),
                UUID.fromString(rs.getString("transaction_id")),
                UUID.fromString(rs.getString("idempotency_key"))
            ));
    }

    /**
     * TODO: Add batch update optimization
     */
    private void markAsProcessing(List<UUID> walIds) {
        String sql = """
            UPDATE ledger.wal_entries 
            SET status = 'PROCESSING'::VARCHAR 
            WHERE id = ANY(:ids)
            """;

        namedJdbcTemplate.update(sql, Map.of("ids", walIds.toArray(new UUID[0])));
    }

    /**
     * TODO: Optimize entry record creation to reduce memory allocations
     * TODO: Add validation for double-entry balance consistency
     */
    private void insertEntryRecords(List<WalEntryRecord> walEntries) {
        var entryRecords = new ArrayList<Map<String, Object>>(walEntries.size() * 2);

        for (var walEntry : walEntries) {
            // DEBIT entry
            entryRecords.add(createEntryRecordMap(walEntry, EntryType.DEBIT, walEntry.debitAccountId()));

            // CREDIT entry
            entryRecords.add(createEntryRecordMap(walEntry, EntryType.CREDIT, walEntry.creditAccountId()));
        }

        String sql = """
            INSERT INTO ledger.entry_records 
            (account_id, transaction_id, entry_type, amount, operation_date, idempotency_key, currency_code)
            VALUES (:accountId, :transactionId, :entryType::VARCHAR, :amount, 
                    :operationDate, :idempotencyKey, :currencyCode)
            """;

        namedJdbcTemplate.batchUpdate(sql, entryRecords.toArray(new Map[0]));
    }

    /**
     * TODO: Use object pooling to reduce allocations
     */
    private Map<String, Object> createEntryRecordMap(WalEntryRecord walEntry, EntryType entryType, UUID accountId) {
        return Map.of(
            "accountId", accountId,
            "transactionId", walEntry.transactionId(),
            "entryType", entryType.name(),
            "amount", walEntry.amount(),
            "operationDate", walEntry.operationDate(),
            "idempotencyKey", walEntry.idempotencyKey(),
            "currencyCode", walEntry.currencyCode()
        );
    }

    private void markAsProcessed(List<UUID> walIds) {
        String sql = """
            UPDATE ledger.wal_entries 
            SET status = 'PROCESSED'::VARCHAR, processed_at = NOW()
            WHERE id = ANY(:ids)
            """;

        namedJdbcTemplate.update(sql, Map.of("ids", walIds.toArray(new UUID[0])));
    }

    private void markAsFailed(List<UUID> walIds, String errorMessage) {
        String sql = """
            UPDATE ledger.wal_entries 
            SET status = 'FAILED'::VARCHAR, processed_at = NOW(), error_message = :errorMessage
            WHERE id = ANY(:ids)
            """;

        namedJdbcTemplate.update(sql, Map.of(
            "ids", walIds.toArray(new UUID[0]),
            "errorMessage", errorMessage
        ));
    }

    @Override
    public void close() {
        try {
            scheduledExecutor.shutdown();
            virtualExecutor.close();
            log.info("Virtual Thread WAL Processor Service closed");
        } catch (Exception e) {
            log.error("Error closing Virtual Thread WAL Processor Service", e);
        }
    }
}
