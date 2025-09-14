package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor.updated;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Production-ready WAL Processor Service with comprehensive monitoring
 */
@Service
@Slf4j
public final class VirtualThreadWalProcessorService implements AutoCloseable {

    private final NamedParameterJdbcTemplate namedJdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final LedgerWalConfiguration config;
    private final ParameterMapPool parameterMapPool;

    private final ExecutorService virtualExecutor;
    private final ScheduledExecutorService scheduledExecutor;

    // Processing metrics
    private final AtomicLong totalProcessed = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);
    private final AtomicLong lastProcessingTime = new AtomicLong(System.currentTimeMillis());

    public VirtualThreadWalProcessorService(
        NamedParameterJdbcTemplate namedJdbcTemplate,
        TransactionTemplate transactionTemplate,
        LedgerWalConfiguration config,
        ParameterMapPool parameterMapPool) {

        this.namedJdbcTemplate = namedJdbcTemplate;
        this.transactionTemplate = transactionTemplate;
        this.config = config;
        this.parameterMapPool = parameterMapPool;

        this.virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.scheduledExecutor = Executors.newScheduledThreadPool(2,
            Thread.ofVirtual().name("wal-processor-scheduler-").factory());
    }

    /**
     * Start automatic WAL processing with health monitoring
     */
    public void startScheduledProcessing() {
        // Main processing task
        scheduledExecutor.scheduleWithFixedDelay(() -> {
            try {
                virtualExecutor.submit(() -> {
                    try {
                        int processed = processPendingWalEntries(config.getBatchSize());
                        lastProcessingTime.set(System.currentTimeMillis());

                        if (processed > 0) {
                            log.debug("Scheduled processing: {} entries processed", processed);
                        }
                    } catch (Exception e) {
                        totalErrors.incrementAndGet();
                        log.error("ERROR in scheduled WAL processing", e);

                        // Health monitoring
                        monitorErrorRate();
                    }
                });
            } catch (Exception e) {
                log.error("CRITICAL: Failed to submit WAL processing task", e);
            }
        }, config.getProcessorScheduledIntervalMs(), config.getProcessorScheduledIntervalMs(), TimeUnit.MILLISECONDS);

        // Health monitoring task
        scheduledExecutor.scheduleWithFixedDelay(() -> {
            long timeSinceLastProcessing = System.currentTimeMillis() - lastProcessingTime.get();
            if (timeSinceLastProcessing > config.getProcessorScheduledIntervalMs() * 10) {
                log.warn("WAL processing may be stuck. Last processing: {} ms ago", timeSinceLastProcessing);
            }
        }, 30_000L, 30_000L, TimeUnit.MILLISECONDS);

        log.info("Started scheduled WAL processing every {}ms with {} parallel workers",
            config.getProcessorScheduledIntervalMs(), config.getProcessorParallelWorkers());
    }

    private void monitorErrorRate() {
        long errorRate = totalErrors.get();
        long processedRate = totalProcessed.get();
        if (processedRate > 0 && (errorRate * 100 / processedRate) > 10) {
            log.warn("High error rate detected: {}% errors ({}/{})",
                errorRate * 100 / processedRate, errorRate, processedRate);
        }
    }

    /**
     * Enhanced WAL processing with better error handling and monitoring
     */
    public int processPendingWalEntries(int batchSize) {
        Timer.Sample timer = Timer.start();

        try {
            Integer result = transactionTemplate.execute(status -> {
                var pendingEntries = getPendingWalEntries(batchSize);

                if (pendingEntries.isEmpty()) {
                    return 0;
                }

                log.debug("Processing {} WAL entries in VT-{}",
                    pendingEntries.size(), Thread.currentThread().threadId());

                try {
                    // Mark as PROCESSING first
                    markAsProcessing(pendingEntries.stream().map(WalEntryRecord::id).toList());

                    // Validate entries before processing
                    validateWalEntries(pendingEntries);

                    // Create double-entry records
                    insertEntryRecords(pendingEntries);

                    // Mark as PROCESSED
                    markAsProcessed(pendingEntries.stream().map(WalEntryRecord::id).toList());

                    long totalProcessedSoFar = totalProcessed.addAndGet(pendingEntries.size());
                    log.debug("Processed {} WAL entries into entry_records [VT-{}]. Total: {}",
                        pendingEntries.size(), Thread.currentThread().threadId(), totalProcessedSoFar);

                    return pendingEntries.size();

                } catch (Exception e) {
                    log.error("Error processing WAL batch of {} entries", pendingEntries.size(), e);
                    markAsFailed(pendingEntries.stream().map(WalEntryRecord::id).toList(),
                        e.getMessage());
                    totalErrors.incrementAndGet();
                    throw e;
                }
            });

            return result != null ? result : 0;

        } catch (Exception e) {
            log.error("CRITICAL: Transaction failed during WAL processing", e);
            totalErrors.incrementAndGet();
            return 0;
        } finally {
            timer.stop();
        }
    }

    /**
     * Validate WAL entries for business rule consistency
     */
    private void validateWalEntries(List<WalEntryRecord> entries) {
        for (var entry : entries) {
            if (entry.debitAccountId().equals(entry.creditAccountId())) {
                throw new IllegalStateException("Invalid WAL entry: debit and credit accounts are the same for sequence " + entry.sequenceNumber());
            }

            if (entry.amount() <= 0) {
                throw new IllegalStateException("Invalid WAL entry: amount must be positive for sequence " + entry.sequenceNumber());
            }

            if (entry.operationDate().isAfter(LocalDate.now())) {
                throw new IllegalStateException("Invalid WAL entry: operation date in future for sequence " + entry.sequenceNumber());
            }
        }
    }

    /**
     * Parallel processing with configurable parallelism
     */
    public CompletableFuture<Integer> processInParallel(int batchSize) {
        int parallelism = config.getProcessorParallelWorkers();
        var futures = new ArrayList<CompletableFuture<Integer>>(parallelism);

        for (int i = 0; i < parallelism; i++) {
            futures.add(CompletableFuture.supplyAsync(() ->
                processPendingWalEntries(batchSize), virtualExecutor));
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(v -> futures.stream().mapToInt(CompletableFuture::join).sum());
    }

    /**
     * Get pending WAL entries with optimized query and proper error handling
     */
    private List<WalEntryRecord> getPendingWalEntries(int batchSize) {
        try {
            String sql = """
                SELECT id, sequence_number, debit_account_id, credit_account_id, 
                       amount, currency_code, operation_date, transaction_id, idempotency_key
                FROM ledger.wal_entries 
                WHERE status = 'PENDING'
                ORDER BY sequence_number
                LIMIT :batchSize
                FOR UPDATE SKIP LOCKED
                """;

            Map<String, Object> params = parameterMapPool.acquire();
            try {
                params.put("batchSize", batchSize);

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
            } finally {
                parameterMapPool.release(params);
            }

        } catch (Exception e) {
            log.error("Error fetching pending WAL entries", e);
            throw e;
        }
    }

    private void markAsProcessing(List<UUID> walIds) {
        String sql = """
            UPDATE ledger.wal_entries 
            SET status = 'PROCESSING'::VARCHAR 
            WHERE id = ANY(:ids)
            """;

        Map<String, Object> params = parameterMapPool.acquire();
        try {
            params.put("ids", walIds.toArray(new UUID[0]));
            namedJdbcTemplate.update(sql, params);
        } finally {
            parameterMapPool.release(params);
        }
    }

    /**
     * Enhanced entry records insertion with object pooling and duplicate detection
     */
    private void insertEntryRecords(List<WalEntryRecord> walEntries) {
        var entryRecords = new ArrayList<Map<String, Object>>(walEntries.size() * 2);

        try {
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
                ON CONFLICT (idempotency_key, account_id, entry_type) DO NOTHING
                """;

            int[] results = namedJdbcTemplate.batchUpdate(sql, entryRecords.toArray(new Map[0]));

            int actualInserts = Arrays.stream(results).sum();
            log.debug("Attempted {} entry record inserts, {} successful", entryRecords.size(), actualInserts);

        } finally {
            // Return all maps to pool
            for (Map<String, Object> params : entryRecords) {
                parameterMapPool.release(params);
            }
        }
    }

    private Map<String, Object> createEntryRecordMap(WalEntryRecord walEntry, EntryType entryType, UUID accountId) {
        Map<String, Object> params = parameterMapPool.acquire();
        params.put("accountId", accountId);
        params.put("transactionId", walEntry.transactionId());
        params.put("entryType", entryType.name());
        params.put("amount", walEntry.amount());
        params.put("operationDate", walEntry.operationDate());
        params.put("idempotencyKey", walEntry.idempotencyKey());
        params.put("currencyCode", walEntry.currencyCode());
        return params;
    }

    private void markAsProcessed(List<UUID> walIds) {
        String sql = """
            UPDATE ledger.wal_entries 
            SET status = 'PROCESSED'::VARCHAR, processed_at = NOW()
            WHERE id = ANY(:ids)
            """;

        Map<String, Object> params = parameterMapPool.acquire();
        try {
            params.put("ids", walIds.toArray(new UUID[0]));
            namedJdbcTemplate.update(sql, params);
        } finally {
            parameterMapPool.release(params);
        }
    }

    private void markAsFailed(List<UUID> walIds, String errorMessage) {
        String sql = """
            UPDATE ledger.wal_entries 
            SET status = 'FAILED'::VARCHAR, processed_at = NOW(), error_message = :errorMessage
            WHERE id = ANY(:ids)
            """;

        Map<String, Object> params = parameterMapPool.acquire();
        try {
            params.put("ids", walIds.toArray(new UUID[0]));
            params.put("errorMessage", errorMessage.substring(0, Math.min(errorMessage.length(), 1000)));
            namedJdbcTemplate.update(sql, params);
        } finally {
            parameterMapPool.release(params);
        }
    }

    // Monitoring methods
    public long getTotalProcessed() {
        return totalProcessed.get();
    }

    public long getTotalErrors() {
        return totalErrors.get();
    }

    public double getErrorRate() {
        long processed = totalProcessed.get();
        return processed > 0 ? (double) totalErrors.get() / processed * 100.0 : 0.0;
    }

    public long getLastProcessingTime() {
        return lastProcessingTime.get();
    }

    @Override
    public void close() {
        try {
            log.info("Shutting down Virtual Thread WAL Processor Service...");

            scheduledExecutor.shutdown();
            if (!scheduledExecutor.awaitTermination(config.getShutdownTimeoutSeconds(), TimeUnit.SECONDS)) {
                log.warn("Scheduled executor did not shutdown gracefully, forcing shutdown");
                scheduledExecutor.shutdownNow();
            }

            virtualExecutor.close();

            log.info("Virtual Thread WAL Processor Service closed. Final stats: {} processed, {} errors ({:.2f}% error rate)",
                getTotalProcessed(), getTotalErrors(), getErrorRate());
        } catch (Exception e) {
            log.error("Error closing Virtual Thread WAL Processor Service", e);
        }
    }
}
