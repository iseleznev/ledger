package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * High-performance Ledger WAL Service using Virtual Threads and Disruptor
 * Optimized for Java 21 Virtual Threads and prepared for Chronicle integration
 */
@Slf4j
@RequiredArgsConstructor
public final class VirtualThreadLedgerWalService implements AutoCloseable {

    private final NamedParameterJdbcTemplate namedJdbcTemplate;
    @Getter
    private final AtomicLong sequenceGenerator;

    // Disruptor components
    @Getter
    private final Disruptor<LedgerWalEvent> disruptor;
    @Getter
    private final VirtualThreadLedgerWalEventHandler eventHandler;
    @Getter
    private final RingBuffer<LedgerWalEvent> ringBuffer;

    public VirtualThreadLedgerWalService(
        DataSource dataSource,
        TransactionTemplate transactionTemplate,
        int ringBufferSize, // Must be power of 2
        int batchSize,
        long flushIntervalMs) {

        this.namedJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        this.sequenceGenerator = new AtomicLong(0);

        // Create Virtual Thread Factory for Disruptor
        ThreadFactory virtualThreadFactory = Thread.ofVirtual()
            .name("ledger-wal-disruptor-", 0)
            .uncaughtExceptionHandler((t, e) ->
                log.error("FATAL: Uncaught exception in Virtual Thread {}: {}", t.getName(), e.getMessage(), e))
            .factory();

        // Initialize event handler with Virtual Thread support
        this.eventHandler = new VirtualThreadLedgerWalEventHandler(
            namedJdbcTemplate, transactionTemplate, batchSize, flushIntervalMs);

        // Create Disruptor with optimized settings for Virtual Threads
        this.disruptor = new Disruptor<>(
            new LedgerWalEventFactory(),
            ringBufferSize,
            virtualThreadFactory,
            ProducerType.MULTI, // Support multiple producers
            new YieldingWaitStrategy() // Better for Virtual Threads than BlockingWaitStrategy
        );

        // Register event handler
        disruptor.handleEventsWith(eventHandler);

        // Start Disruptor
        disruptor.start();
        this.ringBuffer = disruptor.getRingBuffer();

        log.info("Virtual Thread Ledger WAL Service started:");
        log.info("  Ring buffer size: {}", ringBufferSize);
        log.info("  Batch size: {}", batchSize);
        log.info("  Flush interval: {}ms", flushIntervalMs);
        log.info("  Running in Virtual Thread: {}", Thread.currentThread().isVirtual());
    }

    /**
     * Write double-entry operation to WAL with maximum performance
     * This method is lock-free and optimized for high throughput
     * <p>
     * TODO: Add input validation for account IDs and amounts
     * TODO: Implement idempotency check before writing to ring buffer
     * TODO: Add metrics collection for operation latency
     */
    public long writeDoubleEntry(
        UUID debitAccountId,
        UUID creditAccountId,
        long amount,
        String currencyCode,
        LocalDate operationDate,
        UUID transactionId,
        UUID idempotencyKey) {

        // TODO: Add validation
        // validateInputs(debitAccountId, creditAccountId, amount);

        // Get next sequence from ring buffer
        long sequence = ringBuffer.next();

        try {
            // Get pre-allocated event from ring buffer
            LedgerWalEvent event = ringBuffer.get(sequence);
            long sequenceNumber = sequenceGenerator.incrementAndGet();

            // Populate event (object reuse for zero-GC)
            event.setSequenceNumber(sequenceNumber);
            event.setDebitAccountId(debitAccountId);
            event.setCreditAccountId(creditAccountId);
            event.setAmount(amount);
            event.setCurrencyCode(currencyCode);
            event.setOperationDate(operationDate);
            event.setTransactionId(transactionId);
            event.setIdempotencyKey(idempotencyKey);
            event.setStatus(WalStatus.PENDING);

            return sequenceNumber;

        } finally {
            // Publish event to ring buffer
            ringBuffer.publish(sequence);
        }
    }

    /**
     * Convenience method with defaults
     */
    public long writeDoubleEntry(UUID debitAccountId, UUID creditAccountId, long amount) {
        return writeDoubleEntry(
            debitAccountId,
            creditAccountId,
            amount,
            "RUB",
            LocalDate.now(),
            UUID.randomUUID(),
            UUID.randomUUID()
        );
    }

    /**
     * Asynchronous batch operations using Virtual Threads
     * <p>
     * TODO: Add batch size limits and validation
     * TODO: Implement batch operation metrics
     */
    public CompletableFuture<List<Long>> writeDoubleEntriesAsync(List<DoubleEntryRequest> operations) {
        return CompletableFuture.supplyAsync(() -> {
            return operations.stream()
                .mapToLong(req -> writeDoubleEntry(
                    req.debitAccountId(),
                    req.creditAccountId(),
                    req.amount(),
                    req.currencyCode(),
                    req.operationDate(),
                    req.transactionId(),
                    req.idempotencyKey()
                ))
                .boxed()
                .toList();
        }, Executors.newVirtualThreadPerTaskExecutor());
    }

    /**
     * Synchronous batch operations
     */
    public List<Long> writeDoubleEntries(List<DoubleEntryRequest> operations) {
        return operations.stream()
            .mapToLong(req -> writeDoubleEntry(
                req.debitAccountId(),
                req.creditAccountId(),
                req.amount(),
                req.currencyCode(),
                req.operationDate(),
                req.transactionId(),
                req.idempotencyKey()
            ))
            .boxed()
            .toList();
    }

    // Monitoring and statistics
    public long getProcessedCount() {
        return eventHandler.getProcessedCount();
    }

    public long getRingBufferSize() {
        return ringBuffer.getBufferSize();
    }

    public long getRingBufferRemaining() {
        return ringBuffer.remainingCapacity();
    }

    public boolean isVirtualThread() {
        return Thread.currentThread().isVirtual();
    }

    /**
     * TODO: Implement graceful shutdown with proper cleanup sequence
     * TODO: Add shutdown timeout configuration
     * TODO: Implement forced shutdown if graceful shutdown fails
     */
    @Override
    public void close() {
        try {
            log.info("Shutting down Virtual Thread Ledger WAL Service...");

            // Allow time for remaining events to process
            Thread.sleep(200);

            // Shutdown event handler (closes Virtual Thread executor)
            eventHandler.shutdown();

            // Shutdown Disruptor
            disruptor.shutdown(5, TimeUnit.SECONDS);

            log.info("Virtual Thread Ledger WAL Service shutdown completed. Total processed: {}",
                getProcessedCount());
        } catch (Exception e) {
            log.error("ERROR during Virtual Thread WAL service shutdown", e);
        }
    }
}
