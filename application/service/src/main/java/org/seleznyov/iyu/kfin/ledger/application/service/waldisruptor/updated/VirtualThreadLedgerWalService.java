package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor.updated;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.validation.annotation.Validated;

import javax.sql.DataSource;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Production-ready WAL Service with proper Spring integration
 */
@Service
@Slf4j
@Validated
public final class VirtualThreadLedgerWalService implements AutoCloseable {

    private final NamedParameterJdbcTemplate namedJdbcTemplate;
    private final LedgerWalConfiguration config;
    private final LedgerWalMetrics metrics;
    @Getter
    private final AtomicLong sequenceGenerator;

    // Idempotency cache with proper size management
    private final Map<UUID, Long> idempotencyCache;

    @Getter
    private final Disruptor<LedgerWalEvent> disruptor;
    @Getter
    private final VirtualThreadLedgerWalEventHandler eventHandler;
    @Getter
    private final RingBuffer<LedgerWalEvent> ringBuffer;

    public VirtualThreadLedgerWalService(
        DataSource dataSource,
        TransactionTemplate transactionTemplate,
        LedgerWalConfiguration config,
        LedgerWalMetrics metrics,
        ParameterMapPool parameterMapPool) {

        this.namedJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        this.config = config;
        this.metrics = metrics;
        this.sequenceGenerator = new AtomicLong(0);

        // Thread-safe bounded cache
        this.idempotencyCache = Collections.synchronizedMap(
            new LinkedHashMap<UUID, Long>(config.getIdempotencyCacheMaxSize(), 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<UUID, Long> eldest) {
                    return size() > config.getIdempotencyCacheMaxSize();
                }
            });

        ThreadFactory virtualThreadFactory = Thread.ofVirtual()
            .name("ledger-wal-disruptor-", 0)
            .uncaughtExceptionHandler((t, e) ->
                log.error("FATAL: Uncaught exception in Virtual Thread {}: {}", t.getName(), e.getMessage(), e))
            .factory();

        this.eventHandler = new VirtualThreadLedgerWalEventHandler(
            namedJdbcTemplate, transactionTemplate, config, metrics, parameterMapPool);

        this.disruptor = new Disruptor<>(
            new LedgerWalEventFactory(),
            config.getRingBufferSize(),
            virtualThreadFactory,
            ProducerType.MULTI,
            new YieldingWaitStrategy()
        );

        disruptor.handleEventsWith(eventHandler);
        disruptor.start();
        this.ringBuffer = disruptor.getRingBuffer();

        // Register metrics
        metrics.registerRingBufferUtilizationGauge(this);

        log.info("Virtual Thread Ledger WAL Service started with production configuration");
        log.info("  Ring buffer size: {}", config.getRingBufferSize());
        log.info("  Batch size: {}", config.getBatchSize());
        log.info("  Flush interval: {}ms", config.getFlushIntervalMs());
        log.info("  Circuit breaker threshold: {}", config.getCircuitBreakerFailureThreshold());
        log.info("  Idempotency cache max size: {}", config.getIdempotencyCacheMaxSize());
    }

    public long writeDoubleEntry(@Valid LedgerOperationValidationRequest request) {
        var timer = metrics.startWriteTimer();

        try {
            request.validateBusinessRules();

            // Thread-safe idempotency check
            Long existingSequence = idempotencyCache.get(request.getIdempotencyKey());
            if (existingSequence != null) {
                log.debug("Duplicate operation detected for idempotency key: {}", request.getIdempotencyKey());
                return existingSequence;
            }

            // Check ring buffer capacity
            if (ringBuffer.remainingCapacity() < 100) {
                log.warn("Ring buffer nearly full, remaining capacity: {}", ringBuffer.remainingCapacity());
                metrics.recordError("ring_buffer_full");
                throw new IllegalStateException("Ring buffer capacity exceeded");
            }

            long sequence = ringBuffer.next();

            try {
                LedgerWalEvent event = ringBuffer.get(sequence);
                long sequenceNumber = sequenceGenerator.incrementAndGet();

                event.setSequenceNumber(sequenceNumber);
                event.setDebitAccountId(request.getDebitAccountId());
                event.setCreditAccountId(request.getCreditAccountId());
                event.setAmount(request.getAmount());
                event.setCurrencyCode(request.getCurrencyCode());
                event.setOperationDate(request.getOperationDate());
                event.setTransactionId(request.getTransactionId());
                event.setIdempotencyKey(request.getIdempotencyKey());
                event.setStatus(WalStatus.PENDING);

                // Cache for idempotency (thread-safe)
                idempotencyCache.put(request.getIdempotencyKey(), sequenceNumber);

                metrics.recordWrite();
                return sequenceNumber;

            } finally {
                ringBuffer.publish(sequence);
            }

        } catch (Exception e) {
            metrics.recordError("validation_error");
            throw e;
        } finally {
            timer.stop();
        }
    }

    public long writeDoubleEntry(UUID debitAccountId, UUID creditAccountId, long amount) {
        var request = LedgerOperationValidationRequest.builder()
            .debitAccountId(debitAccountId)
            .creditAccountId(creditAccountId)
            .amount(amount)
            .currencyCode("RUB")
            .operationDate(LocalDate.now())
            .transactionId(UUID.randomUUID())
            .idempotencyKey(UUID.randomUUID())
            .build();

        return writeDoubleEntry(request);
    }

    public List<Long> writeDoubleEntries(List<DoubleEntryRequest> operations) {
        return operations.stream()
            .map(req -> {
                var validationRequest = LedgerOperationValidationRequest.builder()
                    .debitAccountId(req.debitAccountId())
                    .creditAccountId(req.creditAccountId())
                    .amount(req.amount())
                    .currencyCode(req.currencyCode())
                    .operationDate(req.operationDate())
                    .transactionId(req.transactionId())
                    .idempotencyKey(req.idempotencyKey())
                    .build();
                return writeDoubleEntry(validationRequest);
            })
            .toList();
    }

    public CompletableFuture<List<Long>> writeDoubleEntriesAsync(List<DoubleEntryRequest> operations) {
        return CompletableFuture.supplyAsync(() -> writeDoubleEntries(operations),
            Executors.newVirtualThreadPerTaskExecutor());
    }

    // Monitoring methods
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

    public boolean isCircuitBreakerOpen() {
        return eventHandler.isCircuitBreakerOpen();
    }

    public long getConsecutiveFailures() {
        return eventHandler.getConsecutiveFailures();
    }

    public int getIdempotencyCacheSize() {
        return idempotencyCache.size();
    }

    @Override
    public void close() {
        try {
            log.info("Shutting down Virtual Thread Ledger WAL Service...");

            long dynamicWaitTime = Math.max(config.getFlushIntervalMs() * 2,
                ringBuffer.getBufferSize() / 10000);
            Thread.sleep(dynamicWaitTime);

            eventHandler.shutdown();

            boolean gracefulShutdown = disruptor.shutdown(
                config.getShutdownTimeoutSeconds(), TimeUnit.SECONDS);

            if (!gracefulShutdown) {
                log.warn("Disruptor did not shutdown gracefully within timeout");
                disruptor.halt();
            }

            log.info("Virtual Thread Ledger WAL Service shutdown completed. Total processed: {}",
                getProcessedCount());
        } catch (Exception e) {
            log.error("ERROR during Virtual Thread WAL service shutdown", e);
        }
    }
}
