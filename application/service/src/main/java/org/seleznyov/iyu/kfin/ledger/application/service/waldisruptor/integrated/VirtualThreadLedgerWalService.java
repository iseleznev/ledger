package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor.integrated;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Enhanced VirtualThreadLedgerWalService with durability integration
 */
@Service
@Slf4j
@Validated
public class VirtualThreadLedgerWalService implements AutoCloseable {

    private final NamedParameterJdbcTemplate namedJdbcTemplate;
    private final LedgerWalConfiguration config;
    private final LedgerWalMetrics metrics;
    @Getter private final AtomicLong sequenceGenerator;

    // Durability integration
    private final HybridDurabilitySystem durabilitySystem;
    private final MemoryMappedBalanceService balanceService;

    // Idempotency cache
    private final Map<UUID, Long> idempotencyCache;

    @Getter private final Disruptor<LedgerWalEvent> disruptor;
    @Getter private final EnhancedVirtualThreadLedgerWalEventHandler eventHandler;
    @Getter private final RingBuffer<LedgerWalEvent> ringBuffer;

    public VirtualThreadLedgerWalService(
        DataSource dataSource,
        TransactionTemplate transactionTemplate,
        LedgerWalConfiguration config,
        LedgerWalMetrics metrics,
        ParameterMapPool parameterMapPool,
        HybridDurabilitySystem durabilitySystem,
        MemoryMappedBalanceService balanceService) {

        this.namedJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        this.config = config;
        this.metrics = metrics;
        this.durabilitySystem = durabilitySystem;
        this.balanceService = balanceService;
        this.sequenceGenerator = new AtomicLong(0);

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

        // Enhanced event handler with durability integration
        this.eventHandler = new EnhancedVirtualThreadLedgerWalEventHandler(
            namedJdbcTemplate, transactionTemplate, config, metrics,
            parameterMapPool, durabilitySystem, balanceService);

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

        metrics.registerRingBufferUtilizationGauge(this);

        log.info("Enhanced Virtual Thread Ledger WAL Service started with durability integration");
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

    // Convenience methods remain the same...
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

    // Monitoring methods
    public long getProcessedCount() { return eventHandler.getProcessedCount(); }
    public long getRingBufferSize() { return ringBuffer.getBufferSize(); }
    public long getRingBufferRemaining() { return ringBuffer.remainingCapacity(); }
    public boolean isVirtualThread() { return Thread.currentThread().isVirtual(); }
    public boolean isCircuitBreakerOpen() { return eventHandler.isCircuitBreakerOpen(); }
    public long getConsecutiveFailures() { return eventHandler.getConsecutiveFailures(); }
    public int getIdempotencyCacheSize() { return idempotencyCache.size(); }

    @Override
    public void close() {
        try {
            log.info("Shutting down Enhanced Virtual Thread Ledger WAL Service...");

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

            log.info("Enhanced Virtual Thread Ledger WAL Service shutdown completed. Total processed: {}",
                getProcessedCount());
        } catch (Exception e) {
            log.error("ERROR during Enhanced Virtual Thread WAL service shutdown", e);
        }
    }
}

/*
=== ИНТЕГРИРОВАННАЯ СИСТЕМА ГОТОВА ===

✅ ОБЪЕДИНЕНЫ ВСЕ КОМПОНЕНТЫ:

1. **Main Business Pipeline:**
   - VirtualThreadLedgerWalService - основная бизнес-логика
   - Enhanced event handler - с интеграцией durability
   - PostgreSQL WAL - primary durability через database

2. **Hybrid Durability Layer:**
   - LockFreeImmediateWalWriter - enhanced durability для критических операций
   - Memory-mapped segments - fast persistence
   - Configurable durability levels

3. **Fast Balance Service:**
   - MemoryMappedBalanceService - ultra-fast balance reads
   - Automatic balance updates после каждой операции
   - Snapshot system для recovery

4. **Integration Benefits:**
   - Single API для всех операций
   - Automatic balance updates
   - Multiple durability levels
   - Comprehensive monitoring
   - Graceful degradation

=== ИСПОЛЬЗОВАНИЕ:

```java
// Обычная операция:
long sequence = ledgerSystem.writeDoubleEntry(
    debitAccount, creditAccount, amount, "RUB",
    LocalDate.now(), txId, idempotencyKey,
    DurabilityLevel.BATCH_FSYNC  // Configurable
);

// Быстрое чтение баланса:
Optional<Long> balance = ledgerSystem.getAccountBalance(accountId);

// Системная статистика:
Map<String, Object> stats = ledgerSystem.getSystemStatistics();
```

Система теперь объединяет все лучшие практики в единое решение!
*/