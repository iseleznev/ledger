package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor.integrated;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Integrated Ledger System - объединяет все компоненты
 * <p>
 * Архитектура:
 * 1. Main Disruptor Pipeline для business logic (VirtualThreadLedgerWalService)
 * 2. Hybrid Durability для persistence (HybridDurabilitySystem)
 * 3. Memory-Mapped Balance Cache для fast reads (MemoryMappedBalanceService)
 * 4. Async Replication для HA (AsyncReplicationManager)
 */
@Service
@Slf4j
public class IntegratedLedgerSystem implements AutoCloseable {

    // Core business logic pipeline
    private final VirtualThreadLedgerWalService mainWalService;
    private final VirtualThreadWalProcessorService walProcessor;

    // Durability and persistence layer
    private final HybridDurabilitySystem durabilitySystem;

    // Fast balance service
    private final MemoryMappedBalanceService balanceService;

    // Configuration
    private final LedgerWalConfiguration config;

    public IntegratedLedgerSystem(
        DataSource dataSource,
        TransactionTemplate transactionTemplate,
        LedgerWalConfiguration config,
        LedgerWalMetrics metrics,
        ParameterMapPool parameterMapPool) throws IOException {

        this.config = config;

        // 1. Initialize durability layer first
        this.durabilitySystem = new HybridDurabilitySystem(
            Path.of(config.getBaseDirectory())
        );

        // 2. Initialize balance service with durability integration
        this.balanceService = new MemoryMappedBalanceService(
            config, durabilitySystem.getJournal()
        );

        // 3. Initialize main WAL service with integrated event handler
        this.mainWalService = new VirtualThreadLedgerWalService(
            dataSource, transactionTemplate, config, metrics,
            parameterMapPool, durabilitySystem, balanceService
        );

        // 4. Initialize WAL processor
        this.walProcessor = new VirtualThreadWalProcessorService(
            new NamedParameterJdbcTemplate(dataSource),
            transactionTemplate, config, parameterMapPool
        );

        // 5. Start processing pipeline
        walProcessor.startScheduledProcessing();

        log.info("Initialized Integrated Ledger System with all components");
    }

    /**
     * Main business operation - write double entry with full durability
     */
    public long writeDoubleEntry(
        UUID debitAccountId,
        UUID creditAccountId,
        long amount,
        String currencyCode,
        LocalDate operationDate,
        UUID transactionId,
        UUID idempotencyKey,
        DurabilityLevel durabilityLevel) throws IOException {

        // 1. Validate and write through main pipeline
        long walSequence = mainWalService.writeDoubleEntry(
            LedgerOperationValidationRequest.builder()
                .debitAccountId(debitAccountId)
                .creditAccountId(creditAccountId)
                .amount(amount)
                .currencyCode(currencyCode)
                .operationDate(operationDate)
                .transactionId(transactionId)
                .idempotencyKey(idempotencyKey)
                .build()
        );

        // 2. Enhanced durability через hybrid system (optional for critical operations)
        if (durabilityLevel == DurabilityLevel.IMMEDIATE_FSYNC) {
            byte[] operationData = createOperationData(
                debitAccountId, creditAccountId, amount, currencyCode,
                operationDate, transactionId, idempotencyKey
            );

            durabilitySystem.writeOperation(operationData, durabilityLevel);
        }

        return walSequence;
    }

    private byte[] createOperationData(UUID debitAccountId, UUID creditAccountId,
                                       long amount, String currencyCode, LocalDate operationDate,
                                       UUID transactionId, UUID idempotencyKey) {
        // Serialize operation для durability system
        ByteBuffer buffer = ByteBuffer.allocate(16 + 16 + 8 + 8 + 8 + 16 + 16);
        buffer.putLong(debitAccountId.getMostSignificantBits());
        buffer.putLong(debitAccountId.getLeastSignificantBits());
        buffer.putLong(creditAccountId.getMostSignificantBits());
        buffer.putLong(creditAccountId.getLeastSignificantBits());
        buffer.putLong(amount);
        buffer.putLong(operationDate.toEpochDay());
        buffer.putLong(System.currentTimeMillis()); // timestamp
        buffer.putLong(transactionId.getMostSignificantBits());
        buffer.putLong(transactionId.getLeastSignificantBits());
        buffer.putLong(idempotencyKey.getMostSignificantBits());
        buffer.putLong(idempotencyKey.getLeastSignificantBits());

        return buffer.array();
    }

    /**
     * Fast balance read from memory-mapped cache
     */
    public Optional<Long> getAccountBalance(UUID accountId) {
        return balanceService.getBalance(accountId)
            .map(balance -> balance.getBalance());
    }

    /**
     * Batch balance reads
     */
    public Map<UUID, Long> getAccountBalances(Set<UUID> accountIds) {
        return balanceService.getBalances(accountIds)
            .entrySet()
            .stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().getBalance()
            ));
    }

    /**
     * System statistics from all components
     */
    public Map<String, Object> getSystemStatistics() {
        Map<String, Object> stats = new HashMap<>();

        // Main WAL service stats
        stats.put("mainWal.processedCount", mainWalService.getProcessedCount());
        stats.put("mainWal.ringBufferUtilization",
            (double) (mainWalService.getRingBufferSize() - mainWalService.getRingBufferRemaining())
                / mainWalService.getRingBufferSize() * 100.0);
        stats.put("mainWal.circuitBreakerOpen", mainWalService.isCircuitBreakerOpen());

        // Balance service stats
        stats.putAll(balanceService.getStatistics());

        // Durability system stats
        stats.putAll(durabilitySystem.getStatistics());

        // WAL processor stats
        stats.put("walProcessor.totalProcessed", walProcessor.getTotalProcessed());
        stats.put("walProcessor.errorRate", walProcessor.getErrorRate());

        return stats;
    }

    /**
     * Health check for all components
     */
    public boolean isHealthy() {
        return !mainWalService.isCircuitBreakerOpen() &&
            !balanceService.isRecovering() &&
            durabilitySystem.isHealthy() &&
            walProcessor.getErrorRate() < 5.0; // Less than 5% error rate
    }

    /**
     * Force sync all components
     */
    public void forceSync() {
        durabilitySystem.forceSync();
        // Main WAL service syncs through PostgreSQL automatically
    }

    /**
     * Create manual snapshot
     */
    public CompletableFuture<Void> createSnapshot() {
        return balanceService.createSnapshotAsync()
            .thenCompose(v -> durabilitySystem.createSnapshot());
    }

    @Override
    public void close() throws IOException {
        log.info("Shutting down Integrated Ledger System...");

        // Shutdown in reverse order
        walProcessor.close();
        mainWalService.close();
        balanceService.close();
        durabilitySystem.close();

        log.info("Integrated Ledger System shutdown completed");
    }
}
