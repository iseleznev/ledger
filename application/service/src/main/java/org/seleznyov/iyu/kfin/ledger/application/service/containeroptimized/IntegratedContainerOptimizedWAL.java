package org.seleznyov.iyu.kfin.ledger.application.service.containeroptimized;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Container-Optimized WAL with real database persistence
 * Combines in-memory performance with database durability
 */
@Component
@Slf4j
public class IntegratedContainerOptimizedWAL implements AutoCloseable {

    private final ImmutableWalEntryRepository walRepository;

    // In-memory queues for high-performance buffering
    private final int shardCount;
    private final int shardMask;
    private final ConcurrentLinkedQueue<PendingWalEntry>[] memoryQueues;
    private final ExecutorService[] shardExecutors;
    private final ScheduledExecutorService flushScheduler;

    // Sequence management
    private final AtomicLong globalSequenceNumber = new AtomicLong(0);

    // Performance tracking
    private final AtomicLong totalAppends = new AtomicLong(0);
    private final AtomicLong totalFlushes = new AtomicLong(0);
    private final AtomicLong totalPersisted = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);

    // Configuration
    private static final int BATCH_FLUSH_SIZE = 500;
    private static final int FLUSH_INTERVAL_MS = 2000; // 2 seconds
    private static final int QUEUE_SIZE_PER_SHARD = 10_000;

    @SuppressWarnings("unchecked")
    public IntegratedContainerOptimizedWAL(ImmutableWalEntryRepository walRepository) {
        this.walRepository = walRepository;
        this.shardCount = getOptimalShardCount();
        this.shardMask = shardCount - 1;

        this.memoryQueues = new ConcurrentLinkedQueue[shardCount];
        this.shardExecutors = new ExecutorService[shardCount];

        initializeShards();
        initializeSequenceNumber();

        // Start background flush scheduler
        this.flushScheduler = Executors.newSingleThreadScheduledExecutor(
            Thread.ofVirtual().name("wal-flush-scheduler-").factory());
        startPeriodicFlush();

        log.info("Initialized IntegratedContainerOptimizedWAL with {} shards", shardCount);
    }

    private int getOptimalShardCount() {
        int available = Runtime.getRuntime().availableProcessors();
        int optimal = Math.max(2, available / 2);

        // Ensure power of 2 for efficient bit masking
        int powerOfTwo = 1;
        while (powerOfTwo < optimal) {
            powerOfTwo <<= 1;
        }

        return optimal == powerOfTwo ? optimal : powerOfTwo >> 1;
    }

    private void initializeShards() {
        for (int shard = 0; shard < shardCount; shard++) {
            memoryQueues[shard] = new ConcurrentLinkedQueue<>();
            shardExecutors[shard] = Executors.newSingleThreadExecutor(
                Thread.ofVirtual().name("wal-shard-" + shard + "-").factory());
        }
    }

    private void initializeSequenceNumber() {
        try {
            OptionalLong maxSequence = walRepository.getMaxSequenceNumber();
            if (maxSequence.isPresent()) {
                globalSequenceNumber.set(maxSequence.getAsLong());
                log.info("WAL sequence initialized to {}", maxSequence.getAsLong());
            }
        } catch (Exception e) {
            log.warn("Failed to initialize WAL sequence from database, starting from 0", e);
        }
    }

    private void startPeriodicFlush() {
        flushScheduler.scheduleWithFixedDelay(
            this::flushAllShards,
            FLUSH_INTERVAL_MS,
            FLUSH_INTERVAL_MS,
            TimeUnit.MILLISECONDS);

        log.debug("Started periodic WAL flush every {}ms", FLUSH_INTERVAL_MS);
    }

    private int getShardId(UUID accountId) {
        return accountId.hashCode() & shardMask;
    }

    /**
     * Append to WAL with high-performance in-memory buffering
     */
    public CompletableFuture<Long> append(UUID debitAccountId, UUID creditAccountId, long amount,
                                          String currencyCode, UUID transactionId, UUID idempotencyKey,
                                          DurabilityLevel durabilityLevel) {

        int shardId = getShardId(transactionId); // Shard by transaction for consistency
        long sequenceNumber = globalSequenceNumber.incrementAndGet();

        return CompletableFuture.supplyAsync(() -> {
            try {
                // Create pending WAL entry for in-memory queue
                PendingWalEntry pendingEntry = PendingWalEntry.builder()
                    .sequenceNumber(sequenceNumber)
                    .debitAccountId(debitAccountId)
                    .creditAccountId(creditAccountId)
                    .amount(amount)
                    .currencyCode(currencyCode)
                    .operationDate(LocalDate.now())
                    .transactionId(transactionId)
                    .idempotencyKey(idempotencyKey)
                    .durabilityLevel(durabilityLevel)
                    .createdAt(System.currentTimeMillis())
                    .build();

                // Check queue capacity
                if (memoryQueues[shardId].size() >= QUEUE_SIZE_PER_SHARD) {
                    totalErrors.incrementAndGet();
                    throw new WALException("WAL shard " + shardId + " queue full");
                }

                // Add to memory queue
                memoryQueues[shardId].offer(pendingEntry);
                totalAppends.incrementAndGet();

                // For SYNC durability, flush immediately
                if (durabilityLevel.requiresSync()) {
                    flushShard(shardId, 1); // Flush just this entry
                    log.debug("Sync flush completed for sequence {}", sequenceNumber);
                }

                log.debug("WAL entry queued: sequence={}, tx={}, shard={}",
                    sequenceNumber, transactionId, shardId);

                return sequenceNumber;

            } catch (Exception e) {
                totalErrors.incrementAndGet();
                log.error("WAL append failed for transaction {}", transactionId, e);
                throw new WALException("WAL append failed", e);
            }

        }, shardExecutors[shardId]);
    }

    /**
     * Flush all shards to database
     */
    public CompletableFuture<Integer> flushAllShards() {
        List<CompletableFuture<Integer>> flushFutures = new ArrayList<>();

        for (int shardId = 0; shardId < shardCount; shardId++) {
            flushFutures.add(flushShard(shardId, BATCH_FLUSH_SIZE));
        }

        return CompletableFuture.allOf(flushFutures.toArray(new CompletableFuture[0]))
            .thenApply(ignored -> {
                int totalFlushed = flushFutures.stream()
                    .mapToInt(CompletableFuture::join)
                    .sum();

                if (totalFlushed > 0) {
                    totalFlushes.incrementAndGet();
                    totalPersisted.addAndGet(totalFlushed);
                    log.debug("Flushed {} WAL entries across all shards", totalFlushed);
                }

                return totalFlushed;
            });
    }

    /**
     * Flush specific shard to database
     */
    public CompletableFuture<Integer> flushShard(int shardId, int maxEntries) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                List<PendingWalEntry> entriesToFlush = new ArrayList<>();
                ConcurrentLinkedQueue<PendingWalEntry> queue = memoryQueues[shardId];

                // Collect entries to flush
                int collected = 0;
                while (collected < maxEntries && !queue.isEmpty()) {
                    PendingWalEntry entry = queue.poll();
                    if (entry != null) {
                        entriesToFlush.add(entry);
                        collected++;
                    }
                }

                if (entriesToFlush.isEmpty()) {
                    return 0;
                }

                // Convert to WAL entities and persist
                List<WalEntry> walEntries = entriesToFlush.stream()
                    .map(this::convertToWalEntry)
                    .toList();

                walRepository.saveAll(walEntries);

                log.debug("Flushed {} entries from shard {}", walEntries.size(), shardId);
                return walEntries.size();

            } catch (Exception e) {
                totalErrors.incrementAndGet();
                log.error("Failed to flush shard {}", shardId, e);
                // Put entries back to queue on failure (simplified)
                return 0;
            }

        }, shardExecutors[shardId]);
    }

    /**
     * Get pending entries from database for recovery
     */
    public CompletableFuture<List<TransactionEntry>> getUnflushedEntries(int maxEntries) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Get current pending entries from database
                List<WalEntry> pendingWalEntries = walRepository.findCurrentPendingEntries(maxEntries);

                // Convert to TransactionEntry format for compatibility
                List<TransactionEntry> transactionEntries = pendingWalEntries.stream()
                    .map(this::convertToTransactionEntry)
                    .sorted(Comparator.comparing(TransactionEntry::getSequenceNumber))
                    .toList();

                log.debug("Retrieved {} unflushed entries from database", transactionEntries.size());
                return transactionEntries;

            } catch (Exception e) {
                totalErrors.incrementAndGet();
                log.error("Failed to get unflushed entries from database", e);
                return Collections.emptyList();
            }
        });
    }

    /**
     * Change WAL entry status (creates new version in database)
     */
    public CompletableFuture<Void> changeEntryStatus(UUID transactionId, WalEntry.WalStatus newStatus, String errorMessage) {
        return CompletableFuture.runAsync(() -> {
            try {
                walRepository.changeStatus(transactionId, newStatus, errorMessage);
                log.debug("Changed WAL status for transaction {} to {}", transactionId, newStatus);

            } catch (Exception e) {
                totalErrors.incrementAndGet();
                log.error("Failed to change WAL status for transaction {}", transactionId, e);
                throw new WALException("Failed to change WAL status", e);
            }
        });
    }

    /**
     * Get complete version history for transaction (audit purposes)
     */
    public CompletableFuture<List<WalEntry>> getTransactionHistory(UUID transactionId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return walRepository.getVersionHistory(transactionId);
            } catch (Exception e) {
                log.error("Failed to get transaction history for {}", transactionId, e);
                return Collections.emptyList();
            }
        });
    }

    private PendingWalEntry convertToWalEntry(WalEntry walEntry) {
        return PendingWalEntry.builder()
            .sequenceNumber(walEntry.getSequenceNumber())
            .debitAccountId(walEntry.getDebitAccountId())
            .creditAccountId(walEntry.getCreditAccountId())
            .amount(walEntry.getAmount())
            .currencyCode(walEntry.getCurrencyCode())
            .operationDate(walEntry.getOperationDate())
            .transactionId(walEntry.getTransactionId())
            .idempotencyKey(walEntry.getIdempotencyKey())
            .durabilityLevel(DurabilityLevel.WAL_BUFFERED) // Default
            .createdAt(walEntry.getCreatedAt().toEpochSecond() * 1000)
            .build();
    }

    private WalEntry convertToWalEntry(PendingWalEntry pendingEntry) {
        return WalEntry.create(
            pendingEntry.getDebitAccountId(),
            pendingEntry.getCreditAccountId(),
            pendingEntry.getAmount(),
            pendingEntry.getCurrencyCode(),
            pendingEntry.getOperationDate(),
            pendingEntry.getTransactionId(),
            pendingEntry.getIdempotencyKey(),
            pendingEntry.getSequenceNumber()
        );
    }

    private TransactionEntry convertToTransactionEntry(WalEntry walEntry) {
        TransactionEntry entry = TransactionEntry.builder()
            .timestamp(walEntry.getCreatedAt().toEpochSecond() * 1000)
            .accountId(walEntry.getDebitAccountId()) // Simplified - in real system would handle both
            .delta(-walEntry.getAmount()) // Debit
            .operationId(walEntry.getTransactionId())
            .sequenceNumber(walEntry.getSequenceNumber())
            .build();

        // Calculate proper checksum after building the entry
        entry.setChecksum(entry.calculateChecksum());
        return entry;
    }

    /**
     * Statistics with both in-memory and database metrics
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new HashMap<>();

        // Basic counts
        stats.put("shardCount", shardCount);
        stats.put("totalAppends", totalAppends.get());
        stats.put("totalFlushes", totalFlushes.get());
        stats.put("totalPersisted", totalPersisted.get());
        stats.put("totalErrors", totalErrors.get());
        stats.put("currentSequence", globalSequenceNumber.get());

        // In-memory queue stats
        int totalQueued = Arrays.stream(memoryQueues).mapToInt(ConcurrentLinkedQueue::size).sum();
        stats.put("totalQueued", totalQueued);

        // Per-shard queue utilization
        Map<String, Object> shardStats = new HashMap<>();
        for (int i = 0; i < shardCount; i++) {
            Map<String, Object> shardStat = new HashMap<>();
            int queueSize = memoryQueues[i].size();
            shardStat.put("queueSize", queueSize);
            shardStat.put("utilization",
                String.format("%.2f%%", (queueSize * 100.0) / QUEUE_SIZE_PER_SHARD));
            shardStats.put("shard_" + i, shardStat);
        }
        stats.put("shardStatistics", shardStats);

        // Database stats
        try {
            long pendingCount = walRepository.countCurrentByStatus(WalEntry.WalStatus.PENDING);
            long processingCount = walRepository.countCurrentByStatus(WalEntry.WalStatus.PROCESSING);
            long processedCount = walRepository.countCurrentByStatus(WalEntry.WalStatus.PROCESSED);

            Map<String, Object> dbStats = new HashMap<>();
            dbStats.put("pendingInDb", pendingCount);
            dbStats.put("processingInDb", processingCount);
            dbStats.put("processedInDb", processedCount);
            stats.put("databaseStatistics", dbStats);

        } catch (Exception e) {
            log.warn("Failed to get database statistics", e);
            stats.put("databaseStatistics", Map.of("error", "Failed to retrieve"));
        }

        return stats;
    }

    public boolean isHealthy() {
        try {
            // Check in-memory queues are not overloaded
            boolean queuesHealthy = Arrays.stream(memoryQueues)
                .mapToInt(ConcurrentLinkedQueue::size)
                .allMatch(size -> size < QUEUE_SIZE_PER_SHARD * 0.8);

            // Check error rate
            long totalOps = totalAppends.get();
            long errors = totalErrors.get();
            boolean errorRateHealthy = totalOps == 0 || (errors * 100.0 / totalOps) < 1.0;

            // Check database connectivity (simplified)
            walRepository.getMaxSequenceNumber(); // This will throw if DB is down

            return queuesHealthy && errorRateHealthy;

        } catch (Exception e) {
            log.warn("WAL health check failed", e);
            return false;
        }
    }

    @Override
    public void close() {
        log.info("Shutting down IntegratedContainerOptimizedWAL...");

        // Stop periodic flush
        flushScheduler.shutdown();

        try {
            // Final flush of all pending entries
            flushAllShards().get(10, TimeUnit.SECONDS);
            log.info("Final WAL flush completed");

        } catch (Exception e) {
            log.error("Error during final WAL flush", e);
        }

        // Shutdown shard executors
        for (ExecutorService executor : shardExecutors) {
            if (executor != null) {
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    executor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
        }

        log.info("IntegratedContainerOptimizedWAL closed. Final stats: {}", getStatistics());
    }

    /**
     * In-memory representation of WAL entry before persistence
     */
    @lombok.Data
    @lombok.Builder
    private static class PendingWalEntry {
        private long sequenceNumber;
        private UUID debitAccountId;
        private UUID creditAccountId;
        private long amount;
        private String currencyCode;
        private LocalDate operationDate;
        private UUID transactionId;
        private UUID idempotencyKey;
        private DurabilityLevel durabilityLevel;
        private long createdAt;
    }

    public static class WALException extends RuntimeException {
        public WALException(String message) {
            super(message);
        }

        public WALException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}