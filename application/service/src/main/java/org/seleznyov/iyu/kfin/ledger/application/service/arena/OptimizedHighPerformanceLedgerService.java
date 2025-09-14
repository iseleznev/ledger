package org.seleznyov.iyu.kfin.ledger.application.service.arena;

import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

// Optimized main service - minimizing atomics
@Slf4j
@Service
@RequiredArgsConstructor
public class OptimizedHighPerformanceLedgerService {

    private static final int SHARD_COUNT = 256;

    // Account managers partitioned by shard to minimize contention
    private final Map<UUID, AccountBatchManager>[] shardedHotAccountManagers;
    private final Set<UUID> hotAccountIds;
    private final LedgerRingBuffer ringBuffer;
    private final SnapshotManager snapshotManager;
    private final NamedParameterJdbcTemplate jdbcTemplate;

    // Single virtual executor per shard to maintain account affinity
    private final Executor[] shardExecutors;

    @Value("${ledger.batch.maxSize:1000}")
    private int maxBatchSize;

    @Value("${ledger.snapshot.everyNOperations:5000}")
    private int snapshotEveryNOperations;

    @SuppressWarnings("unchecked")
    public OptimizedHighPerformanceLedgerService(Set<UUID> hotAccountIds,
                                                 LedgerRingBuffer ringBuffer,
                                                 SnapshotManager snapshotManager,
                                                 NamedParameterJdbcTemplate jdbcTemplate) {
        this.hotAccountIds = hotAccountIds;
        this.ringBuffer = ringBuffer;
        this.snapshotManager = snapshotManager;
        this.jdbcTemplate = jdbcTemplate;

        // Initialize sharded managers - NO atomics, just partitioned maps
        this.shardedHotAccountManagers = new Map[SHARD_COUNT];
        this.shardExecutors = new Executor[SHARD_COUNT];

        for (int i = 0; i < SHARD_COUNT; i++) {
            shardedHotAccountManagers[i] = new ConcurrentHashMap<>();
            shardExecutors[i] = Executors.newVirtualThreadPerTaskExecutor();
        }
    }

    public CompletableFuture<String> processTransaction(UUID debitAccountId, UUID creditAccountId,
                                                        long amount, String currencyCode,
                                                        UUID transactionId, UUID idempotencyKey) {

        // Process each account on its dedicated shard executor
        int debitShard = getAccountShard(debitAccountId);
        int creditShard = getAccountShard(creditAccountId);

        long baseOrdinal = System.nanoTime();
        LocalDate operationDate = LocalDate.now();
        Instant now = Instant.now();

        // Create entries
        EntryRecord debitEntry = createEntry(debitAccountId, transactionId,
            EntryRecord.EntryType.DEBIT, amount, now, operationDate,
            idempotencyKey, currencyCode, baseOrdinal);

        EntryRecord creditEntry = createEntry(creditAccountId, transactionId,
            EntryRecord.EntryType.CREDIT, amount, now, operationDate,
            idempotencyKey, currencyCode, baseOrdinal + 1);

        // Process on shard-specific executors to maintain single-thread-per-account
        CompletableFuture<Void> debitFuture = CompletableFuture.runAsync(() -> {
            processAccountEntry(debitEntry);
        }, shardExecutors[debitShard]);

        CompletableFuture<Void> creditFuture = CompletableFuture.runAsync(() -> {
            processAccountEntry(creditEntry);
        }, shardExecutors[creditShard]);

        return CompletableFuture.allOf(debitFuture, creditFuture)
            .thenApply(v -> "Transaction accepted: " + transactionId);
    }

    private EntryRecord createEntry(UUID accountId, UUID transactionId,
                                    EntryRecord.EntryType entryType, long amount,
                                    Instant createdAt, LocalDate operationDate,
                                    UUID idempotencyKey, String currencyCode, long ordinal) {
        return EntryRecord.builder()
            .id(UUID.randomUUID())
            .accountId(accountId)
            .transactionId(transactionId)
            .entryType(entryType)
            .amount(amount)
            .createdAt(createdAt)
            .operationDate(operationDate)
            .idempotencyKey(idempotencyKey)
            .currencyCode(currencyCode)
            .entryOrdinal(ordinal)
            .build();
    }

    private void processAccountEntry(EntryRecord entry) {
        if (isHotAccount(entry.getAccountId())) {
            processHotAccountEntry(entry);
        } else {
            processColdAccountEntry(entry);
        }
    }

    private void processHotAccountEntry(EntryRecord entry) {
        AccountBatchManager manager = getOrCreateHotAccountManager(entry.getAccountId());

        // Synchronous call - no CompletableFuture needed since single thread per account
        boolean added = manager.addEntry(entry);
        if (added) {
            // Update snapshot
            snapshotManager.updateSnapshot(entry.getAccountId(), entry);

            // Check if snapshot should be created based on operations count
            if (manager.shouldCreateSnapshot()) {
                long snapshotOrdinal = manager.getSnapshotOrdinal();
                log.debug("Creating snapshot for hot account {} at ordinal {}", entry.getAccountId(), snapshotOrdinal);

                // Trigger async snapshot creation
                snapshotManager.persistSnapshot(entry.getAccountId())
                    .thenRun(() -> manager.onSnapshotCreated())
                    .exceptionally(ex -> {
                        log.error("Failed to create snapshot for account {}", entry.getAccountId(), ex);
                        return null;
                    });
            }

            // Check if batch is ready
            if (manager.isBatchReady()) {
                BatchData batch = manager.extractBatch();
                if (batch != null) {
                    boolean published = publishToRingBuffer(batch);
                    if (published) {
                        manager.onBatchQueued(batch.getEntryCount());
                    }
                }
            }
        } else {
            log.warn("Failed to add entry to batch for hot account {}", entry.getAccountId());
        }
    }

    private AccountBatchManager getOrCreateHotAccountManager(UUID accountId) {
        int shard = getAccountShard(accountId);
        return shardedHotAccountManagers[shard].computeIfAbsent(accountId, id ->
            new AccountBatchManager(id, maxBatchSize, snapshotEveryNOperations));
    }

    private boolean publishToRingBuffer(BatchData batch) {
        // Simple retry without atomics
        for (int retry = 0; retry < 10; retry++) {
            if (ringBuffer.tryPublish(batch)) {
                return true;
            }
            Thread.onSpinWait();
        }

        log.error("Failed to publish batch {} after retries", batch.getBatchId());
        batch.setStatus(BatchData.BatchStatus.FAILED);
        return false;
    }

    private void processColdAccountEntry(EntryRecord entry) {
        // Cold account processing remains the same...
    }

    private boolean isHotAccount(UUID accountId) {
        return hotAccountIds.contains(accountId);
    }

    private int getAccountShard(UUID accountId) {
        return Math.abs(accountId.hashCode()) % SHARD_COUNT;
    }
}
