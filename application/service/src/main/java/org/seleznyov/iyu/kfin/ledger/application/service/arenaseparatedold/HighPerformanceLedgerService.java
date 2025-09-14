package org.seleznyov.iyu.kfin.ledger.application.service.arenaseparatedold;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.application.service.arenaseparated.*;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

// High-Performance Ledger Service with Separated Interfaces
@Slf4j
@Service
@RequiredArgsConstructor
public class HighPerformanceLedgerService {

    private final SeparatedAccountManagerRegistry managerRegistry;
    private final LedgerRingBuffer ringBuffer;
    private final SnapshotManager snapshotManager;

    public CompletableFuture<String> processTransaction(UUID debitAccountId, UUID creditAccountId,
                                                        long amount, String currencyCode,
                                                        UUID transactionId, UUID idempotencyKey) {

        long baseOrdinal = System.nanoTime();
        LocalDate operationDate = LocalDate.now();
        Instant now = Instant.now();

        EntryRecord debitEntry = createEntry(debitAccountId, transactionId,
            EntryRecord.EntryType.DEBIT, amount, now, operationDate,
            idempotencyKey, currencyCode, baseOrdinal);

        EntryRecord creditEntry = createEntry(creditAccountId, transactionId,
            EntryRecord.EntryType.CREDIT, amount, now, operationDate,
            idempotencyKey, currencyCode, baseOrdinal + 1);

        CompletableFuture<Void> debitFuture = processAccountEntry(debitEntry);
        CompletableFuture<Void> creditFuture = processAccountEntry(creditEntry);

        return CompletableFuture.allOf(debitFuture, creditFuture)
            .thenApply(v -> "Transaction accepted: " + transactionId);
    }

    private CompletableFuture<Void> processAccountEntry(EntryRecord entry) {
        UUID accountId = entry.getAccountId();

        if (managerRegistry.isHotAccount(accountId)) {
            return processHotAccountEntry(entry);
        } else {
            return processColdAccountEntry(entry);
        }
    }

    private CompletableFuture<Void> processHotAccountEntry(EntryRecord entry) {
        return CompletableFuture.runAsync(() -> {
            HotAccountManager manager = managerRegistry.getHotAccountManager(entry.getAccountId());

            // SYNCHRONOUS call - maximum performance
            boolean added = manager.addEntry(entry);

            if (added) {
                handleSuccessfulHotEntry(manager, entry);
            } else {
                log.warn("Failed to add entry to HOT account {}", entry.getAccountId());
            }
        }, managerRegistry.getHotAccountExecutor(entry.getAccountId()));
    }

    private CompletableFuture<Void> processColdAccountEntry(EntryRecord entry) {
        return CompletableFuture.runAsync(() -> {
            ColdAccountManager manager = managerRegistry.getColdAccountManager(entry.getAccountId());

            // ASYNCHRONOUS call - handles multiple VTs efficiently
            manager.addEntry(entry).thenAccept(added -> {
                if (added) {
                    handleSuccessfulColdEntry(manager, entry);
                } else {
                    log.warn("Failed to add entry to COLD account {}", entry.getAccountId());
                }
            }).exceptionally(ex -> {
                log.error("Error processing COLD account entry", ex);
                return null;
            });
        }, managerRegistry.getColdAccountExecutor());
    }

    private void handleSuccessfulHotEntry(HotAccountManager manager, EntryRecord entry) {
        // Snapshot handling
        handleSnapshotForAccount(manager, entry);

        // Batch handling - only hot accounts use ring buffer
        if (manager.isBatchReady()) {
            BatchData batch = manager.extractBatch();
            if (batch != null) {
                boolean published = publishToRingBuffer(batch);
                if (published) {
                    manager.onBatchQueued(batch.getEntryCount());
                }
            }
        }
    }

//    private void handleSuccessfulColdEntry(BaseAccountManager manager, EntryRecord entry) {
//        // Only snapshot handling - no batches for cold accounts
//        handleSnapshotForAccount(manager, entry);
//    }
//
    private void handleSnapshotForAccount(BaseAccountManager manager, EntryRecord entry) {
        snapshotManager.updateSnapshot(entry.getAccountId(), entry);

        if (manager.shouldCreateSnapshot()) {
            long snapshotOrdinal = manager.getSnapshotOrdinal();
            log.debug("Creating snapshot for account {} at ordinal {}",
                entry.getAccountId(), snapshotOrdinal);

            snapshotManager.persistSnapshot(entry.getAccountId())
                .thenRun(() -> manager.onSnapshotCreated())
                .exceptionally(ex -> {
                    log.error("Failed to create snapshot for account {}",
                        entry.getAccountId(), ex);
                    return null;
                });
        }
    }

    private boolean publishToRingBuffer(BatchData batch) {
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

//    public long getCurrentBalance(UUID accountId) {
//        if (managerRegistry.isHotAccount(accountId)) {
//            HotAccountManager manager = managerRegistry.getHotAccountManager(accountId);
//            AccountSnapshot lastSnapshot = snapshotManager.getLatestSnapshot(accountId);
//            return manager.getCurrentBalance(lastSnapshot);
//        } else {
//            ColdAccountManager manager = managerRegistry.getColdAccountManager(accountId);
//            return manager.getCurrentBalance(null);
//        }
//    }

    public long getCurrentBalance(UUID accountId) {
        if (managerRegistry.isHotAccount(accountId)) {
            HotAccountManager manager = managerRegistry.getHotAccountManager(accountId);
            AccountSnapshot lastSnapshot = snapshotManager.getLatestSnapshot(accountId);
            return manager.getCurrentBalance(lastSnapshot);
        } else {
            // For cold accounts, use the partition manager but get balance from DB
            HybridPartitionedColdAccountManager partitionManager = managerRegistry.getColdPartitionManager(accountId);
            // Cold accounts get fresh balance from DB (low RPS so acceptable)
            return partitionManager.getCurrentBalanceFromDb(accountId);
        }
    }

    private void handleSuccessfulColdEntry(BaseAccountManager manager, EntryRecord entry) {
        // For hybrid partitioned managers, we need special handling
        if (manager instanceof HybridPartitionedColdAccountManager partitionManager) {
            handleSnapshotForColdAccount(partitionManager, entry);
        } else {
            // Fallback for other cold managers
            handleSnapshotForAccount(manager, entry);
        }
    }

    private void handleSnapshotForColdAccount(HybridPartitionedColdAccountManager partitionManager, EntryRecord entry) {
        UUID accountId = entry.getAccountId();

        // Update snapshot in SnapshotManager
        snapshotManager.updateSnapshot(accountId, entry);

        // Check if this specific account needs a snapshot
        if (partitionManager.shouldCreateSnapshotForAccount(accountId)) {
            long snapshotOrdinal = partitionManager.getSnapshotOrdinalForAccount(accountId);
            log.debug("Creating snapshot for COLD account {} at ordinal {}", accountId, snapshotOrdinal);

            snapshotManager.persistSnapshot(accountId)
                .thenRun(() -> partitionManager.onSnapshotCreatedForAccount(accountId))
                .exceptionally(ex -> {
                    log.error("Failed to create snapshot for COLD account {}", accountId, ex);
                    return null;
                });
        }
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
}
