package org.seleznyov.iyu.kfin.ledger.application.service.containeroptimized;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Service for coordinating persistence operations across WAL, Entry Records, and Snapshots
 * Handles transaction management and durability levels
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class PersistenceTransactionManager {

    private final WalEntryRepository walEntryRepository;
    private final EntryRecordRepository entryRecordRepository;
    private final EntrySnapshotRepository entrySnapshotRepository;

    private final AtomicLong sequenceGenerator = new AtomicLong(0);

    /**
     * Initialize sequence generator from database
     */
    public void initializeSequenceGenerator() {
        OptionalLong maxSequence = walEntryRepository.getMaxSequenceNumber();
        if (maxSequence.isPresent()) {
            sequenceGenerator.set(maxSequence.getAsLong());
            log.info("Initialized sequence generator to {}", maxSequence.getAsLong());
        } else {
            log.info("No existing WAL entries, sequence generator starts at 0");
        }
    }

    /**
     * Process double-entry transaction with specified durability level
     */
    @Transactional
    public CompletableFuture<DoubleEntryResult> processDoubleEntry(
        UUID debitAccountId, UUID creditAccountId, long amount,
        String currencyCode, DurabilityLevel durabilityLevel,
        UUID transactionId, UUID idempotencyKey) {

        long startTime = System.nanoTime();

        try {
            // 1. Check idempotency first
            Optional<EntryRecord> existingEntry = entryRecordRepository.findByIdempotencyKey(idempotencyKey);
            if (existingEntry.isPresent()) {
                log.info("Idempotent operation detected for key {}", idempotencyKey);
                return CompletableFuture.completedFuture(DoubleEntryResult.fromExisting(existingEntry.get()));
            }

            LocalDate operationDate = LocalDate.now();
            long sequenceNumber = sequenceGenerator.incrementAndGet();

            // 2. Write to WAL if required by durability level
            WalEntry walEntry = null;
            if (durabilityLevel.requiresWAL()) {
                walEntry = WalEntry.create(debitAccountId, creditAccountId, amount, currencyCode,
                    operationDate, transactionId, idempotencyKey, sequenceNumber);
                walEntry = walEntryRepository.save(walEntry);
                log.debug("WAL entry saved: {}", walEntry.getId());
            }

            // 3. Create entry records
            EntryRecord debitEntry = EntryRecord.createDebit(debitAccountId, transactionId, amount,
                operationDate, idempotencyKey, currencyCode);
            EntryRecord creditEntry = EntryRecord.createCredit(creditAccountId, transactionId, amount,
                operationDate, idempotencyKey, currencyCode);

            List<EntryRecord> entries = entryRecordRepository.saveAll(List.of(debitEntry, creditEntry));
            log.debug("Entry records saved: debit={}, credit={}", debitEntry.getId(), creditEntry.getId());

            // 4. Update WAL status to PROCESSED if applicable
            if (walEntry != null) {
                walEntry.markAsProcessed();
                walEntryRepository.updateStatus(walEntry.getId(), WalEntry.WalStatus.PROCESSED, null);
            }

            // 5. Schedule post-commit snapshot creation if needed
            schedulePostCommitSnapshotCreation(debitAccountId, creditAccountId, transactionId);

            long processingTime = System.nanoTime() - startTime;

            return CompletableFuture.completedFuture(
                DoubleEntryResult.success(entries.get(0), entries.get(1),
                    walEntry, processingTime, sequenceNumber));

        } catch (Exception e) {
            log.error("Failed to process double-entry transaction: debit={}, credit={}, amount={}, tx={}",
                debitAccountId, creditAccountId, amount, transactionId, e);

            long processingTime = System.nanoTime() - startTime;
            return CompletableFuture.completedFuture(
                DoubleEntryResult.failure(e.getMessage(), processingTime));
        }
    }

    /**
     * Recover operations from WAL
     */
    @Transactional
    public CompletableFuture<RecoveryResult> recoverFromWal(int batchSize) {
        try {
            List<WalEntry> pendingEntries = walEntryRepository.findPendingEntries(batchSize);

            if (pendingEntries.isEmpty()) {
                return CompletableFuture.completedFuture(RecoveryResult.noEntries());
            }

            log.info("Recovering {} entries from WAL", pendingEntries.size());

            int successCount = 0;
            int failureCount = 0;
            List<String> errors = new ArrayList<>();

            for (WalEntry walEntry : pendingEntries) {
                try {
                    // Mark as processing
                    walEntry.markAsProcessing();
                    walEntryRepository.updateStatus(walEntry.getId(), WalEntry.WalStatus.PROCESSING, null);

                    // Create entry records
                    EntryRecord debitEntry = EntryRecord.createDebit(
                        walEntry.getDebitAccountId(), walEntry.getTransactionId(), walEntry.getAmount(),
                        walEntry.getOperationDate(), walEntry.getIdempotencyKey(), walEntry.getCurrencyCode());

                    EntryRecord creditEntry = EntryRecord.createCredit(
                        walEntry.getCreditAccountId(), walEntry.getTransactionId(), walEntry.getAmount(),
                        walEntry.getOperationDate(), walEntry.getIdempotencyKey(), walEntry.getCurrencyCode());

                    entryRecordRepository.saveAll(List.of(debitEntry, creditEntry));

                    // Mark as recovered
                    walEntry.markAsRecovered();
                    walEntryRepository.updateStatus(walEntry.getId(), WalEntry.WalStatus.RECOVERED, null);

                    successCount++;
                    log.debug("Recovered WAL entry: {}", walEntry.getId());

                } catch (Exception e) {
                    String errorMsg = "Recovery failed for WAL entry " + walEntry.getId() + ": " + e.getMessage();
                    errors.add(errorMsg);

                    walEntry.markAsFailed(errorMsg);
                    walEntryRepository.updateStatus(walEntry.getId(), WalEntry.WalStatus.FAILED, errorMsg);

                    failureCount++;
                    log.error("Failed to recover WAL entry {}", walEntry.getId(), e);
                }
            }

            return CompletableFuture.completedFuture(
                RecoveryResult.completed(successCount, failureCount, errors));

        } catch (Exception e) {
            log.error("WAL recovery process failed", e);
            return CompletableFuture.completedFuture(
                RecoveryResult.failure("WAL recovery failed: " + e.getMessage()));
        }
    }

    /**
     * Create snapshot for account
     */
    @Transactional
    public CompletableFuture<EntrySnapshot> createSnapshot(UUID accountId, LocalDate operationDate) {
        try {
            // Get latest ordinal for this account
            OptionalLong latestOrdinal = entryRecordRepository.getLatestOrdinal(accountId);
            if (latestOrdinal.isEmpty()) {
                log.debug("No entries found for account {}, skipping snapshot", accountId);
                return CompletableFuture.completedFuture(null);
            }

            long ordinal = latestOrdinal.getAsLong();

            // Calculate current balance
            long balance = entryRecordRepository.calculateBalance(accountId, ordinal);

            // Get operations count (simplified - using ordinal as approximation)
            int operationsCount = (int) ordinal;

            // Find last entry record ID (would need additional query in real implementation)
            List<EntryRecord> recentEntries = entryRecordRepository.findByAccountId(accountId, 1);
            UUID lastEntryRecordId = recentEntries.isEmpty() ? null : recentEntries.get(0).getId();

            // Create snapshot
            EntrySnapshot snapshot = EntrySnapshot.create(accountId, operationDate, balance,
                lastEntryRecordId, ordinal, operationsCount);

            EntrySnapshot savedSnapshot = entrySnapshotRepository.save(snapshot);
            log.info("Created snapshot for account {}: balance={}, ordinal={}",
                accountId, balance, ordinal);

            return CompletableFuture.completedFuture(savedSnapshot);

        } catch (Exception e) {
            log.error("Failed to create snapshot for account {}", accountId, e);
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Get current balance using snapshots for optimization
     */
    public CompletableFuture<BalanceResult> getCurrentBalance(UUID accountId) {
        try {
            // Try to find latest snapshot first
            Optional<EntrySnapshot> latestSnapshot = entrySnapshotRepository.findLatestByAccountId(accountId);

            if (latestSnapshot.isPresent()) {
                EntrySnapshot snapshot = latestSnapshot.get();
                OptionalLong currentOrdinal = entryRecordRepository.getLatestOrdinal(accountId);

                if (currentOrdinal.isPresent() && currentOrdinal.getAsLong() > snapshot.getLastEntryOrdinal()) {
                    // Calculate balance change since snapshot
                    long balanceChange = entryRecordRepository.calculateBalanceSince(
                        accountId, snapshot.getLastEntryOrdinal(), currentOrdinal.getAsLong());

                    long currentBalance = snapshot.getBalance() + balanceChange;
                    log.debug("Calculated balance from snapshot for account {}: snapshot={}, change={}, current={}",
                        accountId, snapshot.getBalance(), balanceChange, currentBalance);

                    return CompletableFuture.completedFuture(
                        BalanceResult.fromSnapshot(currentBalance, snapshot, balanceChange));
                } else {
                    // Snapshot is current
                    return CompletableFuture.completedFuture(
                        BalanceResult.fromSnapshot(snapshot.getBalance(), snapshot, 0L));
                }
            } else {
                // No snapshot, calculate from all entries
                OptionalLong currentOrdinal = entryRecordRepository.getLatestOrdinal(accountId);
                if (currentOrdinal.isPresent()) {
                    long balance = entryRecordRepository.calculateBalance(accountId, currentOrdinal.getAsLong());
                    return CompletableFuture.completedFuture(BalanceResult.fromCalculation(balance));
                } else {
                    return CompletableFuture.completedFuture(BalanceResult.zero());
                }
            }

        } catch (Exception e) {
            log.error("Failed to get current balance for account {}", accountId, e);
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Batch get current balances for multiple accounts
     */
    public CompletableFuture<Map<UUID, BalanceResult>> getCurrentBalances(Set<UUID> accountIds) {
        if (accountIds.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }

        try {
            // Get latest snapshots for all accounts
            Map<UUID, EntrySnapshot> snapshots = entrySnapshotRepository.findLatestByAccountIds(accountIds);

            Map<UUID, BalanceResult> results = new HashMap<>();

            for (UUID accountId : accountIds) {
                try {
                    EntrySnapshot snapshot = snapshots.get(accountId);

                    if (snapshot != null) {
                        OptionalLong currentOrdinal = entryRecordRepository.getLatestOrdinal(accountId);

                        if (currentOrdinal.isPresent() && currentOrdinal.getAsLong() > snapshot.getLastEntryOrdinal()) {
                            long balanceChange = entryRecordRepository.calculateBalanceSince(
                                accountId, snapshot.getLastEntryOrdinal(), currentOrdinal.getAsLong());
                            long currentBalance = snapshot.getBalance() + balanceChange;
                            results.put(accountId, BalanceResult.fromSnapshot(currentBalance, snapshot, balanceChange));
                        } else {
                            results.put(accountId, BalanceResult.fromSnapshot(snapshot.getBalance(), snapshot, 0L));
                        }
                    } else {
                        OptionalLong currentOrdinal = entryRecordRepository.getLatestOrdinal(accountId);
                        if (currentOrdinal.isPresent()) {
                            long balance = entryRecordRepository.calculateBalance(accountId, currentOrdinal.getAsLong());
                            results.put(accountId, BalanceResult.fromCalculation(balance));
                        } else {
                            results.put(accountId, BalanceResult.zero());
                        }
                    }
                } catch (Exception e) {
                    log.error("Failed to get balance for account {} in batch", accountId, e);
                    results.put(accountId, BalanceResult.error(e.getMessage()));
                }
            }

            return CompletableFuture.completedFuture(results);

        } catch (Exception e) {
            log.error("Failed to batch get balances for {} accounts", accountIds.size(), e);
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Schedule snapshot creation after transaction commits
     */
    private void schedulePostCommitSnapshotCreation(UUID debitAccountId, UUID creditAccountId, UUID transactionId) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    CompletableFuture.runAsync(() -> {
                        try {
                            // Check if accounts need snapshots (simplified logic)
                            LocalDate today = LocalDate.now();

                            // This is a simplified check - in reality would be more sophisticated
                            if (shouldCreateSnapshot(debitAccountId)) {
                                createSnapshot(debitAccountId, today);
                            }

                            if (shouldCreateSnapshot(creditAccountId)) {
                                createSnapshot(creditAccountId, today);
                            }

                        } catch (Exception e) {
                            log.warn("Post-commit snapshot creation failed for transaction {}", transactionId, e);
                        }
                    });
                }
            });
        }
    }

    /**
     * Simple heuristic to determine if snapshot should be created
     * In real implementation, this would be more sophisticated
     */
    private boolean shouldCreateSnapshot(UUID accountId) {
        try {
            Optional<EntrySnapshot> latestSnapshot = entrySnapshotRepository.findLatestByAccountId(accountId);

            if (latestSnapshot.isEmpty()) {
                return true; // No snapshot exists
            }

            EntrySnapshot snapshot = latestSnapshot.get();
            OptionalLong currentOrdinal = entryRecordRepository.getLatestOrdinal(accountId);

            if (currentOrdinal.isPresent()) {
                long operationsSinceSnapshot = currentOrdinal.getAsLong() - snapshot.getLastEntryOrdinal();
                return operationsSinceSnapshot >= 100; // Threshold of 100 operations
            }

            return false;

        } catch (Exception e) {
            log.warn("Error checking if snapshot needed for account {}", accountId, e);
            return false;
        }
    }

    // Result classes
    public static class DoubleEntryResult {
        private boolean success;
        private EntryRecord debitEntry;
        private EntryRecord creditEntry;
        private WalEntry walEntry;
        private String errorMessage;
        private long processingTimeNanos;
        private long sequenceNumber;

        public static DoubleEntryResult success(EntryRecord debitEntry, EntryRecord creditEntry,
                                                WalEntry walEntry, long processingTimeNanos, long sequenceNumber) {
            DoubleEntryResult result = new DoubleEntryResult();
            result.success = true;
            result.debitEntry = debitEntry;
            result.creditEntry = creditEntry;
            result.walEntry = walEntry;
            result.processingTimeNanos = processingTimeNanos;
            result.sequenceNumber = sequenceNumber;
            return result;
        }

        public static DoubleEntryResult failure(String errorMessage, long processingTimeNanos) {
            DoubleEntryResult result = new DoubleEntryResult();
            result.success = false;
            result.errorMessage = errorMessage;
            result.processingTimeNanos = processingTimeNanos;
            return result;
        }

        public static DoubleEntryResult fromExisting(EntryRecord existingEntry) {
            DoubleEntryResult result = new DoubleEntryResult();
            result.success = true;
            result.debitEntry = existingEntry;
            // Would need to find the corresponding credit entry in real implementation
            return result;
        }

        // Getters
        public boolean isSuccess() { return success; }
        public EntryRecord getDebitEntry() { return debitEntry; }
        public EntryRecord getCreditEntry() { return creditEntry; }
        public WalEntry getWalEntry() { return walEntry; }
        public String getErrorMessage() { return errorMessage; }
        public long getProcessingTimeNanos() { return processingTimeNanos; }
        public long getSequenceNumber() { return sequenceNumber; }
    }

    public static class RecoveryResult {
        private boolean success;
        private int successCount;
        private int failureCount;
        private List<String> errors;
        private String errorMessage;

        public static RecoveryResult completed(int successCount, int failureCount, List<String> errors) {
            RecoveryResult result = new RecoveryResult();
            result.success = true;
            result.successCount = successCount;
            result.failureCount = failureCount;
            result.errors = errors;
            return result;
        }

        public static RecoveryResult noEntries() {
            RecoveryResult result = new RecoveryResult();
            result.success = true;
            result.successCount = 0;
            result.failureCount = 0;
            result.errors = Collections.emptyList();
            return result;
        }

        public static RecoveryResult failure(String errorMessage) {
            RecoveryResult result = new RecoveryResult();
            result.success = false;
            result.errorMessage = errorMessage;
            result.errors = Collections.emptyList();
            return result;
        }

        // Getters
        public boolean isSuccess() { return success; }
        public int getSuccessCount() { return successCount; }
        public int getFailureCount() { return failureCount; }
        public List<String> getErrors() { return errors != null ? errors : Collections.emptyList(); }
        public String getErrorMessage() { return errorMessage; }
    }

    public static class BalanceResult {
        private long balance;
        private boolean fromSnapshot;
        private EntrySnapshot snapshot;
        private long balanceChange;
        private String errorMessage;

        public static BalanceResult fromSnapshot(long balance, EntrySnapshot snapshot, long balanceChange) {
            BalanceResult result = new BalanceResult();
            result.balance = balance;
            result.fromSnapshot = true;
            result.snapshot = snapshot;
            result.balanceChange = balanceChange;
            return result;
        }

        public static BalanceResult fromCalculation(long balance) {
            BalanceResult result = new BalanceResult();
            result.balance = balance;
            result.fromSnapshot = false;
            return result;
        }

        public static BalanceResult zero() {
            BalanceResult result = new BalanceResult();
            result.balance = 0L;
            result.fromSnapshot = false;
            return result;
        }

        public static BalanceResult error(String errorMessage) {
            BalanceResult result = new BalanceResult();
            result.errorMessage = errorMessage;
            return result;
        }

        // Getters
        public long getBalance() { return balance; }
        public boolean isFromSnapshot() { return fromSnapshot; }
        public EntrySnapshot getSnapshot() { return snapshot; }
        public long getBalanceChange() { return balanceChange; }
        public String getErrorMessage() { return errorMessage; }
        public boolean hasError() { return errorMessage != null; }
    }
}