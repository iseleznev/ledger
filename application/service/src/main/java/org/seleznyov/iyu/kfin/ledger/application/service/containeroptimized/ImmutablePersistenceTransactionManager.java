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
 * IMMUTABLE Persistence Transaction Manager
 * Coordinates persistence operations using only INSERT operations
 * No UPDATE or DELETE - all changes create new records
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ImmutablePersistenceTransactionManager {

    private final ImmutableWalEntryRepository walEntryRepository;
    private final ImmutableEntryRecordRepository entryRecordRepository;
    private final ImmutableEntrySnapshotRepository entrySnapshotRepository;

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
     * Process double-entry transaction with IMMUTABLE approach
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

            // 2. Write to WAL if required by durability level (IMMUTABLE - only insert)
            WalEntry walEntry = null;
            if (durabilityLevel.requiresWAL()) {
                walEntry = WalEntry.create(debitAccountId, creditAccountId, amount, currencyCode,
                    operationDate, transactionId, idempotencyKey, sequenceNumber);
                walEntry = walEntryRepository.save(walEntry);
                log.debug("WAL entry saved: {}", walEntry.getId());
            }

            // 3. Create entry records (IMMUTABLE - only insert)
            EntryRecord debitEntry = EntryRecord.createDebit(debitAccountId, transactionId, amount,
                operationDate, idempotencyKey, currencyCode);
            EntryRecord creditEntry = EntryRecord.createCredit(creditAccountId, transactionId, amount,
                operationDate, idempotencyKey, currencyCode);

            List<EntryRecord> entries = entryRecordRepository.saveAll(List.of(debitEntry, creditEntry));
            log.debug("Entry records saved: debit={}, credit={}", debitEntry.getId(), creditEntry.getId());

            // 4. Change WAL status to PROCESSED if applicable (IMMUTABLE - create new version)
            if (walEntry != null) {
                WalEntry processedEntry = walEntryRepository.changeStatus(
                    walEntry.getTransactionId(), WalEntry.WalStatus.PROCESSED, null);
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
     * Create storno (reversal) transaction - IMMUTABLE approach
     */
    @Transactional
    public CompletableFuture<StornoResult> createStornoTransaction(
        UUID originalTransactionId, UUID newTransactionId, UUID newIdempotencyKey,
        String reason, DurabilityLevel durabilityLevel) {

        long startTime = System.nanoTime();

        try {
            // Create storno entries (opposite amounts)
            List<EntryRecord> stornoEntries = entryRecordRepository.createStornoEntries(
                originalTransactionId, newTransactionId, newIdempotencyKey);

            if (stornoEntries.isEmpty()) {
                throw new IllegalArgumentException("No entries found for transaction " + originalTransactionId);
            }

            // Create WAL entry for storno if required
            WalEntry stornoWalEntry = null;
            if (durabilityLevel.requiresWAL()) {
                // For storno, we create a WAL entry that references the reversal
                long sequenceNumber = sequenceGenerator.incrementAndGet();

                // Get original amounts for WAL (simplified - in real system would be more complex)
                EntryRecord debitStorno = stornoEntries.stream()
                    .filter(e -> e.getEntryType() == EntryRecord.EntryType.DEBIT)
                    .findFirst().orElseThrow();
                EntryRecord creditStorno = stornoEntries.stream()
                    .filter(e -> e.getEntryType() == EntryRecord.EntryType.CREDIT)
                    .findFirst().orElseThrow();

                stornoWalEntry = WalEntry.create(
                    debitStorno.getAccountId(), creditStorno.getAccountId(),
                    debitStorno.getAmount(), debitStorno.getCurrencyCode(),
                    LocalDate.now(), newTransactionId, newIdempotencyKey, sequenceNumber);

                stornoWalEntry = walEntryRepository.save(stornoWalEntry);

                // Mark as processed immediately
                stornoWalEntry = walEntryRepository.changeStatus(
                    stornoWalEntry.getTransactionId(), WalEntry.WalStatus.PROCESSED, null);
            }

            long processingTime = System.nanoTime() - startTime;

            log.info("Created storno transaction {} for original {}: {} entries",
                newTransactionId, originalTransactionId, stornoEntries.size());

            return CompletableFuture.completedFuture(
                StornoResult.success(originalTransactionId, newTransactionId,
                    stornoEntries, stornoWalEntry, reason, processingTime));

        } catch (Exception e) {
            log.error("Failed to create storno transaction for {}", originalTransactionId, e);
            long processingTime = System.nanoTime() - startTime;
            return CompletableFuture.completedFuture(
                StornoResult.failure(originalTransactionId, e.getMessage(), processingTime));
        }
    }

    /**
     * Recover operations from WAL using IMMUTABLE approach
     */
    @Transactional
    public CompletableFuture<RecoveryResult> recoverFromWal(int batchSize) {
        try {
            List<WalEntry> pendingEntries = walEntryRepository.findCurrentPendingEntries(batchSize);

            if (pendingEntries.isEmpty()) {
                return CompletableFuture.completedFuture(RecoveryResult.noEntries());
            }

            log.info("Recovering {} entries from WAL using immutable approach", pendingEntries.size());

            int successCount = 0;
            int failureCount = 0;
            List<String> errors = new ArrayList<>();

            for (WalEntry walEntry : pendingEntries) {
                try {
                    // Change status to PROCESSING (creates new version)
                    WalEntry processingEntry = walEntryRepository.changeStatus(
                        walEntry.getTransactionId(), WalEntry.WalStatus.PROCESSING, null);

                    // Create entry records from WAL data
                    EntryRecord debitEntry = EntryRecord.createDebit(
                        walEntry.getDebitAccountId(), walEntry.getTransactionId(), walEntry.getAmount(),
                        walEntry.getOperationDate(), walEntry.getIdempotencyKey(), walEntry.getCurrencyCode());

                    EntryRecord creditEntry = EntryRecord.createCredit(
                        walEntry.getCreditAccountId(), walEntry.getTransactionId(), walEntry.getAmount(),
                        walEntry.getOperationDate(), walEntry.getIdempotencyKey(), walEntry.getCurrencyCode());

                    entryRecordRepository.saveAll(List.of(debitEntry, creditEntry));

                    // Mark as RECOVERED (creates new version)
                    walEntryRepository.changeStatus(
                        walEntry.getTransactionId(), WalEntry.WalStatus.RECOVERED, null);

                    successCount++;
                    log.debug("Recovered WAL entry: {}", walEntry.getId());

                } catch (Exception e) {
                    String errorMsg = "Recovery failed for WAL entry " + walEntry.getId() + ": " + e.getMessage();
                    errors.add(errorMsg);

                    // Mark as FAILED (creates new version)
                    try {
                        walEntryRepository.changeStatus(
                            walEntry.getTransactionId(), WalEntry.WalStatus.FAILED, errorMsg);
                    } catch (Exception statusUpdateError) {
                        log.error("Failed to update WAL status to FAILED for {}", walEntry.getId(), statusUpdateError);
                    }

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
     * Create snapshot for account using IMMUTABLE approach
     */
    @Transactional
    public CompletableFuture<EntrySnapshot> createSnapshot(UUID accountId, LocalDate operationDate) {
        try {
            // Get latest active ordinal for this account
            OptionalLong latestOrdinal = entryRecordRepository.getLatestActiveOrdinal(accountId);
            if (latestOrdinal.isEmpty()) {
                log.debug("No active entries found for account {}, skipping snapshot", accountId);
                return CompletableFuture.completedFuture(null);
            }

            long ordinal = latestOrdinal.getAsLong();

            // Calculate current active balance
            long balance = entryRecordRepository.calculateActiveBalance(accountId, ordinal);

            // Get active operations count (simplified - using ordinal as approximation)
            int operationsCount = (int) ordinal;

            // Find last entry record ID (would need additional query in real implementation)
            List<EntryRecord> recentEntries = entryRecordRepository.findActiveByAccountId(accountId, 1);
            UUID lastEntryRecordId = recentEntries.isEmpty() ? null : recentEntries.get(0).getId();

            // Create new snapshot (potentially superseding existing current ones)
            EntrySnapshot newSnapshot = EntrySnapshot.create(accountId, operationDate, balance,
                lastEntryRecordId, ordinal, operationsCount);

            // Check if there's already a current snapshot for this date
            Optional<EntrySnapshot> existingCurrent = entrySnapshotRepository
                .findCurrentByAccountAndDate(accountId, operationDate);

            EntrySnapshot savedSnapshot;
            if (existingCurrent.isPresent()) {
                // Create superseding snapshot (marks previous as non-current)
                savedSnapshot = entrySnapshotRepository.createSuperseding(newSnapshot);
                log.info("Created superseding snapshot for account {}: balance={}, ordinal={} (replaced previous)",
                    accountId, balance, ordinal);
            } else {
                // Just save new current snapshot
                savedSnapshot = entrySnapshotRepository.save(newSnapshot);
                log.info("Created new snapshot for account {}: balance={}, ordinal={}",
                    accountId, balance, ordinal);
            }

            return CompletableFuture.completedFuture(savedSnapshot);

        } catch (Exception e) {
            log.error("Failed to create snapshot for account {}", accountId, e);
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Get current balance using snapshots for optimization (IMMUTABLE approach)
     */
    public CompletableFuture<BalanceResult> getCurrentBalance(UUID accountId) {
        try {
            // Try to find latest current snapshot first
            Optional<EntrySnapshot> latestSnapshot = entrySnapshotRepository.findCurrentLatestByAccountId(accountId);

            if (latestSnapshot.isPresent()) {
                EntrySnapshot snapshot = latestSnapshot.get();
                OptionalLong currentOrdinal = entryRecordRepository.getLatestActiveOrdinal(accountId);

                if (currentOrdinal.isPresent() && currentOrdinal.getAsLong() > snapshot.getLastEntryOrdinal()) {
                    // Calculate balance change since snapshot (only from active entries)
                    long balanceChange = entryRecordRepository.calculateBalanceChangeSince(
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
                // No snapshot, calculate from all active entries
                OptionalLong currentOrdinal = entryRecordRepository.getLatestActiveOrdinal(accountId);
                if (currentOrdinal.isPresent()) {
                    long balance = entryRecordRepository.calculateActiveBalance(accountId, currentOrdinal.getAsLong());
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
     * Batch get current balances for multiple accounts (IMMUTABLE approach)
     */
    public CompletableFuture<Map<UUID, BalanceResult>> getCurrentBalances(Set<UUID> accountIds) {
        if (accountIds.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }

        try {
            // Get current latest snapshots for all accounts
            Map<UUID, EntrySnapshot> snapshots = entrySnapshotRepository.findCurrentLatestByAccountIds(accountIds);

            Map<UUID, BalanceResult> results = new HashMap<>();

            for (UUID accountId : accountIds) {
                try {
                    EntrySnapshot snapshot = snapshots.get(accountId);

                    if (snapshot != null) {
                        OptionalLong currentOrdinal = entryRecordRepository.getLatestActiveOrdinal(accountId);

                        if (currentOrdinal.isPresent() && currentOrdinal.getAsLong() > snapshot.getLastEntryOrdinal()) {
                            long balanceChange = entryRecordRepository.calculateBalanceChangeSince(
                                accountId, snapshot.getLastEntryOrdinal(), currentOrdinal.getAsLong());
                            long currentBalance = snapshot.getBalance() + balanceChange;
                            results.put(accountId, BalanceResult.fromSnapshot(currentBalance, snapshot, balanceChange));
                        } else {
                            results.put(accountId, BalanceResult.fromSnapshot(snapshot.getBalance(), snapshot, 0L));
                        }
                    } else {
                        OptionalLong currentOrdinal = entryRecordRepository.getLatestActiveOrdinal(accountId);
                        if (currentOrdinal.isPresent()) {
                            long balance = entryRecordRepository.calculateActiveBalance(accountId, currentOrdinal.getAsLong());
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
     * Get complete audit trail for transaction (IMMUTABLE benefits)
     */
    public CompletableFuture<AuditTrailResult> getTransactionAuditTrail(UUID transactionId) {
        try {
            // Get entry records audit trail
            Map<String, List<EntryRecord>> entryAuditTrail = entryRecordRepository.getTransactionAuditTrail(transactionId);

            // Get WAL version history
            List<WalEntry> walVersions = walEntryRepository.getVersionHistory(transactionId);

            return CompletableFuture.completedFuture(
                AuditTrailResult.create(transactionId, entryAuditTrail, walVersions));

        } catch (Exception e) {
            log.error("Failed to get audit trail for transaction {}", transactionId, e);
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
                            LocalDate today = LocalDate.now();

                            // Check if accounts need snapshots using more sophisticated logic
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
     * Enhanced heuristic to determine if snapshot should be created
     */
    private boolean shouldCreateSnapshot(UUID accountId) {
        try {
            Optional<EntrySnapshot> latestSnapshot = entrySnapshotRepository.findCurrentLatestByAccountId(accountId);

            if (latestSnapshot.isEmpty()) {
                return true; // No snapshot exists
            }

            EntrySnapshot snapshot = latestSnapshot.get();
            OptionalLong currentOrdinal = entryRecordRepository.getLatestActiveOrdinal(accountId);

            if (currentOrdinal.isPresent()) {
                long activeOperationsSinceSnapshot = entryRecordRepository.countActiveOperationsSince(
                    accountId, snapshot.getLastEntryOrdinal());
                return activeOperationsSinceSnapshot >= 100; // Threshold of 100 active operations
            }

            return false;

        } catch (Exception e) {
            log.warn("Error checking if snapshot needed for account {}", accountId, e);
            return false;
        }
    }

    /**
     * Get all active account IDs (delegates to repository)
     */
    public Set<UUID> getAllActiveAccountIds() {
        return entryRecordRepository.getAllActiveAccountIds();
    }

    /**
     * Get account IDs with recent activity (for targeted reconciliation)
     */
    public Set<UUID> getAccountIdsWithActivitySince(java.time.LocalDate sinceDate) {
        return entryRecordRepository.getAccountIdsWithActivitySince(sinceDate);
    }

    public static class StornoResult {
        private boolean success;
        private UUID originalTransactionId;
        private UUID stornoTransactionId;
        private List<EntryRecord> stornoEntries;
        private WalEntry stornoWalEntry;
        private String reason;
        private String errorMessage;
        private long processingTimeNanos;

        public static StornoResult success(UUID originalTransactionId, UUID stornoTransactionId,
                                           List<EntryRecord> stornoEntries, WalEntry stornoWalEntry, String reason, long processingTimeNanos) {
            StornoResult result = new StornoResult();
            result.success = true;
            result.originalTransactionId = originalTransactionId;
            result.stornoTransactionId = stornoTransactionId;
            result.stornoEntries = stornoEntries;
            result.stornoWalEntry = stornoWalEntry;
            result.reason = reason;
            result.processingTimeNanos = processingTimeNanos;
            return result;
        }

        public static StornoResult failure(UUID originalTransactionId, String errorMessage, long processingTimeNanos) {
            StornoResult result = new StornoResult();
            result.success = false;
            result.originalTransactionId = originalTransactionId;
            result.errorMessage = errorMessage;
            result.processingTimeNanos = processingTimeNanos;
            return result;
        }

        // Getters
        public boolean isSuccess() { return success; }
        public UUID getOriginalTransactionId() { return originalTransactionId; }
        public UUID getStornoTransactionId() { return stornoTransactionId; }
        public List<EntryRecord> getStornoEntries() { return stornoEntries != null ? stornoEntries : Collections.emptyList(); }
        public WalEntry getStornoWalEntry() { return stornoWalEntry; }
        public String getReason() { return reason; }
        public String getErrorMessage() { return errorMessage; }
        public long getProcessingTimeNanos() { return processingTimeNanos; }
    }

    public static class AuditTrailResult {
        private UUID transactionId;
        private Map<String, List<EntryRecord>> entryAuditTrail;
        private List<WalEntry> walVersionHistory;

        public static AuditTrailResult create(UUID transactionId,
                                              Map<String, List<EntryRecord>> entryAuditTrail, List<WalEntry> walVersionHistory) {
            AuditTrailResult result = new AuditTrailResult();
            result.transactionId = transactionId;
            result.entryAuditTrail = entryAuditTrail;
            result.walVersionHistory = walVersionHistory;
            return result;
        }

        // Getters
        public UUID getTransactionId() { return transactionId; }
        public Map<String, List<EntryRecord>> getEntryAuditTrail() {
            return entryAuditTrail != null ? entryAuditTrail : Collections.emptyMap();
        }
        public List<WalEntry> getWalVersionHistory() {
            return walVersionHistory != null ? walVersionHistory : Collections.emptyList();
        }
    }

    // Reuse existing result classes from the previous version
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