package org.seleznyov.iyu.kfin.ledger.application.service.containeroptimized;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Recovery service for disaster recovery and data consistency
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class LedgerRecoveryService {

    private final ContainerOptimizedLedgerService ledgerService;
    private final ContainerOptimizedBalanceStorage balanceStorage;
    private final ContainerOptimizedWAL walStorage;
    private final ContainerOptimizedSnapshotService snapshotService;

    /**
     * Perform complete system recovery from WAL
     */
    public CompletableFuture<RecoveryResult> performFullRecovery() {
        log.info("Starting full system recovery from WAL...");
        long startTime = System.currentTimeMillis();

        return walStorage.getUnflushedEntries(Integer.MAX_VALUE)
            .thenCompose(this::processRecoveryEntries)
            .thenCompose(processingResult ->
                snapshotService.getAllSnapshots()
                    .thenCompose(snapshots -> validateConsistency(processingResult, snapshots))
            )
            .thenApply(validationResult -> {
                long endTime = System.currentTimeMillis();
                log.info("Full recovery completed in {}ms", endTime - startTime);
                return validationResult;
            })
            .exceptionally(error -> {
                log.error("Full recovery failed", error);
                return RecoveryResult.builder()
                    .success(false)
                    .errorMessage("Recovery failed: " + error.getMessage())
                    .build();
            });
    }

    /**
     * Recover specific accounts from WAL
     */
    public CompletableFuture<RecoveryResult> recoverAccounts(Set<UUID> accountIds) {
        log.info("Starting recovery for {} accounts", accountIds.size());

        return walStorage.getUnflushedEntries(Integer.MAX_VALUE)
            .thenApply(entries -> entries.stream()
                .filter(entry -> accountIds.contains(entry.getAccountId()))
                .collect(Collectors.toList()))
            .thenCompose(this::processRecoveryEntries)
            .exceptionally(error -> {
                log.error("Account recovery failed", error);
                return RecoveryResult.builder()
                    .success(false)
                    .errorMessage("Account recovery failed: " + error.getMessage())
                    .build();
            });
    }

    private CompletableFuture<RecoveryResult> processRecoveryEntries(List<TransactionEntry> entries) {
        if (entries.isEmpty()) {
            return CompletableFuture.completedFuture(
                RecoveryResult.builder()
                    .success(true)
                    .entriesProcessed(0)
                    .balancesRestored(0)
                    .build());
        }

        log.info("Processing {} recovery entries", entries.size());

        // Group entries by account for batch processing
        Map<UUID, List<TransactionEntry>> entriesByAccount = entries.stream()
            .collect(Collectors.groupingBy(TransactionEntry::getAccountId));

        List<CompletableFuture<Map<UUID, Long>>> recoveryFutures = entriesByAccount.entrySet().stream()
            .map(this::recoverAccountBalance)
            .collect(Collectors.toList());

        return CompletableFuture.allOf(recoveryFutures.toArray(new CompletableFuture[0]))
            .thenApply(ignored -> {
                Map<UUID, Long> recoveredBalances = new HashMap<>();
                recoveryFutures.forEach(future -> recoveredBalances.putAll(future.join()));

                return RecoveryResult.builder()
                    .success(true)
                    .entriesProcessed(entries.size())
                    .balancesRestored(recoveredBalances.size())
                    .recoveredBalances(recoveredBalances)
                    .build();
            });
    }

    private CompletableFuture<Map<UUID, Long>> recoverAccountBalance(
        Map.Entry<UUID, List<TransactionEntry>> accountEntry) {

        UUID accountId = accountEntry.getKey();
        List<TransactionEntry> entries = accountEntry.getValue();

        // Sort by sequence number to ensure proper order
        entries.sort(Comparator.comparing(TransactionEntry::getSequenceNumber));

        // Calculate total delta for this account
        long totalDelta = entries.stream().mapToLong(TransactionEntry::getDelta).sum();

        // Get current balance and apply recovery delta
        return balanceStorage.getBalance(accountId)
            .thenCompose(currentBalance -> {
                long baseBalance = currentBalance.orElse(0L);
                long recoveredBalance = baseBalance + totalDelta;

                return balanceStorage.updateBalance(accountId, recoveredBalance)
                    .thenApply(ignored -> Map.of(accountId, recoveredBalance));
            });
    }

    /**
     * Validate system consistency after recovery
     */
    private CompletableFuture<RecoveryResult> validateConsistency(
        RecoveryResult processingResult, Map<UUID, AccountBalanceSnapshot> snapshots) {

        log.info("Validating system consistency after recovery");

        if (!processingResult.isSuccess()) {
            return CompletableFuture.completedFuture(processingResult);
        }

        // Validate recovered balances against snapshots
        List<String> inconsistencies = new ArrayList<>();
        Map<UUID, Long> recoveredBalances = processingResult.getRecoveredBalances();

        for (Map.Entry<UUID, Long> balanceEntry : recoveredBalances.entrySet()) {
            UUID accountId = balanceEntry.getKey();
            Long currentBalance = balanceEntry.getValue();

            AccountBalanceSnapshot snapshot = snapshots.get(accountId);
            if (snapshot != null) {
                // Simplified consistency check - in reality this would be more complex
                if (Math.abs(currentBalance - snapshot.getBalance()) > 1000000) { // > $10k difference
                    inconsistencies.add("Account " + accountId + " balance inconsistency: " +
                        "current=" + currentBalance + ", snapshot=" + snapshot.getBalance());
                }
            }
        }

        return CompletableFuture.completedFuture(
            RecoveryResult.builder()
                .success(inconsistencies.isEmpty())
                .entriesProcessed(processingResult.getEntriesProcessed())
                .balancesRestored(processingResult.getBalancesRestored())
                .recoveredBalances(recoveredBalances)
                .inconsistencies(inconsistencies)
                .errorMessage(inconsistencies.isEmpty() ? null :
                    "Found " + inconsistencies.size() + " consistency issues")
                .build());
    }

    /**
     * Create system backup
     */
    public CompletableFuture<BackupResult> createBackup() {
        log.info("Creating system backup...");
        long startTime = System.currentTimeMillis();

        CompletableFuture<Map<UUID, Long>> balanceBackup = balanceStorage.createSnapshot();
        CompletableFuture<Map<UUID, AccountBalanceSnapshot>> snapshotBackup = snapshotService.getAllSnapshots();
        CompletableFuture<List<TransactionEntry>> walBackup = walStorage.getUnflushedEntries(Integer.MAX_VALUE);

        return CompletableFuture.allOf(balanceBackup, snapshotBackup, walBackup)
            .thenApply(ignored -> {
                long endTime = System.currentTimeMillis();

                return BackupResult.builder()
                    .success(true)
                    .balances(balanceBackup.join())
                    .snapshots(snapshotBackup.join())
                    .walEntries(walBackup.join())
                    .backupTimeMs(endTime - startTime)
                    .timestamp(endTime)
                    .build();
            })
            .exceptionally(error -> {
                log.error("Backup creation failed", error);
                return BackupResult.builder()
                    .success(false)
                    .errorMessage("Backup failed: " + error.getMessage())
                    .build();
            });
    }

    /**
     * Restore from backup
     */
    public CompletableFuture<RecoveryResult> restoreFromBackup(BackupResult backup) {
        if (!backup.isSuccess()) {
            return CompletableFuture.completedFuture(
                RecoveryResult.builder()
                    .success(false)
                    .errorMessage("Cannot restore from failed backup")
                    .build());
        }

        log.info("Restoring system from backup...");

        CompletableFuture<Integer> balanceRestore = balanceStorage.restoreFromSnapshot(backup.getBalances());
        CompletableFuture<Integer> snapshotRestore = snapshotService.restoreSnapshots(backup.getSnapshots());

        return CompletableFuture.allOf(balanceRestore, snapshotRestore)
            .thenApply(ignored -> {
                int balancesRestored = balanceRestore.join();
                int snapshotsRestored = snapshotRestore.join();

                log.info("Restore completed: {} balances, {} snapshots",
                    balancesRestored, snapshotsRestored);

                return RecoveryResult.builder()
                    .success(true)
                    .balancesRestored(balancesRestored)
                    .snapshotsRestored(snapshotsRestored)
                    .build();
            })
            .exceptionally(error -> {
                log.error("Restore from backup failed", error);
                return RecoveryResult.builder()
                    .success(false)
                    .errorMessage("Restore failed: " + error.getMessage())
                    .build();
            });
    }

    // Result classes
    public static class RecoveryResult {
        private boolean success;
        private int entriesProcessed;
        private int balancesRestored;
        private int snapshotsRestored;
        private Map<UUID, Long> recoveredBalances;
        private List<String> inconsistencies;
        private String errorMessage;

        // Builder pattern implementation
        public static Builder builder() { return new Builder(); }

        public static class Builder {
            private RecoveryResult result = new RecoveryResult();

            public Builder success(boolean success) { result.success = success; return this; }
            public Builder entriesProcessed(int count) { result.entriesProcessed = count; return this; }
            public Builder balancesRestored(int count) { result.balancesRestored = count; return this; }
            public Builder snapshotsRestored(int count) { result.snapshotsRestored = count; return this; }
            public Builder recoveredBalances(Map<UUID, Long> balances) { result.recoveredBalances = balances; return this; }
            public Builder inconsistencies(List<String> inconsistencies) { result.inconsistencies = inconsistencies; return this; }
            public Builder errorMessage(String message) { result.errorMessage = message; return this; }

            public RecoveryResult build() { return result; }
        }

        // Getters
        public boolean isSuccess() { return success; }
        public int getEntriesProcessed() { return entriesProcessed; }
        public int getBalancesRestored() { return balancesRestored; }
        public int getSnapshotsRestored() { return snapshotsRestored; }
        public Map<UUID, Long> getRecoveredBalances() { return recoveredBalances != null ? recoveredBalances : Map.of(); }
        public List<String> getInconsistencies() { return inconsistencies != null ? inconsistencies : List.of(); }
        public String getErrorMessage() { return errorMessage; }
    }

    public static class BackupResult {
        private boolean success;
        private Map<UUID, Long> balances;
        private Map<UUID, AccountBalanceSnapshot> snapshots;
        private List<TransactionEntry> walEntries;
        private long backupTimeMs;
        private long timestamp;
        private String errorMessage;

        public static Builder builder() { return new Builder(); }

        public static class Builder {
            private BackupResult result = new BackupResult();

            public Builder success(boolean success) { result.success = success; return this; }
            public Builder balances(Map<UUID, Long> balances) { result.balances = balances; return this; }
            public Builder snapshots(Map<UUID, AccountBalanceSnapshot> snapshots) { result.snapshots = snapshots; return this; }
            public Builder walEntries(List<TransactionEntry> entries) { result.walEntries = entries; return this; }
            public Builder backupTimeMs(long time) { result.backupTimeMs = time; return this; }
            public Builder timestamp(long timestamp) { result.timestamp = timestamp; return this; }
            public Builder errorMessage(String message) { result.errorMessage = message; return this; }

            public BackupResult build() { return result; }
        }

        // Getters
        public boolean isSuccess() { return success; }
        public Map<UUID, Long> getBalances() { return balances != null ? balances : Map.of(); }
        public Map<UUID, AccountBalanceSnapshot> getSnapshots() { return snapshots != null ? snapshots : Map.of(); }
        public List<TransactionEntry> getWalEntries() { return walEntries != null ? walEntries : List.of(); }
        public long getBackupTimeMs() { return backupTimeMs; }
        public long getTimestamp() { return timestamp; }
        public String getErrorMessage() { return errorMessage; }
    }
}