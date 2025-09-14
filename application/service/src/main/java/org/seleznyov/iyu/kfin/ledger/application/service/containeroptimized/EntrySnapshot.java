package org.seleznyov.iyu.kfin.ledger.application.service.containeroptimized;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Entry Snapshot entity for balance optimization
 * Maps to ledger.entries_snapshots table
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EntrySnapshot {

    private UUID id;
    private UUID accountId;
    private LocalDate operationDate;
    private long balance; // Balance at snapshot time
    private UUID lastEntryRecordId; // Last processed entry record
    private long lastEntryOrdinal; // Last processed entry ordinal
    private int operationsCount; // Total operations processed at snapshot time
    private long snapshotOrdinal; // Snapshot order for same date
    private OffsetDateTime createdAt;

    /**
     * Create new snapshot
     */
    public static EntrySnapshot create(UUID accountId, LocalDate operationDate, long balance,
                                       UUID lastEntryRecordId, long lastEntryOrdinal, int operationsCount) {
        return EntrySnapshot.builder()
            .accountId(accountId)
            .operationDate(operationDate)
            .balance(balance)
            .lastEntryRecordId(lastEntryRecordId)
            .lastEntryOrdinal(lastEntryOrdinal)
            .operationsCount(operationsCount)
            .createdAt(OffsetDateTime.now())
            .build();
    }

    /**
     * Create superseding snapshot (marks previous as non-current)
     */
    public static EntrySnapshot createSuperseding(UUID accountId, LocalDate operationDate, long balance,
                                                  UUID lastEntryRecordId, long lastEntryOrdinal, int operationsCount) {
        return EntrySnapshot.builder()
            .accountId(accountId)
            .operationDate(operationDate)
            .balance(balance)
            .lastEntryRecordId(lastEntryRecordId)
            .lastEntryOrdinal(lastEntryOrdinal)
            .operationsCount(operationsCount)
            .createdAt(OffsetDateTime.now())
            .build();
    }

    /**
     * Check if snapshot is valid for calculating balance at given ordinal
     */
    public boolean isValidForOrdinal(long targetOrdinal) {
        return lastEntryOrdinal <= targetOrdinal;
    }

    /**
     * Check if snapshot is recent for given date
     */
    public boolean isRecentFor(LocalDate targetDate) {
        return operationDate.equals(targetDate) || operationDate.isBefore(targetDate);
    }

    /**
     * Calculate operations to process from this snapshot to target ordinal
     */
    public long getOperationsToProcess(long targetOrdinal) {
        if (targetOrdinal < lastEntryOrdinal) {
            throw new IllegalArgumentException("Target ordinal " + targetOrdinal +
                " is before snapshot ordinal " + lastEntryOrdinal);
        }
        return targetOrdinal - lastEntryOrdinal;
    }

    /**
     * Check if snapshot needs refresh based on operations since creation
     */
    public boolean needsRefresh(long currentOrdinal, int refreshThreshold) {
        return (currentOrdinal - lastEntryOrdinal) >= refreshThreshold;
    }

    /**
     * Validate snapshot data
     */
    public boolean isValid() {
        return accountId != null
            && operationDate != null
            && lastEntryRecordId != null
            && lastEntryOrdinal > 0
            && operationsCount >= 0;
    }

    /**
     * Get age in days
     */
    public long getAgeInDays() {
        return java.time.temporal.ChronoUnit.DAYS.between(operationDate, LocalDate.now());
    }

    /**
     * Check if snapshot is stale (older than specified days)
     */
    public boolean isStale(int maxAgeDays) {
        return getAgeInDays() > maxAgeDays;
    }
}