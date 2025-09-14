package org.seleznyov.iyu.kfin.ledger.application.service.containeroptimized;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Account balance snapshot entity
 * Maps to ledger.entries_snapshots table
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AccountBalanceSnapshot {

    private UUID id;
    private UUID accountId;
    private LocalDate operationDate;
    private long balance;
    private UUID lastEntryRecordId;
    private long lastEntryOrdinal;
    private int operationsCount;
    private long snapshotOrdinal;
    private OffsetDateTime createdAt;

    /**
     * Create a new snapshot from current state
     */
    public static AccountBalanceSnapshot create(UUID accountId, LocalDate operationDate,
                                                long balance, UUID lastEntryRecordId,
                                                long lastEntryOrdinal, int operationsCount) {
        return AccountBalanceSnapshot.builder()
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
     * Check if this snapshot is valid for calculating balance at given ordinal
     */
    public boolean isValidForOrdinal(long targetOrdinal) {
        return lastEntryOrdinal <= targetOrdinal;
    }

    /**
     * Check if snapshot is recent enough (within same day)
     */
    public boolean isRecentFor(LocalDate targetDate) {
        return operationDate.equals(targetDate) || operationDate.isBefore(targetDate);
    }

    /**
     * Calculate how many operations to process from this snapshot
     */
    public int getOperationsToProcess(long targetOrdinal) {
        if (targetOrdinal < lastEntryOrdinal) {
            throw new IllegalArgumentException("Target ordinal " + targetOrdinal +
                " is before snapshot ordinal " + lastEntryOrdinal);
        }
        return (int) (targetOrdinal - lastEntryOrdinal);
    }
}