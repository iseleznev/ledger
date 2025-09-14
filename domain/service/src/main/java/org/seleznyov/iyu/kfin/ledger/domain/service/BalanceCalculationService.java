package org.seleznyov.iyu.kfin.ledger.domain.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import org.seleznyov.iyu.kfin.ledger.domain.model.snapshot.EntriesSnapshot;
import org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.api.repository.EntryRecordRepository;
import org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.api.repository.EntriesSnapshotRepository;


import java.time.LocalDate;
import java.util.UUID;

/**
 * Domain service responsible for balance calculations using snapshot-optimized approach.
 * Implements the formula: current_balance = snapshot.balance + SUM(operations after snapshot)
 */
@Service
@RequiredArgsConstructor
public class BalanceCalculationService {

    private final EntryRecordRepository entryRecordRepository;
    private final EntriesSnapshotRepository snapshotRepository;

    /**
     * Calculate current balance for account using snapshot optimization.
     *
     * @param accountId Account to calculate balance for
     * @param operationDate Date up to which to calculate balance
     * @return Current balance amount
     */
    public long calculateBalance(UUID accountId, LocalDate operationDate) {
        return entryRecordRepository.balance(accountId, operationDate);
    }

    /**
     * Calculate balance with detailed breakdown for debugging/monitoring.
     *
     * @param accountId Account to calculate balance for
     * @param operationDate Date up to which to calculate balance
     * @return Detailed balance breakdown
     */
    public BalanceBreakdown calculateBalanceWithBreakdown(UUID accountId, LocalDate operationDate) {
        try {
            // Try to get latest snapshot
            EntriesSnapshot snapshot = snapshotRepository.lastSnapshot(accountId, operationDate);

            // Calculate operations after snapshot
            var summary = entryRecordRepository.entriesSummary(accountId, operationDate);

            return new BalanceBreakdown(
                summary.balance(),
                snapshot.balance(),
                summary.balance() - snapshot.balance(), // delta since snapshot
                summary.operationsCount(),
                snapshot.operationDay(),
                true // has snapshot
            );
        } catch (IllegalStateException e) {
            // No snapshot exists, calculate from all entries
            long balance = entryRecordRepository.balance(accountId, operationDate);
            int entriesCount = entryRecordRepository.entriesCount(accountId, operationDate);

            return new BalanceBreakdown(
                balance,
                0L, // no snapshot
                balance, // all operations
                entriesCount,
                null, // no snapshot date
                false // no snapshot
            );
        }
    }

    /**
     * Check if account has sufficient balance for debit operation.
     *
     * @param accountId Account to check
     * @param amount Amount to debit
     * @param operationDate Operation date
     * @return true if sufficient balance exists
     */
    public boolean hasSufficientBalance(UUID accountId, long amount, LocalDate operationDate) {
        if (amount < 0) {
            throw new IllegalArgumentException("Amount must be positive");
        }

        long currentBalance = calculateBalance(accountId, operationDate);
        return currentBalance >= amount;
    }

    /**
     * Calculate available balance (current balance) for account.
     * For ledger systems, this is typically the same as current balance,
     * but can be extended for credit limits, holds, etc.
     *
     * @param accountId Account ID
     * @param operationDate Operation date
     * @return Available balance
     */
    public long calculateAvailableBalance(UUID accountId, LocalDate operationDate) {
        return calculateBalance(accountId, operationDate);
    }

    /**
     * Detailed balance breakdown for monitoring and debugging.
     */
    public record BalanceBreakdown(
        long currentBalance,
        long snapshotBalance,
        long deltaFromSnapshot,
        int operationsCount,
        LocalDate snapshotDate,
        boolean hasSnapshot
    ) {}
}