package org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.api.repository;

import org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.api.model.result.EntriesSummaryResult;

import java.time.LocalDate;
import java.util.UUID;

public interface EntryRecordRepository {

    int entriesCount(UUID accountId, LocalDate operationDate);

    long balance(UUID accountId, LocalDate operationDate);

    EntriesSummaryResult entriesSummary(UUID accountId, LocalDate operationDate);

    void insert(
        UUID debitAccountId,
        UUID creditAccountId,
        LocalDate operationDate,
        UUID transactionId,
        long amount,
        String currencyCode,
        UUID idempotencyKey
    );

    /**
     * Checks if records exist for specific transaction ID.
     *
     * @param transactionId Transaction ID to check
     * @return true if records found
     */
    boolean existsByTransactionId(UUID transactionId);

    /**
     * Counts records for specific transaction ID.
     * Should be 2 for complete double-entry transaction.
     *
     * @param transactionId Transaction ID to count
     * @return number of records found
     */
    int countByTransactionId(UUID transactionId);
}
