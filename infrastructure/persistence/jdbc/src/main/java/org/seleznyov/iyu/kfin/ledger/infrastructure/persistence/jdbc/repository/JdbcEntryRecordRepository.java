package org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.jdbc.repository;

import lombok.RequiredArgsConstructor;
import org.seleznyov.iyu.kfin.ledger.domain.model.entryrecord.EntryType;
import org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.api.model.result.EntriesSummaryResult;
import org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.api.repository.EntryRecordRepository;
import org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.jdbc.client.*;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class JdbcEntryRecordRepository implements EntryRecordRepository {

    private final EntryRecordsBalanceClient balanceClient;
    private final EntriesCountClient entriesCountClient;
    private final DoubleEntryRecordInsertClient insertClient;
    private final EntriesSummaryClient summaryClient;
    private final EntryRecordExistsByTransactionIdClient existsClient;      // Новый клиент
    private final EntryRecordCountByTransactionIdClient countByTxClient;    // Новый клиент

    @Override
    public int entriesCount(UUID accountId, LocalDate operationDate) {
        return entriesCountClient.entriesCount(accountId, operationDate);
    }

    @Override
    public void insert(
        UUID debitAccountId,
        UUID creditAccountId,
        LocalDate operationDate,
        UUID transactionId,
        long amount,
        String currencyCode,
        UUID idempotencyKey
    ) {
        final UUID firstAccountId = debitAccountId.compareTo(creditAccountId) < 0 ? debitAccountId : creditAccountId;
        final UUID secondAccountId = debitAccountId.compareTo(creditAccountId) < 0 ? creditAccountId : debitAccountId;
        final EntryType firstEntryType = debitAccountId == firstAccountId ? EntryType.DEBIT : EntryType.CREDIT;
        final EntryType secondEntryType = debitAccountId == secondAccountId ? EntryType.DEBIT : EntryType.CREDIT;
        insertClient.insert(
            firstAccountId,
            secondAccountId,
            firstEntryType,
            secondEntryType,
            amount,
            operationDate,
            currencyCode,
            transactionId
        );
    }

    @Override
    public long balance(UUID accountId, LocalDate operationDate) {
        return balanceClient.balance(accountId, operationDate);
    }

    @Override
    public EntriesSummaryResult entriesSummary(UUID accountId, LocalDate operationDate) {
        return summaryClient.entriesSummary(accountId, operationDate);
    }

    @Override
    public boolean existsByTransactionId(UUID transactionId) {
        return existsClient.existsByTransactionId(transactionId);
    }

    @Override
    public int countByTransactionId(UUID transactionId) {
        return countByTxClient.countByTransactionId(transactionId);
    }
}
