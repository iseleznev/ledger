package org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.jdbc.client;

import lombok.RequiredArgsConstructor;
import org.seleznyov.iyu.kfin.ledger.domain.model.disruptor.WALEntry;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;

@Component
@RequiredArgsConstructor
public class WALEntryInsertClient {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public void insert(WALEntry walEntry) {
        int rowsInserted = jdbcTemplate.update("""
                INSERT INTO ledger.wal_entries
                (id, sequence_number, debit_account_id, credit_account_id, amount, 
                 currency_code, operation_date, transaction_id, idempotency_key, 
                 status, created_at, processed_at, error_message)
                VALUES (:id, :sequenceNumber, :debitAccountId, :creditAccountId, :amount,
                        :currencyCode, :operationDate, :transactionId, :idempotencyKey,
                        :status, :createdAt, :processedAt, :errorMessage)
                """,
            new MapSqlParameterSource()
                .addValue("id", walEntry.id())
                .addValue("sequenceNumber", walEntry.sequenceNumber())
                .addValue("debitAccountId", walEntry.debitAccountId())
                .addValue("creditAccountId", walEntry.creditAccountId())
                .addValue("amount", walEntry.amount())
                .addValue("currencyCode", walEntry.currencyCode())
                .addValue("operationDate", walEntry.operationDate())
                .addValue("transactionId", walEntry.transactionId())
                .addValue("idempotencyKey", walEntry.idempotencyKey())
                .addValue("status", walEntry.status().name())
                .addValue("createdAt", Timestamp.valueOf(walEntry.createdAt()))
                .addValue("processedAt", walEntry.processedAt() != null ?
                    Timestamp.valueOf(walEntry.processedAt()) : null)
                .addValue("errorMessage", walEntry.errorMessage())
        );

        if (rowsInserted == 0) {
            throw new IllegalStateException("Failed to insert WAL entry: " + walEntry.id());
        }
    }
}