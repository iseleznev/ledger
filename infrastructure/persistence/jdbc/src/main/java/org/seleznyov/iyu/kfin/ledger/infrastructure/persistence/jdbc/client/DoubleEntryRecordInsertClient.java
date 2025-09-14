package org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.jdbc.client;

import lombok.RequiredArgsConstructor;
import org.seleznyov.iyu.kfin.ledger.domain.model.entryrecord.EntryType;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class DoubleEntryRecordInsertClient {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public void insert(
        UUID firstAccountId,
        UUID secondAccountId,
        EntryType firstEntryType,
        EntryType secondEntryType,
        long amount,
        LocalDate operationDate,
        String currencyCode,
        UUID transactionId
    ) {
        final int rowsInserted = jdbcTemplate.update("""
                INSERT INTO entry_records (account_id, transaction_id, entry_type, amount, operation_date, created_at, idempotency_key, currency_code)
                VALUES (:firstAccountId, :transactionId, :firstEntryType, :amount, :operationDate, :createdAt, :idempotencyKey, :currencyCode),
                       (:secondAccountId, :transactionId, :secondEntryType, :amount, :operationDate, :createdAt, :idempotencyKey, :currencyCode)
                """,
            Map.of(
                "firstAccountId", firstAccountId,
                "secondAccountId", secondAccountId,
                "amount", amount,
                "operationDate", operationDate,
                "createdAt", LocalDateTime.now(),
                "idempotencyKey", transactionId,
                "transactionId", transactionId,
                "firstEntryType", firstEntryType.name(),
                "secondEntryType", secondEntryType.name(),
                "currencyCode", currencyCode
            )
        );
        if (rowsInserted == 0) {
            throw new IllegalStateException();
        }
        if (rowsInserted != 2) {
            throw new IllegalStateException("Unexpected number of rows inserted: " + rowsInserted);
        }
    }
}
