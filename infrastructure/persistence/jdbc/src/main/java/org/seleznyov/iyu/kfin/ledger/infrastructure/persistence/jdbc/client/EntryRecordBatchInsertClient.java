package org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.jdbc.client;

import lombok.RequiredArgsConstructor;
import org.seleznyov.iyu.kfin.ledger.domain.model.entryrecord.EntryType;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class EntryRecordBatchInsertClient {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public void insert(Map<String, Object>[] parameters) {
        jdbcTemplate.batchUpdate("""
                INSERT INTO entry_records (account_id, transaction_id, entry_type, amount, operation_date, created_at, idempotency_key, currency_code)
                VALUES (:firstAccountId, :transactionId, :firstEntryType, :amount, :operationDate, :createdAt, :idempotencyKey, :currencyCode)
                """,
            parameters
        );
//        if (rowsInserted == 0) {
//            throw new IllegalStateException();
//        }
//        if (rowsInserted != 2) {
//            throw new IllegalStateException("Unexpected number of rows inserted: " + rowsInserted);
//        }
    }
}
