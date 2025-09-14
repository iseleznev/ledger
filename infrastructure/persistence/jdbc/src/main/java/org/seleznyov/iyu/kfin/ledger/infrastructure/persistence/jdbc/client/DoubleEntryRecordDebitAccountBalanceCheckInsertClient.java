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
public class DoubleEntryRecordDebitAccountBalanceCheckInsertClient {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public void insert(
        UUID debitAccountId,
        UUID creditAccountId,
        long amount,
        LocalDate operationDate,
        String currencyCode,
        UUID idempotencyKey,
        UUID transactionId
    ) {
        final int rowsInserted = jdbcTemplate.update("""
                WITH latest_snapshot_balance AS (
                    SELECT balance, last_entry_ordinal 
                    FROM entries_snapshots
                    WHERE account_id = :debitAccountId AND operation_date <= :operationDate
                    ORDER BY operation_date DESC, snapshot_ordinal DESC LIMIT 1
                ),
                entries_amount AS (
                    SELECT COALESCE(ls.balance, 0) + COALESCE(SUM(
                        CASE WHEN er.entry_type = 'CREDIT' THEN er.amount ELSE -er.amount END
                    ), 0) as actual_balance
                    FROM latest_snapshot_balance ls
                    FULL OUTER JOIN entry_records er ON (
                        er.account_id = :debitAccountId AND er.operation_date <= :operationDate
                        AND er.entry_ordinal > COALESCE(ls.last_entry_ordinal, 0)
                    )
                    GROUP BY ls.balance
                ),
                idempotency_key_exists AS (
                  SELECT idempotency_key
                  FROM entry_records
                  WHERE idempotency_key = :idempotencyKey
                  LIMIT 1
                ),
                debit_entry_insert AS (
                  INSERT INTO entry_records (account_id, transaction_id, entry_type, amount, created_at, operation_date, idempotency_key, currency_code)
                  SELECT :debitAccountId, :transactionId, :debitEntryType, :amount, :createdAt, :operationDate, :idempotencyKey, :currencyCode
                  FROM entries_amount FULL OUTER JOIN idempotency_key_exists ON true
                  WHERE entries_amount.actual_balance >= :amount AND idempotency_key_exists.idempotency_key IS NULL
                  RETURNING account_id
                ),
                credit_entry_insert AS (
                  INSERT INTO entry_records (account_id, transaction_id, entry_type, amount, created_at, operation_date, idempotency_key, currency_code)
                  SELECT :creditAccountId, :transactionId, :creditEntryType, :amount, :createdAt, :operationDate, :idempotencyKey, :currencyCode
                  FROM idempotency_key_exists
                  WHERE idempotency_key_exists.idempotency_key IS NULL
                  RETURNING account_id
                )
                SELECT account_id
                FROM debit_entry_insert
                UNION ALL
                SELECT account_id
                FROM credit_entry_insert
                """,
            Map.of(
                "debitAccountId", debitAccountId,
                "creditAccountId", creditAccountId,
                "amount", amount,
                "operationDate", operationDate,
                "createdAt", LocalDateTime.now(),
                "idempotencyKey", idempotencyKey,
                "transactionId", transactionId,
                "currencyCode", currencyCode,
                "debitEntryType", EntryType.DEBIT.name(),
                "creditEntryType", EntryType.CREDIT.name()
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
