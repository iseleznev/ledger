package org.seleznyov.iyu.kfin.ledger.application.service.containeroptimized;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Repository for EntryRecord operations using JDBC Template
 * Optimized for high-performance batch operations
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class EntryRecordRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final Executor asyncExecutor;

    private static final String INSERT_ENTRY_SQL = """
        INSERT INTO ledger.entry_records 
        (id, account_id, transaction_id, entry_type, amount, created_at, operation_date, idempotency_key, currency_code, entry_ordinal)
        VALUES (:id, :accountId, :transactionId, :entryType, :amount, :createdAt, :operationDate, :idempotencyKey, :currencyCode, 
                COALESCE(:entryOrdinal, nextval('ledger_entry_ordinal_sequence')))
        """;

    private static final String SELECT_BY_ACCOUNT_SQL = """
        SELECT id, account_id, transaction_id, entry_type, amount, created_at, operation_date, idempotency_key, currency_code, entry_ordinal
        FROM ledger.entry_records 
        WHERE account_id = :accountId 
        ORDER BY entry_ordinal DESC 
        LIMIT :limit
        """;

    private static final String SELECT_BALANCE_SQL = """
        SELECT COALESCE(SUM(CASE WHEN entry_type = 'CREDIT' THEN amount ELSE -amount END), 0) as balance
        FROM ledger.entry_records 
        WHERE account_id = :accountId
        AND entry_ordinal <= :maxOrdinal
        """;

    private static final String SELECT_BALANCE_SINCE_ORDINAL_SQL = """
        SELECT COALESCE(SUM(CASE WHEN entry_type = 'CREDIT' THEN amount ELSE -amount END), 0) as balance
        FROM ledger.entry_records 
        WHERE account_id = :accountId
        AND entry_ordinal > :sinceOrdinal
        AND entry_ordinal <= :maxOrdinal
        """;

    private static final String SELECT_BY_TRANSACTION_SQL = """
        SELECT id, account_id, transaction_id, entry_type, amount, created_at, operation_date, idempotency_key, currency_code, entry_ordinal
        FROM ledger.entry_records 
        WHERE transaction_id = :transactionId
        ORDER BY entry_type DESC
        """;

    private static final String SELECT_BY_IDEMPOTENCY_SQL = """
        SELECT id, account_id, transaction_id, entry_type, amount, created_at, operation_date, idempotency_key, currency_code, entry_ordinal
        FROM ledger.entry_records 
        WHERE idempotency_key = :idempotencyKey
        """;

    private static final String SELECT_LATEST_ORDINAL_SQL = """
        SELECT MAX(entry_ordinal) as max_ordinal
        FROM ledger.entry_records 
        WHERE account_id = :accountId
        """;

    private static final String COUNT_OPERATIONS_SINCE_ORDINAL_SQL = """
        SELECT COUNT(*) as operation_count
        FROM ledger.entry_records 
        WHERE account_id = :accountId 
        AND entry_ordinal > :sinceOrdinal
        """;

    private static final RowMapper<EntryRecord> ENTRY_RECORD_ROW_MAPPER = new EntryRecordRowMapper();

    /**
     * Insert single entry record
     */
    @Transactional
    public EntryRecord save(EntryRecord entryRecord) {
        try {
            if (entryRecord.getId() == null) {
                entryRecord.setId(UUID.randomUUID());
            }

            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", entryRecord.getId())
                .addValue("accountId", entryRecord.getAccountId())
                .addValue("transactionId", entryRecord.getTransactionId())
                .addValue("entryType", entryRecord.getEntryType().name())
                .addValue("amount", entryRecord.getAmount())
                .addValue("createdAt", entryRecord.getCreatedAt())
                .addValue("operationDate", entryRecord.getOperationDate())
                .addValue("idempotencyKey", entryRecord.getIdempotencyKey())
                .addValue("currencyCode", entryRecord.getCurrencyCode())
                .addValue("entryOrdinal", entryRecord.getEntryOrdinal());

            int rowsAffected = jdbcTemplate.update(INSERT_ENTRY_SQL, params);

            if (rowsAffected != 1) {
                throw new DataAccessException("Failed to insert entry record, rows affected: " + rowsAffected) {};
            }

            log.debug("Saved entry record: {}", entryRecord.getId());
            return entryRecord;

        } catch (Exception e) {
            log.error("Failed to save entry record for account {}", entryRecord.getAccountId(), e);
            throw new DataAccessException("Failed to save entry record", e) {};
        }
    }

    /**
     * Batch insert entry records - critical for performance
     */
    @Transactional
    public List<EntryRecord> saveAll(List<EntryRecord> entryRecords) {
        if (entryRecords.isEmpty()) {
            return entryRecords;
        }

        try {
            SqlParameterSource[] batchParams = entryRecords.stream()
                .map(record -> {
                    if (record.getId() == null) {
                        record.setId(UUID.randomUUID());
                    }

                    return new MapSqlParameterSource()
                        .addValue("id", record.getId())
                        .addValue("accountId", record.getAccountId())
                        .addValue("transactionId", record.getTransactionId())
                        .addValue("entryType", record.getEntryType().name())
                        .addValue("amount", record.getAmount())
                        .addValue("createdAt", record.getCreatedAt())
                        .addValue("operationDate", record.getOperationDate())
                        .addValue("idempotencyKey", record.getIdempotencyKey())
                        .addValue("currencyCode", record.getCurrencyCode())
                        .addValue("entryOrdinal", record.getEntryOrdinal());
                })
                .toArray(SqlParameterSource[]::new);

            int[] rowsAffected = jdbcTemplate.batchUpdate(INSERT_ENTRY_SQL, batchParams);

            int totalRows = Arrays.stream(rowsAffected).sum();
            if (totalRows != entryRecords.size()) {
                throw new DataAccessException("Batch insert failed, expected " + entryRecords.size() +
                    " rows, affected " + totalRows) {};
            }

            log.debug("Batch saved {} entry records", entryRecords.size());
            return entryRecords;

        } catch (Exception e) {
            log.error("Failed to batch save {} entry records", entryRecords.size(), e);
            throw new DataAccessException("Failed to batch save entry records", e) {};
        }
    }

    /**
     * Get account entries with limit
     */
    public List<EntryRecord> findByAccountId(UUID accountId, int limit) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("accountId", accountId)
                .addValue("limit", limit);

            List<EntryRecord> entries = jdbcTemplate.query(SELECT_BY_ACCOUNT_SQL, params, ENTRY_RECORD_ROW_MAPPER);
            log.debug("Found {} entries for account {}", entries.size(), accountId);

            return entries;

        } catch (Exception e) {
            log.error("Failed to find entries for account {}", accountId, e);
            throw new DataAccessException("Failed to find entries for account", e) {};
        }
    }

    /**
     * Calculate account balance up to specific ordinal
     */
    public long calculateBalance(UUID accountId, long maxOrdinal) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("accountId", accountId)
                .addValue("maxOrdinal", maxOrdinal);

            Long balance = jdbcTemplate.queryForObject(SELECT_BALANCE_SQL, params, Long.class);
            return balance != null ? balance : 0L;

        } catch (Exception e) {
            log.error("Failed to calculate balance for account {} at ordinal {}", accountId, maxOrdinal, e);
            throw new DataAccessException("Failed to calculate balance", e) {};
        }
    }

    /**
     * Calculate balance change since specific ordinal
     */
    public long calculateBalanceSince(UUID accountId, long sinceOrdinal, long maxOrdinal) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("accountId", accountId)
                .addValue("sinceOrdinal", sinceOrdinal)
                .addValue("maxOrdinal", maxOrdinal);

            Long balance = jdbcTemplate.queryForObject(SELECT_BALANCE_SINCE_ORDINAL_SQL, params, Long.class);
            return balance != null ? balance : 0L;

        } catch (Exception e) {
            log.error("Failed to calculate balance since ordinal {} for account {}", sinceOrdinal, accountId, e);
            throw new DataAccessException("Failed to calculate balance since ordinal", e) {};
        }
    }

    /**
     * Get entries by transaction ID
     */
    public List<EntryRecord> findByTransactionId(UUID transactionId) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("transactionId", transactionId);

            List<EntryRecord> entries = jdbcTemplate.query(SELECT_BY_TRANSACTION_SQL, params, ENTRY_RECORD_ROW_MAPPER);
            log.debug("Found {} entries for transaction {}", entries.size(), transactionId);

            return entries;

        } catch (Exception e) {
            log.error("Failed to find entries for transaction {}", transactionId, e);
            throw new DataAccessException("Failed to find entries for transaction", e) {};
        }
    }

    /**
     * Find entry by idempotency key
     */
    public Optional<EntryRecord> findByIdempotencyKey(UUID idempotencyKey) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("idempotencyKey", idempotencyKey);

            List<EntryRecord> entries = jdbcTemplate.query(SELECT_BY_IDEMPOTENCY_SQL, params, ENTRY_RECORD_ROW_MAPPER);

            if (entries.size() > 1) {
                log.warn("Multiple entries found for idempotency key {}", idempotencyKey);
            }

            return entries.isEmpty() ? Optional.empty() : Optional.of(entries.get(0));

        } catch (Exception e) {
            log.error("Failed to find entry by idempotency key {}", idempotencyKey, e);
            throw new DataAccessException("Failed to find entry by idempotency key", e) {};
        }
    }

    /**
     * Get latest ordinal for account
     */
    public OptionalLong getLatestOrdinal(UUID accountId) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("accountId", accountId);

            Long maxOrdinal = jdbcTemplate.queryForObject(SELECT_LATEST_ORDINAL_SQL, params, Long.class);
            return maxOrdinal != null ? OptionalLong.of(maxOrdinal) : OptionalLong.empty();

        } catch (Exception e) {
            log.error("Failed to get latest ordinal for account {}", accountId, e);
            throw new DataAccessException("Failed to get latest ordinal", e) {};
        }
    }

    /**
     * Count operations since ordinal
     */
    public int countOperationsSince(UUID accountId, long sinceOrdinal) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("accountId", accountId)
                .addValue("sinceOrdinal", sinceOrdinal);

            Integer count = jdbcTemplate.queryForObject(COUNT_OPERATIONS_SINCE_ORDINAL_SQL, params, Integer.class);
            return count != null ? count : 0;

        } catch (Exception e) {
            log.error("Failed to count operations since ordinal {} for account {}", sinceOrdinal, accountId, e);
            throw new DataAccessException("Failed to count operations since ordinal", e) {};
        }
    }

    /**
     * Async variants for non-blocking operations
     */
    public CompletableFuture<List<EntryRecord>> findByAccountIdAsync(UUID accountId, int limit) {
        return CompletableFuture.supplyAsync(() -> findByAccountId(accountId, limit), asyncExecutor);
    }

    public CompletableFuture<Long> calculateBalanceAsync(UUID accountId, long maxOrdinal) {
        return CompletableFuture.supplyAsync(() -> calculateBalance(accountId, maxOrdinal), asyncExecutor);
    }

    /**
     * RowMapper for EntryRecord
     */
    private static class EntryRecordRowMapper implements RowMapper<EntryRecord> {
        @Override
        public EntryRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
            return EntryRecord.builder()
                .id(UUID.fromString(rs.getString("id")))
                .accountId(UUID.fromString(rs.getString("account_id")))
                .transactionId(UUID.fromString(rs.getString("transaction_id")))
                .entryType(EntryRecord.EntryType.valueOf(rs.getString("entry_type")))
                .amount(rs.getLong("amount"))
                .createdAt(rs.getObject("created_at", java.time.OffsetDateTime.class))
                .operationDate(rs.getObject("operation_date", LocalDate.class))
                .idempotencyKey(UUID.fromString(rs.getString("idempotency_key")))
                .currencyCode(rs.getString("currency_code"))
                .entryOrdinal(rs.getLong("entry_ordinal"))
                .build();
        }
    }
}