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
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Repository for WalEntry operations using JDBC Template
 * Optimized for high-throughput WAL operations
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class WalEntryRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final Executor asyncExecutor;

    private static final String INSERT_WAL_ENTRY_SQL = """
        INSERT INTO ledger.wal_entries 
        (id, sequence_number, debit_account_id, credit_account_id, amount, currency_code, 
         operation_date, transaction_id, idempotency_key, status, created_at)
        VALUES (:id, :sequenceNumber, :debitAccountId, :creditAccountId, :amount, :currencyCode,
                :operationDate, :transactionId, :idempotencyKey, :status, :createdAt)
        """;

    private static final String UPDATE_STATUS_SQL = """
        UPDATE ledger.wal_entries 
        SET status = :status, processed_at = :processedAt, error_message = :errorMessage
        WHERE id = :id
        """;

    private static final String SELECT_PENDING_ENTRIES_SQL = """
        SELECT id, sequence_number, debit_account_id, credit_account_id, amount, currency_code,
               operation_date, transaction_id, idempotency_key, status, created_at, processed_at, error_message
        FROM ledger.wal_entries 
        WHERE status IN ('PENDING', 'PROCESSING')
        ORDER BY sequence_number
        LIMIT :limit
        """;

    private static final String SELECT_BY_STATUS_SQL = """
        SELECT id, sequence_number, debit_account_id, credit_account_id, amount, currency_code,
               operation_date, transaction_id, idempotency_key, status, created_at, processed_at, error_message
        FROM ledger.wal_entries 
        WHERE status = :status
        ORDER BY sequence_number
        LIMIT :limit
        """;

    private static final String SELECT_BY_SEQUENCE_RANGE_SQL = """
        SELECT id, sequence_number, debit_account_id, credit_account_id, amount, currency_code,
               operation_date, transaction_id, idempotency_key, status, created_at, processed_at, error_message
        FROM ledger.wal_entries 
        WHERE sequence_number BETWEEN :fromSequence AND :toSequence
        ORDER BY sequence_number
        """;

    private static final String SELECT_STALE_ENTRIES_SQL = """
        SELECT id, sequence_number, debit_account_id, credit_account_id, amount, currency_code,
               operation_date, transaction_id, idempotency_key, status, created_at, processed_at, error_message
        FROM ledger.wal_entries 
        WHERE status IN ('PENDING', 'PROCESSING')
        AND created_at < :threshold
        ORDER BY sequence_number
        LIMIT :limit
        """;

    private static final String DELETE_PROCESSED_ENTRIES_SQL = """
        DELETE FROM ledger.wal_entries 
        WHERE status IN ('PROCESSED', 'RECOVERED')
        AND processed_at < :threshold
        """;

    private static final String COUNT_BY_STATUS_SQL = """
        SELECT COUNT(*) FROM ledger.wal_entries WHERE status = :status
        """;

    private static final String SELECT_MAX_SEQUENCE_SQL = """
        SELECT MAX(sequence_number) FROM ledger.wal_entries
        """;

    private static final RowMapper<WalEntry> WAL_ENTRY_ROW_MAPPER = new WalEntryRowMapper();

    /**
     * Save WAL entry
     */
    @Transactional
    public WalEntry save(WalEntry walEntry) {
        try {
            if (walEntry.getId() == null) {
                walEntry.setId(UUID.randomUUID());
            }

            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", walEntry.getId())
                .addValue("sequenceNumber", walEntry.getSequenceNumber())
                .addValue("debitAccountId", walEntry.getDebitAccountId())
                .addValue("creditAccountId", walEntry.getCreditAccountId())
                .addValue("amount", walEntry.getAmount())
                .addValue("currencyCode", walEntry.getCurrencyCode())
                .addValue("operationDate", walEntry.getOperationDate())
                .addValue("transactionId", walEntry.getTransactionId())
                .addValue("idempotencyKey", walEntry.getIdempotencyKey())
                .addValue("status", walEntry.getStatus().name())
                .addValue("createdAt", walEntry.getCreatedAt());

            int rowsAffected = jdbcTemplate.update(INSERT_WAL_ENTRY_SQL, params);

            if (rowsAffected != 1) {
                throw new DataAccessException("Failed to insert WAL entry, rows affected: " + rowsAffected) {};
            }

            log.debug("Saved WAL entry: {}, sequence: {}", walEntry.getId(), walEntry.getSequenceNumber());
            return walEntry;

        } catch (Exception e) {
            log.error("Failed to save WAL entry for transaction {}", walEntry.getTransactionId(), e);
            throw new DataAccessException("Failed to save WAL entry", e) {};
        }
    }

    /**
     * Batch save WAL entries
     */
    @Transactional
    public List<WalEntry> saveAll(List<WalEntry> walEntries) {
        if (walEntries.isEmpty()) {
            return walEntries;
        }

        try {
            SqlParameterSource[] batchParams = walEntries.stream()
                .map(entry -> {
                    if (entry.getId() == null) {
                        entry.setId(UUID.randomUUID());
                    }

                    return new MapSqlParameterSource()
                        .addValue("id", entry.getId())
                        .addValue("sequenceNumber", entry.getSequenceNumber())
                        .addValue("debitAccountId", entry.getDebitAccountId())
                        .addValue("creditAccountId", entry.getCreditAccountId())
                        .addValue("amount", entry.getAmount())
                        .addValue("currencyCode", entry.getCurrencyCode())
                        .addValue("operationDate", entry.getOperationDate())
                        .addValue("transactionId", entry.getTransactionId())
                        .addValue("idempotencyKey", entry.getIdempotencyKey())
                        .addValue("status", entry.getStatus().name())
                        .addValue("createdAt", entry.getCreatedAt());
                })
                .toArray(SqlParameterSource[]::new);

            int[] rowsAffected = jdbcTemplate.batchUpdate(INSERT_WAL_ENTRY_SQL, batchParams);

            int totalRows = Arrays.stream(rowsAffected).sum();
            if (totalRows != walEntries.size()) {
                throw new DataAccessException("Batch WAL insert failed, expected " + walEntries.size() +
                    " rows, affected " + totalRows) {};
            }

            log.debug("Batch saved {} WAL entries", walEntries.size());
            return walEntries;

        } catch (Exception e) {
            log.error("Failed to batch save {} WAL entries", walEntries.size(), e);
            throw new DataAccessException("Failed to batch save WAL entries", e) {};
        }
    }

    /**
     * Update WAL entry status
     */
    @Transactional
    public void updateStatus(UUID walEntryId, WalEntry.WalStatus status, String errorMessage) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", walEntryId)
                .addValue("status", status.name())
                .addValue("processedAt", OffsetDateTime.now())
                .addValue("errorMessage", errorMessage);

            int rowsAffected = jdbcTemplate.update(UPDATE_STATUS_SQL, params);

            if (rowsAffected != 1) {
                log.warn("WAL entry status update affected {} rows for ID {}", rowsAffected, walEntryId);
            }

            log.debug("Updated WAL entry {} to status {}", walEntryId, status);

        } catch (Exception e) {
            log.error("Failed to update WAL entry status for ID {}", walEntryId, e);
            throw new DataAccessException("Failed to update WAL entry status", e) {};
        }
    }

    /**
     * Get pending entries for processing
     */
    public List<WalEntry> findPendingEntries(int limit) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("limit", limit);

            List<WalEntry> entries = jdbcTemplate.query(SELECT_PENDING_ENTRIES_SQL, params, WAL_ENTRY_ROW_MAPPER);
            log.debug("Found {} pending WAL entries", entries.size());

            return entries;

        } catch (Exception e) {
            log.error("Failed to find pending WAL entries", e);
            throw new DataAccessException("Failed to find pending WAL entries", e) {};
        }
    }

    /**
     * Get entries by status
     */
    public List<WalEntry> findByStatus(WalEntry.WalStatus status, int limit) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("status", status.name())
                .addValue("limit", limit);

            List<WalEntry> entries = jdbcTemplate.query(SELECT_BY_STATUS_SQL, params, WAL_ENTRY_ROW_MAPPER);
            log.debug("Found {} WAL entries with status {}", entries.size(), status);

            return entries;

        } catch (Exception e) {
            log.error("Failed to find WAL entries with status {}", status, e);
            throw new DataAccessException("Failed to find WAL entries by status", e) {};
        }
    }

    /**
     * Get entries by sequence number range
     */
    public List<WalEntry> findBySequenceRange(long fromSequence, long toSequence) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("fromSequence", fromSequence)
                .addValue("toSequence", toSequence);

            List<WalEntry> entries = jdbcTemplate.query(SELECT_BY_SEQUENCE_RANGE_SQL, params, WAL_ENTRY_ROW_MAPPER);
            log.debug("Found {} WAL entries in sequence range {}-{}", entries.size(), fromSequence, toSequence);

            return entries;

        } catch (Exception e) {
            log.error("Failed to find WAL entries in sequence range {}-{}", fromSequence, toSequence, e);
            throw new DataAccessException("Failed to find WAL entries by sequence range", e) {};
        }
    }

    /**
     * Get stale entries that need recovery
     */
    public List<WalEntry> findStaleEntries(int maxAgeMinutes, int limit) {
        try {
            OffsetDateTime threshold = OffsetDateTime.now().minusMinutes(maxAgeMinutes);

            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("threshold", threshold)
                .addValue("limit", limit);

            List<WalEntry> entries = jdbcTemplate.query(SELECT_STALE_ENTRIES_SQL, params, WAL_ENTRY_ROW_MAPPER);
            log.debug("Found {} stale WAL entries older than {} minutes", entries.size(), maxAgeMinutes);

            return entries;

        } catch (Exception e) {
            log.error("Failed to find stale WAL entries", e);
            throw new DataAccessException("Failed to find stale WAL entries", e) {};
        }
    }

    /**
     * Clean up old processed entries
     */
    @Transactional
    public int cleanupProcessedEntries(int maxAgeHours) {
        try {
            OffsetDateTime threshold = OffsetDateTime.now().minusHours(maxAgeHours);

            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("threshold", threshold);

            int deletedCount = jdbcTemplate.update(DELETE_PROCESSED_ENTRIES_SQL, params);
            log.info("Cleaned up {} processed WAL entries older than {} hours", deletedCount, maxAgeHours);

            return deletedCount;

        } catch (Exception e) {
            log.error("Failed to cleanup processed WAL entries", e);
            throw new DataAccessException("Failed to cleanup processed WAL entries", e) {};
        }
    }

    /**
     * Count entries by status
     */
    public long countByStatus(WalEntry.WalStatus status) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("status", status.name());

            Long count = jdbcTemplate.queryForObject(COUNT_BY_STATUS_SQL, params, Long.class);
            return count != null ? count : 0L;

        } catch (Exception e) {
            log.error("Failed to count WAL entries by status {}", status, e);
            throw new DataAccessException("Failed to count WAL entries by status", e) {};
        }
    }

    /**
     * Get maximum sequence number
     */
    public OptionalLong getMaxSequenceNumber() {
        try {
            Long maxSequence = jdbcTemplate.queryForObject(SELECT_MAX_SEQUENCE_SQL,
                new MapSqlParameterSource(), Long.class);
            return maxSequence != null ? OptionalLong.of(maxSequence) : OptionalLong.empty();

        } catch (Exception e) {
            log.error("Failed to get max sequence number", e);
            throw new DataAccessException("Failed to get max sequence number", e) {};
        }
    }

    /**
     * Async variants
     */
    public CompletableFuture<List<WalEntry>> findPendingEntriesAsync(int limit) {
        return CompletableFuture.supplyAsync(() -> findPendingEntries(limit), asyncExecutor);
    }

    public CompletableFuture<Void> updateStatusAsync(UUID walEntryId, WalEntry.WalStatus status, String errorMessage) {
        return CompletableFuture.runAsync(() -> updateStatus(walEntryId, status, errorMessage), asyncExecutor);
    }

    /**
     * RowMapper for WalEntry
     */
    private static class WalEntryRowMapper implements RowMapper<WalEntry> {
        @Override
        public WalEntry mapRow(ResultSet rs, int rowNum) throws SQLException {
            return WalEntry.builder()
                .id(UUID.fromString(rs.getString("id")))
                .sequenceNumber(rs.getLong("sequence_number"))
                .debitAccountId(UUID.fromString(rs.getString("debit_account_id")))
                .creditAccountId(UUID.fromString(rs.getString("credit_account_id")))
                .amount(rs.getLong("amount"))
                .currencyCode(rs.getString("currency_code"))
                .operationDate(rs.getObject("operation_date", LocalDate.class))
                .transactionId(UUID.fromString(rs.getString("transaction_id")))
                .idempotencyKey(UUID.fromString(rs.getString("idempotency_key")))
                .status(WalEntry.WalStatus.valueOf(rs.getString("status")))
                .createdAt(rs.getObject("created_at", OffsetDateTime.class))
                .processedAt(rs.getObject("processed_at", OffsetDateTime.class))
                .errorMessage(rs.getString("error_message"))
                .build();
        }
    }
}