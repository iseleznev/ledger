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
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * IMMUTABLE Repository for WalEntry operations
 * Only INSERT operations - status changes create new versions
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class ImmutableWalEntryRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final Executor asyncExecutor;

    private static final String INSERT_WAL_ENTRY_SQL = """
        INSERT INTO ledger.wal_entries 
        (id, sequence_number, debit_account_id, credit_account_id, amount, currency_code, 
         operation_date, transaction_id, idempotency_key, status, created_at, processed_at, error_message,
         parent_id, is_current, version_number)
        VALUES (:id, :sequenceNumber, :debitAccountId, :creditAccountId, :amount, :currencyCode,
                :operationDate, :transactionId, :idempotencyKey, :status, :createdAt, :processedAt, :errorMessage,
                :parentId, :isCurrent, COALESCE(:versionNumber, nextval('ledger_wal_version_sequence')))
        """;

    private static final String MARK_AS_NON_CURRENT_SQL = """
        INSERT INTO ledger.wal_entries 
        (id, sequence_number, debit_account_id, credit_account_id, amount, currency_code, 
         operation_date, transaction_id, idempotency_key, status, created_at, processed_at, error_message,
         parent_id, is_current, version_number)
        SELECT id, sequence_number, debit_account_id, credit_account_id, amount, currency_code,
               operation_date, transaction_id, idempotency_key, status, created_at, processed_at, error_message,
               parent_id, false, version_number
        FROM ledger.wal_entries 
        WHERE transaction_id = :transactionId AND is_current = true
        """;

    private static final String SELECT_CURRENT_PENDING_ENTRIES_SQL = """
        SELECT id, sequence_number, debit_account_id, credit_account_id, amount, currency_code,
               operation_date, transaction_id, idempotency_key, status, created_at, processed_at, error_message,
               parent_id, is_current, version_number
        FROM ledger.wal_entries 
        WHERE status IN ('PENDING', 'PROCESSING')
        AND is_current = true
        ORDER BY sequence_number
        LIMIT :limit
        """;

    private static final String SELECT_CURRENT_BY_STATUS_SQL = """
        SELECT id, sequence_number, debit_account_id, credit_account_id, amount, currency_code,
               operation_date, transaction_id, idempotency_key, status, created_at, processed_at, error_message,
               parent_id, is_current, version_number
        FROM ledger.wal_entries 
        WHERE status = :status
        AND is_current = true
        ORDER BY sequence_number
        LIMIT :limit
        """;

    private static final String SELECT_CURRENT_BY_TRANSACTION_SQL = """
        SELECT id, sequence_number, debit_account_id, credit_account_id, amount, currency_code,
               operation_date, transaction_id, idempotency_key, status, created_at, processed_at, error_message,
               parent_id, is_current, version_number
        FROM ledger.wal_entries 
        WHERE transaction_id = :transactionId
        AND is_current = true
        """;

    private static final String SELECT_VERSION_HISTORY_SQL = """
        SELECT id, sequence_number, debit_account_id, credit_account_id, amount, currency_code,
               operation_date, transaction_id, idempotency_key, status, created_at, processed_at, error_message,
               parent_id, is_current, version_number
        FROM ledger.wal_entries 
        WHERE transaction_id = :transactionId
        ORDER BY version_number ASC
        """;

    private static final String SELECT_CURRENT_STALE_ENTRIES_SQL = """
        SELECT id, sequence_number, debit_account_id, credit_account_id, amount, currency_code,
               operation_date, transaction_id, idempotency_key, status, created_at, processed_at, error_message,
               parent_id, is_current, version_number
        FROM ledger.wal_entries 
        WHERE status IN ('PENDING', 'PROCESSING')
        AND is_current = true
        AND created_at < :threshold
        ORDER BY sequence_number
        LIMIT :limit
        """;

    private static final String COUNT_CURRENT_BY_STATUS_SQL = """
        SELECT COUNT(*) FROM ledger.wal_entries 
        WHERE status = :status AND is_current = true
        """;

    private static final String SELECT_MAX_SEQUENCE_SQL = """
        SELECT MAX(sequence_number) FROM ledger.wal_entries WHERE is_current = true
        """;

    private static final RowMapper<WalEntry> WAL_ENTRY_ROW_MAPPER = new WalEntryRowMapper();

    /**
     * Save WAL entry (IMMUTABLE - only insert)
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
                .addValue("createdAt", walEntry.getCreatedAt())
                .addValue("processedAt", walEntry.getProcessedAt())
                .addValue("errorMessage", walEntry.getErrorMessage());

            int rowsAffected = jdbcTemplate.update(INSERT_WAL_ENTRY_SQL, params);

            if (rowsAffected != 1) {
                throw new DataAccessException("Failed to insert WAL entry, rows affected: " + rowsAffected) {};
            }

            log.debug("Saved WAL entry: {}, sequence: {}",
                walEntry.getId(), walEntry.getSequenceNumber());
            return walEntry;

        } catch (Exception e) {
            log.error("Failed to save WAL entry for transaction {}", walEntry.getTransactionId(), e);
            throw new DataAccessException("Failed to save WAL entry", e) {};
        }
    }

    /**
     * Change WAL entry status by creating new version (IMMUTABLE approach)
     */
    @Transactional
    public WalEntry changeStatus(UUID transactionId, WalEntry.WalStatus newStatus, String errorMessage) {
        try {
            // Find current version
            Optional<WalEntry> currentEntry = findCurrentByTransactionId(transactionId);
            if (currentEntry.isEmpty()) {
                throw new DataAccessException("No current WAL entry found for transaction " + transactionId) {};
            }

            WalEntry current = currentEntry.get();

            // Mark current version as non-current
            markAsNonCurrent(transactionId);

            // Create new version with status change
            WalEntry newVersion = current.createNewVersion(newStatus, errorMessage);
            WalEntry savedNewVersion = save(newVersion);

            log.debug("Changed WAL entry status for transaction {} from {} to {}",
                transactionId, current.getStatus(), newStatus);

            return savedNewVersion;

        } catch (Exception e) {
            log.error("Failed to change WAL entry status for transaction {}", transactionId, e);
            throw new DataAccessException("Failed to change WAL entry status", e) {};
        }
    }

    /**
     * Mark current version as non-current (part of immutable status change)
     */
    @Transactional
    public void markAsNonCurrent(UUID transactionId) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("transactionId", transactionId);

            int rowsAffected = jdbcTemplate.update(MARK_AS_NON_CURRENT_SQL, params);

            log.debug("Marked {} WAL entry versions as non-current for transaction {}", rowsAffected, transactionId);

        } catch (Exception e) {
            log.error("Failed to mark WAL entries as non-current for transaction {}", transactionId, e);
            throw new DataAccessException("Failed to mark WAL entries as non-current", e) {};
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
                        .addValue("createdAt", entry.getCreatedAt())
                        .addValue("processedAt", entry.getProcessedAt())
                        .addValue("errorMessage", entry.getErrorMessage());
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
     * Get current pending entries for processing
     */
    public List<WalEntry> findCurrentPendingEntries(int limit) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("limit", limit);

            List<WalEntry> entries = jdbcTemplate.query(SELECT_CURRENT_PENDING_ENTRIES_SQL, params, WAL_ENTRY_ROW_MAPPER);
            log.debug("Found {} current pending WAL entries", entries.size());

            return entries;

        } catch (Exception e) {
            log.error("Failed to find current pending WAL entries", e);
            throw new DataAccessException("Failed to find current pending WAL entries", e) {};
        }
    }

    /**
     * Get current entries by status
     */
    public List<WalEntry> findCurrentByStatus(WalEntry.WalStatus status, int limit) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("status", status.name())
                .addValue("limit", limit);

            List<WalEntry> entries = jdbcTemplate.query(SELECT_CURRENT_BY_STATUS_SQL, params, WAL_ENTRY_ROW_MAPPER);
            log.debug("Found {} current WAL entries with status {}", entries.size(), status);

            return entries;

        } catch (Exception e) {
            log.error("Failed to find current WAL entries with status {}", status, e);
            throw new DataAccessException("Failed to find current WAL entries by status", e) {};
        }
    }

    /**
     * Get current entry by transaction ID
     */
    public Optional<WalEntry> findCurrentByTransactionId(UUID transactionId) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("transactionId", transactionId);

            List<WalEntry> entries = jdbcTemplate.query(SELECT_CURRENT_BY_TRANSACTION_SQL, params, WAL_ENTRY_ROW_MAPPER);

            if (entries.size() > 1) {
                log.warn("Multiple current WAL entries found for transaction {}", transactionId);
            }

            return entries.isEmpty() ? Optional.empty() : Optional.of(entries.get(0));

        } catch (Exception e) {
            log.error("Failed to find current WAL entry for transaction {}", transactionId, e);
            throw new DataAccessException("Failed to find current WAL entry by transaction", e) {};
        }
    }

    /**
     * Get complete version history for transaction (audit trail)
     */
    public List<WalEntry> getVersionHistory(UUID transactionId) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("transactionId", transactionId);

            List<WalEntry> versions = jdbcTemplate.query(SELECT_VERSION_HISTORY_SQL, params, WAL_ENTRY_ROW_MAPPER);
            log.debug("Found {} versions for transaction {}", versions.size(), transactionId);

            return versions;

        } catch (Exception e) {
            log.error("Failed to get version history for transaction {}", transactionId, e);
            throw new DataAccessException("Failed to get version history", e) {};
        }
    }

    /**
     * Get current stale entries that need recovery
     */
    public List<WalEntry> findCurrentStaleEntries(int maxAgeMinutes, int limit) {
        try {
            OffsetDateTime threshold = OffsetDateTime.now().minusMinutes(maxAgeMinutes);

            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("threshold", threshold)
                .addValue("limit", limit);

            List<WalEntry> entries = jdbcTemplate.query(SELECT_CURRENT_STALE_ENTRIES_SQL, params, WAL_ENTRY_ROW_MAPPER);
            log.debug("Found {} current stale WAL entries older than {} minutes", entries.size(), maxAgeMinutes);

            return entries;

        } catch (Exception e) {
            log.error("Failed to find current stale WAL entries", e);
            throw new DataAccessException("Failed to find current stale WAL entries", e) {};
        }
    }

    /**
     * Count current entries by status
     */
    public long countCurrentByStatus(WalEntry.WalStatus status) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("status", status.name());

            Long count = jdbcTemplate.queryForObject(COUNT_CURRENT_BY_STATUS_SQL, params, Long.class);
            return count != null ? count : 0L;

        } catch (Exception e) {
            log.error("Failed to count current WAL entries by status {}", status, e);
            throw new DataAccessException("Failed to count current WAL entries by status", e) {};
        }
    }

    /**
     * Get maximum sequence number from current entries
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
     * Async variants for non-blocking operations
     */
    public CompletableFuture<List<WalEntry>> findCurrentPendingEntriesAsync(int limit) {
        return CompletableFuture.supplyAsync(() -> findCurrentPendingEntries(limit), asyncExecutor);
    }

    public CompletableFuture<WalEntry> changeStatusAsync(UUID transactionId, WalEntry.WalStatus status, String errorMessage) {
        return CompletableFuture.supplyAsync(() -> changeStatus(transactionId, status, errorMessage), asyncExecutor);
    }

    public CompletableFuture<List<WalEntry>> getVersionHistoryAsync(UUID transactionId) {
        return CompletableFuture.supplyAsync(() -> getVersionHistory(transactionId), asyncExecutor);
    }

    /**
     * RowMapper for WalEntry with immutable fields
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