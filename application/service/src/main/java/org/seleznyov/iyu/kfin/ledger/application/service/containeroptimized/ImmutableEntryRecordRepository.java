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
 * IMMUTABLE Repository for EntryRecord operations
 * Only INSERT operations - no UPDATE or DELETE
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class ImmutableEntryRecordRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final Executor asyncExecutor;

    private static final String INSERT_ENTRY_SQL = """
        INSERT INTO ledger.entry_records 
        (id, account_id, transaction_id, entry_type, amount, created_at, operation_date, 
         idempotency_key, currency_code, entry_ordinal, original_entry_id, correction_type, is_active)
        VALUES (:id, :accountId, :transactionId, :entryType, :amount, :createdAt, :operationDate, 
                :idempotencyKey, :currencyCode, 
                COALESCE(:entryOrdinal, nextval('ledger_entry_ordinal_sequence')),
                :originalEntryId, :correctionType, :isActive)
        """;

    private static final String SELECT_ACTIVE_BY_ACCOUNT_SQL = """
        SELECT id, account_id, transaction_id, entry_type, amount, created_at, operation_date, 
               idempotency_key, currency_code, entry_ordinal, original_entry_id, correction_type, is_active
        FROM ledger.entry_records 
        WHERE account_id = :accountId 
        AND is_active = true
        ORDER BY entry_ordinal DESC 
        LIMIT :limit
        """;

    private static final String CALCULATE_ACTIVE_BALANCE_SQL = """
        SELECT COALESCE(SUM(
            CASE 
                WHEN entry_type = 'CREDIT' THEN amount 
                ELSE -amount 
            END), 0) as balance
        FROM ledger.entry_records 
        WHERE account_id = :accountId
        AND is_active = true
        AND entry_ordinal <= :maxOrdinal
        """;

    private static final String CALCULATE_BALANCE_CHANGE_SQL = """
        SELECT COALESCE(SUM(
            CASE 
                WHEN entry_type = 'CREDIT' THEN amount 
                ELSE -amount 
            END), 0) as balance_change
        FROM ledger.entry_records 
        WHERE account_id = :accountId
        AND is_active = true
        AND entry_ordinal > :sinceOrdinal
        AND entry_ordinal <= :maxOrdinal
        """;

    private static final String SELECT_BY_TRANSACTION_SQL = """
        SELECT id, account_id, transaction_id, entry_type, amount, created_at, operation_date, 
               idempotency_key, currency_code, entry_ordinal, original_entry_id, correction_type, is_active
        FROM ledger.entry_records 
        WHERE transaction_id = :transactionId
        ORDER BY entry_type DESC, created_at ASC
        """;

    private static final String SELECT_BY_IDEMPOTENCY_SQL = """
        SELECT id, account_id, transaction_id, entry_type, amount, created_at, operation_date, 
               idempotency_key, currency_code, entry_ordinal, original_entry_id, correction_type, is_active
        FROM ledger.entry_records 
        WHERE idempotency_key = :idempotencyKey
        ORDER BY created_at DESC
        LIMIT 1
        """;

    private static final String SELECT_LATEST_ACTIVE_ORDINAL_SQL = """
        SELECT MAX(entry_ordinal) as max_ordinal
        FROM ledger.entry_records 
        WHERE account_id = :accountId
        AND is_active = true
        """;

    private static final String COUNT_ACTIVE_OPERATIONS_SINCE_ORDINAL_SQL = """
        SELECT COUNT(*) as operation_count
        FROM ledger.entry_records 
        WHERE account_id = :accountId 
        AND is_active = true
        AND entry_ordinal > :sinceOrdinal
        """;

    private static final String SELECT_CORRECTIONS_FOR_ENTRY_SQL = """
        SELECT id, account_id, transaction_id, entry_type, amount, created_at, operation_date, 
               idempotency_key, currency_code, entry_ordinal, original_entry_id, correction_type, is_active
        FROM ledger.entry_records 
        WHERE original_entry_id = :originalEntryId
        ORDER BY created_at ASC
        """;

    private static final RowMapper<EntryRecord> ENTRY_RECORD_ROW_MAPPER = new EntryRecordRowMapper();

    /**
     * Insert single entry record (IMMUTABLE - only insert)
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
     * Batch insert entry records (IMMUTABLE - only insert)
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
     * Create storno (reversal) entries for original transaction
     */
    @Transactional
    public List<EntryRecord> createStornoEntries(UUID originalTransactionId, UUID newTransactionId, UUID newIdempotencyKey) {
        try {
            // Find original entries
            List<EntryRecord> originalEntries = findByTransactionId(originalTransactionId);

            if (originalEntries.isEmpty()) {
                throw new DataAccessException("No entries found for transaction " + originalTransactionId) {};
            }

            // Create storno entries (same amounts but opposite effect)
            List<EntryRecord> stornoEntries = originalEntries.stream()
                .map(original -> original.createStorno(newTransactionId, newIdempotencyKey))
                .toList();

            // Save storno entries
            List<EntryRecord> savedStornoEntries = saveAll(stornoEntries);

            log.info("Created {} storno entries for transaction {}", savedStornoEntries.size(), originalTransactionId);
            return savedStornoEntries;

        } catch (Exception e) {
            log.error("Failed to create storno entries for transaction {}", originalTransactionId, e);
            throw new DataAccessException("Failed to create storno entries", e) {};
        }
    }

    /**
     * Get active account entries with limit
     */
    public List<EntryRecord> findActiveByAccountId(UUID accountId, int limit) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("accountId", accountId)
                .addValue("limit", limit);

            List<EntryRecord> entries = jdbcTemplate.query(SELECT_ACTIVE_BY_ACCOUNT_SQL, params, ENTRY_RECORD_ROW_MAPPER);
            log.debug("Found {} active entries for account {}", entries.size(), accountId);

            return entries;

        } catch (Exception e) {
            log.error("Failed to find active entries for account {}", accountId, e);
            throw new DataAccessException("Failed to find active entries for account", e) {};
        }
    }

    /**
     * Calculate account balance from active entries up to specific ordinal
     */
    public long calculateActiveBalance(UUID accountId, long maxOrdinal) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("accountId", accountId)
                .addValue("maxOrdinal", maxOrdinal);

            Long balance = jdbcTemplate.queryForObject(CALCULATE_ACTIVE_BALANCE_SQL, params, Long.class);
            return balance != null ? balance : 0L;

        } catch (Exception e) {
            log.error("Failed to calculate active balance for account {} at ordinal {}", accountId, maxOrdinal, e);
            throw new DataAccessException("Failed to calculate active balance", e) {};
        }
    }

    /**
     * Calculate balance change from active entries since specific ordinal
     */
    public long calculateBalanceChangeSince(UUID accountId, long sinceOrdinal, long maxOrdinal) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("accountId", accountId)
                .addValue("sinceOrdinal", sinceOrdinal)
                .addValue("maxOrdinal", maxOrdinal);

            Long balanceChange = jdbcTemplate.queryForObject(CALCULATE_BALANCE_CHANGE_SQL, params, Long.class);
            return balanceChange != null ? balanceChange : 0L;

        } catch (Exception e) {
            log.error("Failed to calculate balance change since ordinal {} for account {}", sinceOrdinal, accountId, e);
            throw new DataAccessException("Failed to calculate balance change since ordinal", e) {};
        }
    }

    /**
     * Get entries by transaction ID (includes all versions/corrections)
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
     * Find most recent entry by idempotency key
     */
    public Optional<EntryRecord> findByIdempotencyKey(UUID idempotencyKey) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("idempotencyKey", idempotencyKey);

            List<EntryRecord> entries = jdbcTemplate.query(SELECT_BY_IDEMPOTENCY_SQL, params, ENTRY_RECORD_ROW_MAPPER);

            return entries.isEmpty() ? Optional.empty() : Optional.of(entries.get(0));

        } catch (Exception e) {
            log.error("Failed to find entry by idempotency key {}", idempotencyKey, e);
            throw new DataAccessException("Failed to find entry by idempotency key", e) {};
        }
    }

    /**
     * Get latest active ordinal for account
     */
    public OptionalLong getLatestActiveOrdinal(UUID accountId) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("accountId", accountId);

            Long maxOrdinal = jdbcTemplate.queryForObject(SELECT_LATEST_ACTIVE_ORDINAL_SQL, params, Long.class);
            return maxOrdinal != null ? OptionalLong.of(maxOrdinal) : OptionalLong.empty();

        } catch (Exception e) {
            log.error("Failed to get latest active ordinal for account {}", accountId, e);
            throw new DataAccessException("Failed to get latest active ordinal", e) {};
        }
    }

    /**
     * Count active operations since ordinal
     */
    public int countActiveOperationsSince(UUID accountId, long sinceOrdinal) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("accountId", accountId)
                .addValue("sinceOrdinal", sinceOrdinal);

            Integer count = jdbcTemplate.queryForObject(COUNT_ACTIVE_OPERATIONS_SINCE_ORDINAL_SQL, params, Integer.class);
            return count != null ? count : 0;

        } catch (Exception e) {
            log.error("Failed to count active operations since ordinal {} for account {}", sinceOrdinal, accountId, e);
            throw new DataAccessException("Failed to count active operations since ordinal", e) {};
        }
    }

    /**
     * Find all corrections/storno for an entry (audit trail)
     */
    public List<EntryRecord> findCorrectionsForEntry(UUID originalEntryId) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("originalEntryId", originalEntryId);

            List<EntryRecord> corrections = jdbcTemplate.query(SELECT_CORRECTIONS_FOR_ENTRY_SQL, params, ENTRY_RECORD_ROW_MAPPER);
            log.debug("Found {} corrections for entry {}", corrections.size(), originalEntryId);

            return corrections;

        } catch (Exception e) {
            log.error("Failed to find corrections for entry {}", originalEntryId, e);
            throw new DataAccessException("Failed to find corrections for entry", e) {};
        }
    }

    /**
     * Get complete audit trail for transaction (original + all corrections)
     */
    public Map<String, List<EntryRecord>> getTransactionAuditTrail(UUID transactionId) {
        try {
            List<EntryRecord> allEntries = findByTransactionId(transactionId);

            Map<String, List<EntryRecord>> auditTrail = new HashMap<>();

            // Separate original entries from corrections
            List<EntryRecord> originalEntries = allEntries;

            List<EntryRecord> corrections = allEntries;

            auditTrail.put("original", originalEntries);
            auditTrail.put("corrections", corrections);

            // Group corrections by type
            return auditTrail;

        } catch (Exception e) {
            log.error("Failed to get audit trail for transaction {}", transactionId, e);
            throw new DataAccessException("Failed to get audit trail for transaction", e) {};
        }
    }

    /**
     * Async variants for non-blocking operations
     */
    public CompletableFuture<List<EntryRecord>> findActiveByAccountIdAsync(UUID accountId, int limit) {
        return CompletableFuture.supplyAsync(() -> findActiveByAccountId(accountId, limit), asyncExecutor);
    }

    public CompletableFuture<Long> calculateActiveBalanceAsync(UUID accountId, long maxOrdinal) {
        return CompletableFuture.supplyAsync(() -> calculateActiveBalance(accountId, maxOrdinal), asyncExecutor);
    }

    public CompletableFuture<List<EntryRecord>> createStornoEntriesAsync(UUID originalTransactionId, UUID newTransactionId, UUID newIdempotencyKey) {
        return CompletableFuture.supplyAsync(() -> createStornoEntries(originalTransactionId, newTransactionId, newIdempotencyKey), asyncExecutor);
    }

    /**
     * Get all unique account IDs from active entries (for reconciliation)
     */
    public Set<UUID> getAllActiveAccountIds() {
        try {
            String sql = "SELECT DISTINCT account_id FROM ledger.entry_records WHERE is_active = true";

            List<UUID> accountIds = jdbcTemplate.query(sql, new MapSqlParameterSource(),
                (rs, rowNum) -> UUID.fromString(rs.getString("account_id")));

            Set<UUID> uniqueAccountIds = new HashSet<>(accountIds);
            log.debug("Found {} unique active account IDs", uniqueAccountIds.size());

            return uniqueAccountIds;

        } catch (Exception e) {
            log.error("Failed to get all active account IDs", e);
            throw new DataAccessException("Failed to get all active account IDs", e) {};
        }
    }

    /**
     * Get account IDs with activity since specific date (for targeted reconciliation)
     */
    public Set<UUID> getAccountIdsWithActivitySince(java.time.LocalDate sinceDate) {
        try {
            String sql = """
                SELECT DISTINCT account_id 
                FROM ledger.entry_records 
                WHERE is_active = true 
                AND operation_date >= :sinceDate
                """;

            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("sinceDate", sinceDate);

            List<UUID> accountIds = jdbcTemplate.query(sql, params,
                (rs, rowNum) -> UUID.fromString(rs.getString("account_id")));

            Set<UUID> uniqueAccountIds = new HashSet<>(accountIds);
            log.debug("Found {} account IDs with activity since {}", uniqueAccountIds.size(), sinceDate);

            return uniqueAccountIds;

        } catch (Exception e) {
            log.error("Failed to get account IDs with activity since {}", sinceDate, e);
            throw new DataAccessException("Failed to get account IDs with activity since date", e) {};
        }
    }

    /**
     * Async variant for getting all account IDs
     */
    public CompletableFuture<Set<UUID>> getAllActiveAccountIdsAsync() {
        return CompletableFuture.supplyAsync(this::getAllActiveAccountIds, asyncExecutor);
    }
    private static class EntryRecordRowMapper implements RowMapper<EntryRecord> {
        @Override
        public EntryRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
            String correctionTypeStr = rs.getString("correction_type");

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