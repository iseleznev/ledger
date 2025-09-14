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
import java.util.stream.Collectors;

/**
 * IMMUTABLE Repository for EntrySnapshot operations
 * Only INSERT operations - snapshots are immutable by design
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class ImmutableEntrySnapshotRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final Executor asyncExecutor;

    private static final String INSERT_SNAPSHOT_SQL = """
        INSERT INTO ledger.entries_snapshots 
        (id, account_id, operation_date, balance, last_entry_record_id, last_entry_ordinal,
         operations_count, snapshot_ordinal, created_at, is_current)
        VALUES (:id, :accountId, :operationDate, :balance, :lastEntryRecordId, :lastEntryOrdinal,
                :operationsCount, COALESCE(:snapshotOrdinal, nextval('ledger_snapshot_ordinal_sequence')), 
                :createdAt, :isCurrent)
        """;

    private static final String MARK_PREVIOUS_AS_NON_CURRENT_SQL = """
        INSERT INTO ledger.entries_snapshots 
        (id, account_id, operation_date, balance, last_entry_record_id, last_entry_ordinal,
         operations_count, snapshot_ordinal, created_at, is_current)
        SELECT id, account_id, operation_date, balance, last_entry_record_id, last_entry_ordinal,
               operations_count, snapshot_ordinal, created_at, false
        FROM ledger.entries_snapshots
        WHERE account_id = :accountId AND operation_date = :operationDate AND is_current = true
        """;

    private static final String SELECT_CURRENT_LATEST_FOR_ACCOUNT_SQL = """
        SELECT id, account_id, operation_date, balance, last_entry_record_id, last_entry_ordinal,
               operations_count, snapshot_ordinal, created_at, is_current
        FROM ledger.entries_snapshots
        WHERE account_id = :accountId
        AND is_current = true
        ORDER BY operation_date DESC, snapshot_ordinal DESC
        LIMIT 1
        """;

    private static final String SELECT_CURRENT_LATEST_BEFORE_DATE_SQL = """
        SELECT id, account_id, operation_date, balance, last_entry_record_id, last_entry_ordinal,
               operations_count, snapshot_ordinal, created_at, is_current
        FROM ledger.entries_snapshots
        WHERE account_id = :accountId 
        AND operation_date <= :operationDate
        AND is_current = true
        ORDER BY operation_date DESC, snapshot_ordinal DESC
        LIMIT 1
        """;

    private static final String SELECT_ALL_BY_ACCOUNT_AND_DATE_SQL = """
        SELECT id, account_id, operation_date, balance, last_entry_record_id, last_entry_ordinal,
               operations_count, snapshot_ordinal, created_at, is_current
        FROM ledger.entries_snapshots
        WHERE account_id = :accountId AND operation_date = :operationDate
        ORDER BY snapshot_ordinal DESC, created_at DESC
        LIMIT :limit
        """;

    private static final String SELECT_CURRENT_BY_ACCOUNT_AND_DATE_SQL = """
        SELECT id, account_id, operation_date, balance, last_entry_record_id, last_entry_ordinal,
               operations_count, snapshot_ordinal, created_at, is_current
        FROM ledger.entries_snapshots
        WHERE account_id = :accountId 
        AND operation_date = :operationDate
        AND is_current = true
        ORDER BY snapshot_ordinal DESC
        LIMIT 1
        """;

    private static final String SELECT_OLD_SNAPSHOTS_SQL = """
        SELECT id, account_id, operation_date, balance, last_entry_record_id, last_entry_ordinal,
               operations_count, snapshot_ordinal, created_at, is_current
        FROM ledger.entries_snapshots
        WHERE created_at < :threshold
        ORDER BY created_at
        LIMIT :limit
        """;

    private static final String COUNT_SNAPSHOTS_FOR_ACCOUNT_SQL = """
        SELECT COUNT(*) FROM ledger.entries_snapshots WHERE account_id = :accountId
        """;

    private static final String COUNT_CURRENT_SNAPSHOTS_FOR_ACCOUNT_SQL = """
        SELECT COUNT(*) FROM ledger.entries_snapshots 
        WHERE account_id = :accountId AND is_current = true
        """;

    private static final String SELECT_ACCOUNTS_NEEDING_SNAPSHOTS_SQL = """
        SELECT DISTINCT er.account_id
        FROM ledger.entry_records er
        LEFT JOIN ledger.entries_snapshots es ON er.account_id = es.account_id AND es.is_current = true
        WHERE er.is_active = true
        AND (es.account_id IS NULL OR (er.entry_ordinal - es.last_entry_ordinal) >= :threshold)
        LIMIT :limit
        """;

    private static final RowMapper<EntrySnapshot> ENTRY_SNAPSHOT_ROW_MAPPER = new EntrySnapshotRowMapper();

    /**
     * Save entry snapshot (IMMUTABLE - only insert)
     */
    @Transactional
    public EntrySnapshot save(EntrySnapshot snapshot) {
        try {
            if (snapshot.getId() == null) {
                snapshot.setId(UUID.randomUUID());
            }

            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", snapshot.getId())
                .addValue("accountId", snapshot.getAccountId())
                .addValue("operationDate", snapshot.getOperationDate())
                .addValue("balance", snapshot.getBalance())
                .addValue("lastEntryRecordId", snapshot.getLastEntryRecordId())
                .addValue("lastEntryOrdinal", snapshot.getLastEntryOrdinal())
                .addValue("operationsCount", snapshot.getOperationsCount())
                .addValue("snapshotOrdinal", snapshot.getSnapshotOrdinal())
                .addValue("createdAt", snapshot.getCreatedAt())
                .addValue("isCurrent", snapshot.isCurrent());

            int rowsAffected = jdbcTemplate.update(INSERT_SNAPSHOT_SQL, params);

            if (rowsAffected != 1) {
                throw new DataAccessException("Failed to insert snapshot, rows affected: " + rowsAffected) {};
            }

            log.debug("Saved snapshot for account {} on date {} (current: {})",
                snapshot.getAccountId(), snapshot.getOperationDate(), snapshot.isCurrent());
            return snapshot;

        } catch (Exception e) {
            log.error("Failed to save snapshot for account {}", snapshot.getAccountId(), e);
            throw new DataAccessException("Failed to save snapshot", e) {};
        }
    }

    /**
     * Create superseding snapshot (marks previous as non-current via new records)
     */
    @Transactional
    public EntrySnapshot createSuperseding(EntrySnapshot newSnapshot) {
        try {
            // Mark existing current snapshots as non-current by creating new records
            markPreviousAsNonCurrent(newSnapshot.getAccountId(), newSnapshot.getOperationDate());

            // Save new current snapshot
            newSnapshot.setCurrent(true);
            return save(newSnapshot);

        } catch (Exception e) {
            log.error("Failed to create superseding snapshot for account {} on date {}",
                newSnapshot.getAccountId(), newSnapshot.getOperationDate(), e);
            throw new DataAccessException("Failed to create superseding snapshot", e) {};
        }
    }

    /**
     * Mark previous snapshots as non-current by creating new records (IMMUTABLE approach)
     */
    @Transactional
    public void markPreviousAsNonCurrent(UUID accountId, LocalDate operationDate) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("accountId", accountId)
                .addValue("operationDate", operationDate);

            int rowsAffected = jdbcTemplate.update(MARK_PREVIOUS_AS_NON_CURRENT_SQL, params);

            log.debug("Marked {} previous snapshots as non-current for account {} on date {}",
                rowsAffected, accountId, operationDate);

        } catch (Exception e) {
            log.error("Failed to mark previous snapshots as non-current for account {} on date {}",
                accountId, operationDate, e);
            throw new DataAccessException("Failed to mark previous snapshots as non-current", e) {};
        }
    }

    /**
     * Batch save snapshots
     */
    @Transactional
    public List<EntrySnapshot> saveAll(List<EntrySnapshot> snapshots) {
        if (snapshots.isEmpty()) {
            return snapshots;
        }

        try {
            SqlParameterSource[] batchParams = snapshots.stream()
                .map(snapshot -> {
                    if (snapshot.getId() == null) {
                        snapshot.setId(UUID.randomUUID());
                    }

                    return new MapSqlParameterSource()
                        .addValue("id", snapshot.getId())
                        .addValue("accountId", snapshot.getAccountId())
                        .addValue("operationDate", snapshot.getOperationDate())
                        .addValue("balance", snapshot.getBalance())
                        .addValue("lastEntryRecordId", snapshot.getLastEntryRecordId())
                        .addValue("lastEntryOrdinal", snapshot.getLastEntryOrdinal())
                        .addValue("operationsCount", snapshot.getOperationsCount())
                        .addValue("snapshotOrdinal", snapshot.getSnapshotOrdinal())
                        .addValue("createdAt", snapshot.getCreatedAt())
                        .addValue("isCurrent", snapshot.isCurrent());
                })
                .toArray(SqlParameterSource[]::new);

            int[] rowsAffected = jdbcTemplate.batchUpdate(INSERT_SNAPSHOT_SQL, batchParams);

            int totalRows = Arrays.stream(rowsAffected).sum();
            if (totalRows != snapshots.size()) {
                throw new DataAccessException("Batch snapshot insert failed, expected " + snapshots.size() +
                    " rows, affected " + totalRows) {};
            }

            log.debug("Batch saved {} snapshots", snapshots.size());
            return snapshots;

        } catch (Exception e) {
            log.error("Failed to batch save {} snapshots", snapshots.size(), e);
            throw new DataAccessException("Failed to batch save snapshots", e) {};
        }
    }

    /**
     * Get current latest snapshot for account
     */
    public Optional<EntrySnapshot> findCurrentLatestByAccountId(UUID accountId) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("accountId", accountId);

            List<EntrySnapshot> snapshots = jdbcTemplate.query(SELECT_CURRENT_LATEST_FOR_ACCOUNT_SQL,
                params, ENTRY_SNAPSHOT_ROW_MAPPER);

            return snapshots.isEmpty() ? Optional.empty() : Optional.of(snapshots.get(0));

        } catch (Exception e) {
            log.error("Failed to find current latest snapshot for account {}", accountId, e);
            throw new DataAccessException("Failed to find current latest snapshot", e) {};
        }
    }

    /**
     * Get current latest snapshot before or on specific date
     */
    public Optional<EntrySnapshot> findCurrentLatestBeforeDate(UUID accountId, LocalDate operationDate) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("accountId", accountId)
                .addValue("operationDate", operationDate);

            List<EntrySnapshot> snapshots = jdbcTemplate.query(SELECT_CURRENT_LATEST_BEFORE_DATE_SQL,
                params, ENTRY_SNAPSHOT_ROW_MAPPER);

            return snapshots.isEmpty() ? Optional.empty() : Optional.of(snapshots.get(0));

        } catch (Exception e) {
            log.error("Failed to find current snapshot before date {} for account {}", operationDate, accountId, e);
            throw new DataAccessException("Failed to find current snapshot before date", e) {};
        }
    }

    /**
     * Get current snapshot for specific account and date
     */
    public Optional<EntrySnapshot> findCurrentByAccountAndDate(UUID accountId, LocalDate operationDate) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("accountId", accountId)
                .addValue("operationDate", operationDate);

            List<EntrySnapshot> snapshots = jdbcTemplate.query(SELECT_CURRENT_BY_ACCOUNT_AND_DATE_SQL,
                params, ENTRY_SNAPSHOT_ROW_MAPPER);

            return snapshots.isEmpty() ? Optional.empty() : Optional.of(snapshots.get(0));

        } catch (Exception e) {
            log.error("Failed to find current snapshot for account {} on date {}", accountId, operationDate, e);
            throw new DataAccessException("Failed to find current snapshot by account and date", e) {};
        }
    }

    /**
     * Get all snapshots (including non-current) for specific account and date
     */
    public List<EntrySnapshot> findAllByAccountAndDate(UUID accountId, LocalDate operationDate, int limit) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("accountId", accountId)
                .addValue("operationDate", operationDate)
                .addValue("limit", limit);

            List<EntrySnapshot> snapshots = jdbcTemplate.query(SELECT_ALL_BY_ACCOUNT_AND_DATE_SQL,
                params, ENTRY_SNAPSHOT_ROW_MAPPER);
            log.debug("Found {} snapshots (all versions) for account {} on date {}",
                snapshots.size(), accountId, operationDate);

            return snapshots;

        } catch (Exception e) {
            log.error("Failed to find all snapshots for account {} on date {}", accountId, operationDate, e);
            throw new DataAccessException("Failed to find all snapshots by account and date", e) {};
        }
    }

    /**
     * Find old snapshots for potential cleanup/archival
     */
    public List<EntrySnapshot> findOldSnapshots(int maxAgeDays, int limit) {
        try {
            OffsetDateTime threshold = OffsetDateTime.now().minusDays(maxAgeDays);

            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("threshold", threshold)
                .addValue("limit", limit);

            List<EntrySnapshot> snapshots = jdbcTemplate.query(SELECT_OLD_SNAPSHOTS_SQL,
                params, ENTRY_SNAPSHOT_ROW_MAPPER);
            log.debug("Found {} old snapshots older than {} days", snapshots.size(), maxAgeDays);

            return snapshots;

        } catch (Exception e) {
            log.error("Failed to find old snapshots", e);
            throw new DataAccessException("Failed to find old snapshots", e) {};
        }
    }

    /**
     * Count all snapshots for account
     */
    public long countByAccountId(UUID accountId) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("accountId", accountId);

            Long count = jdbcTemplate.queryForObject(COUNT_SNAPSHOTS_FOR_ACCOUNT_SQL, params, Long.class);
            return count != null ? count : 0L;

        } catch (Exception e) {
            log.error("Failed to count snapshots for account {}", accountId, e);
            throw new DataAccessException("Failed to count snapshots for account", e) {};
        }
    }

    /**
     * Count current snapshots for account
     */
    public long countCurrentByAccountId(UUID accountId) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("accountId", accountId);

            Long count = jdbcTemplate.queryForObject(COUNT_CURRENT_SNAPSHOTS_FOR_ACCOUNT_SQL, params, Long.class);
            return count != null ? count : 0L;

        } catch (Exception e) {
            log.error("Failed to count current snapshots for account {}", accountId, e);
            throw new DataAccessException("Failed to count current snapshots for account", e) {};
        }
    }

    /**
     * Find accounts that need snapshots based on threshold
     */
    public List<UUID> findAccountsNeedingSnapshots(int threshold, int limit) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("threshold", threshold)
                .addValue("limit", limit);

            List<UUID> accountIds = jdbcTemplate.query(SELECT_ACCOUNTS_NEEDING_SNAPSHOTS_SQL, params,
                (rs, rowNum) -> UUID.fromString(rs.getString("account_id")));

            log.debug("Found {} accounts needing snapshots with threshold {}", accountIds.size(), threshold);
            return accountIds;

        } catch (Exception e) {
            log.error("Failed to find accounts needing snapshots", e);
            throw new DataAccessException("Failed to find accounts needing snapshots", e) {};
        }
    }

    /**
     * Batch find current latest snapshots for multiple accounts
     */
    public Map<UUID, EntrySnapshot> findCurrentLatestByAccountIds(Set<UUID> accountIds) {
        if (accountIds.isEmpty()) {
            return Collections.emptyMap();
        }

        try {
            // Using IN clause for batch lookup
            String inClause = String.join(",", Collections.nCopies(accountIds.size(), "?"));
            String sql = """
                WITH ranked_snapshots AS (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY operation_date DESC, snapshot_ordinal DESC) as rn
                    FROM ledger.entries_snapshots
                    WHERE account_id IN (""" + inClause + """)
                    AND is_current = true
                )
                SELECT id, account_id, operation_date, balance, last_entry_record_id, last_entry_ordinal,
                       operations_count, snapshot_ordinal, created_at, is_current
                FROM ranked_snapshots WHERE rn = 1
                """;

            List<Object> paramList = new ArrayList<>(accountIds);

            List<EntrySnapshot> snapshots = jdbcTemplate.query(sql, paramList.toArray(), ENTRY_SNAPSHOT_ROW_MAPPER);

            return snapshots.stream()
                .collect(Collectors.toMap(EntrySnapshot::getAccountId, snapshot -> snapshot));

        } catch (Exception e) {
            log.error("Failed to batch find current latest snapshots for {} accounts", accountIds.size(), e);
            throw new DataAccessException("Failed to batch find current latest snapshots", e) {};
        }
    }

    /**
     * Async variants
     */
    public CompletableFuture<Optional<EntrySnapshot>> findCurrentLatestByAccountIdAsync(UUID accountId) {
        return CompletableFuture.supplyAsync(() -> findCurrentLatestByAccountId(accountId), asyncExecutor);
    }

    public CompletableFuture<EntrySnapshot> saveAsync(EntrySnapshot snapshot) {
        return CompletableFuture.supplyAsync(() -> save(snapshot), asyncExecutor);
    }

    public CompletableFuture<EntrySnapshot> createSupersedingAsync(EntrySnapshot newSnapshot) {
        return CompletableFuture.supplyAsync(() -> createSuperseding(newSnapshot), asyncExecutor);
    }

    public CompletableFuture<Map<UUID, EntrySnapshot>> findCurrentLatestByAccountIdsAsync(Set<UUID> accountIds) {
        return CompletableFuture.supplyAsync(() -> findCurrentLatestByAccountIds(accountIds), asyncExecutor);
    }

    /**
     * RowMapper for EntrySnapshot with immutable fields
     */
    private static class EntrySnapshotRowMapper implements RowMapper<EntrySnapshot> {
        @Override
        public EntrySnapshot mapRow(ResultSet rs, int rowNum) throws SQLException {
            return EntrySnapshot.builder()
                .id(UUID.fromString(rs.getString("id")))
                .accountId(UUID.fromString(rs.getString("account_id")))
                .operationDate(rs.getObject("operation_date", LocalDate.class))
                .balance(rs.getLong("balance"))
                .lastEntryRecordId(UUID.fromString(rs.getString("last_entry_record_id")))
                .lastEntryOrdinal(rs.getLong("last_entry_ordinal"))
                .operationsCount(rs.getInt("operations_count"))
                .snapshotOrdinal(rs.getLong("snapshot_ordinal"))
                .createdAt(rs.getObject("created_at", OffsetDateTime.class))
                .isCurrent(rs.getBoolean("is_current"))
                .build();
        }
    }
}