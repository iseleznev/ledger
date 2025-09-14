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
 * Repository for EntrySnapshot operations using JDBC Template
 * Optimized for fast balance calculations
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class EntrySnapshotRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final Executor asyncExecutor;

    private static final String INSERT_SNAPSHOT_SQL = """
        INSERT INTO ledger.entries_snapshots 
        (id, account_id, operation_date, balance, last_entry_record_id, last_entry_ordinal,
         operations_count, snapshot_ordinal, created_at)
        VALUES (:id, :accountId, :operationDate, :balance, :lastEntryRecordId, :lastEntryOrdinal,
                :operationsCount, COALESCE(:snapshotOrdinal, nextval('ledger_snapshot_ordinal_sequence')), :createdAt)
        """;

    private static final String SELECT_LATEST_FOR_ACCOUNT_SQL = """
        SELECT id, account_id, operation_date, balance, last_entry_record_id, last_entry_ordinal,
               operations_count, snapshot_ordinal, created_at
        FROM ledger.entries_snapshots
        WHERE account_id = :accountId
        ORDER BY operation_date DESC, snapshot_ordinal DESC
        LIMIT 1
        """;

    private static final String SELECT_LATEST_BEFORE_DATE_SQL = """
        SELECT id, account_id, operation_date, balance, last_entry_record_id, last_entry_ordinal,
               operations_count, snapshot_ordinal, created_at
        FROM ledger.entries_snapshots
        WHERE account_id = :accountId AND operation_date <= :operationDate
        ORDER BY operation_date DESC, snapshot_ordinal DESC
        LIMIT 1
        """;

    private static final String SELECT_BY_ACCOUNT_AND_DATE_SQL = """
        SELECT id, account_id, operation_date, balance, last_entry_record_id, last_entry_ordinal,
               operations_count, snapshot_ordinal, created_at
        FROM ledger.entries_snapshots
        WHERE account_id = :accountId AND operation_date = :operationDate
        ORDER BY snapshot_ordinal DESC
        LIMIT :limit
        """;

    private static final String SELECT_STALE_SNAPSHOTS_SQL = """
        SELECT id, account_id, operation_date, balance, last_entry_record_id, last_entry_ordinal,
               operations_count, snapshot_ordinal, created_at
        FROM ledger.entries_snapshots
        WHERE created_at < :threshold
        ORDER BY created_at
        LIMIT :limit
        """;

    private static final String DELETE_OLD_SNAPSHOTS_SQL = """
        DELETE FROM ledger.entries_snapshots
        WHERE account_id = :accountId
        AND (account_id, operation_date, snapshot_ordinal) NOT IN (
            SELECT account_id, operation_date, snapshot_ordinal
            FROM ledger.entries_snapshots
            WHERE account_id = :accountId
            ORDER BY operation_date DESC, snapshot_ordinal DESC
            LIMIT :keepCount
        )
        """;

    private static final String COUNT_SNAPSHOTS_FOR_ACCOUNT_SQL = """
        SELECT COUNT(*) FROM ledger.entries_snapshots WHERE account_id = :accountId
        """;

    private static final String SELECT_ACCOUNTS_NEEDING_SNAPSHOTS_SQL = """
        SELECT DISTINCT er.account_id
        FROM ledger.entry_records er
        LEFT JOIN ledger.entries_snapshots es ON er.account_id = es.account_id
        WHERE es.account_id IS NULL 
        OR (er.entry_ordinal - es.last_entry_ordinal) >= :threshold
        LIMIT :limit
        """;

    private static final RowMapper<EntrySnapshot> ENTRY_SNAPSHOT_ROW_MAPPER = new EntrySnapshotRowMapper();

    /**
     * Save entry snapshot
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
                .addValue("createdAt", snapshot.getCreatedAt());

            int rowsAffected = jdbcTemplate.update(INSERT_SNAPSHOT_SQL, params);

            if (rowsAffected != 1) {
                throw new DataAccessException("Failed to insert snapshot, rows affected: " + rowsAffected) {};
            }

            log.debug("Saved snapshot for account {} on date {}", snapshot.getAccountId(), snapshot.getOperationDate());
            return snapshot;

        } catch (Exception e) {
            log.error("Failed to save snapshot for account {}", snapshot.getAccountId(), e);
            throw new DataAccessException("Failed to save snapshot", e) {};
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
                        .addValue("createdAt", snapshot.getCreatedAt());
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
     * Get latest snapshot for account
     */
    public Optional<EntrySnapshot> findLatestByAccountId(UUID accountId) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("accountId", accountId);

            List<EntrySnapshot> snapshots = jdbcTemplate.query(SELECT_LATEST_FOR_ACCOUNT_SQL, params, ENTRY_SNAPSHOT_ROW_MAPPER);

            return snapshots.isEmpty() ? Optional.empty() : Optional.of(snapshots.get(0));

        } catch (Exception e) {
            log.error("Failed to find latest snapshot for account {}", accountId, e);
            throw new DataAccessException("Failed to find latest snapshot", e) {};
        }
    }

    /**
     * Get latest snapshot before or on specific date
     */
    public Optional<EntrySnapshot> findLatestBeforeDate(UUID accountId, LocalDate operationDate) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("accountId", accountId)
                .addValue("operationDate", operationDate);

            List<EntrySnapshot> snapshots = jdbcTemplate.query(SELECT_LATEST_BEFORE_DATE_SQL, params, ENTRY_SNAPSHOT_ROW_MAPPER);

            return snapshots.isEmpty() ? Optional.empty() : Optional.of(snapshots.get(0));

        } catch (Exception e) {
            log.error("Failed to find snapshot before date {} for account {}", operationDate, accountId, e);
            throw new DataAccessException("Failed to find snapshot before date", e) {};
        }
    }

    /**
     * Get snapshots for specific account and date
     */
    public List<EntrySnapshot> findByAccountAndDate(UUID accountId, LocalDate operationDate, int limit) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("accountId", accountId)
                .addValue("operationDate", operationDate)
                .addValue("limit", limit);

            List<EntrySnapshot> snapshots = jdbcTemplate.query(SELECT_BY_ACCOUNT_AND_DATE_SQL, params, ENTRY_SNAPSHOT_ROW_MAPPER);
            log.debug("Found {} snapshots for account {} on date {}", snapshots.size(), accountId, operationDate);

            return snapshots;

        } catch (Exception e) {
            log.error("Failed to find snapshots for account {} on date {}", accountId, operationDate, e);
            throw new DataAccessException("Failed to find snapshots by account and date", e) {};
        }
    }

    /**
     * Find stale snapshots for cleanup
     */
    public List<EntrySnapshot> findStaleSnapshots(int maxAgeDays, int limit) {
        try {
            OffsetDateTime threshold = OffsetDateTime.now().minusDays(maxAgeDays);

            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("threshold", threshold)
                .addValue("limit", limit);

            List<EntrySnapshot> snapshots = jdbcTemplate.query(SELECT_STALE_SNAPSHOTS_SQL, params, ENTRY_SNAPSHOT_ROW_MAPPER);
            log.debug("Found {} stale snapshots older than {} days", snapshots.size(), maxAgeDays);

            return snapshots;

        } catch (Exception e) {
            log.error("Failed to find stale snapshots", e);
            throw new DataAccessException("Failed to find stale snapshots", e) {};
        }
    }

    /**
     * Clean up old snapshots for account (keep only the latest N)
     */
    @Transactional
    public int cleanupOldSnapshots(UUID accountId, int keepCount) {
        try {
            SqlParameterSource params = new MapSqlParameterSource()
                .addValue("accountId", accountId)
                .addValue("keepCount", keepCount);

            int deletedCount = jdbcTemplate.update(DELETE_OLD_SNAPSHOTS_SQL, params);
            log.debug("Cleaned up {} old snapshots for account {}, kept {}", deletedCount, accountId, keepCount);

            return deletedCount;

        } catch (Exception e) {
            log.error("Failed to cleanup old snapshots for account {}", accountId, e);
            throw new DataAccessException("Failed to cleanup old snapshots", e) {};
        }
    }

    /**
     * Count snapshots for account
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
     * Batch find latest snapshots for multiple accounts
     */
    public Map<UUID, EntrySnapshot> findLatestByAccountIds(Set<UUID> accountIds) {
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
                )
                SELECT id, account_id, operation_date, balance, last_entry_record_id, last_entry_ordinal,
                       operations_count, snapshot_ordinal, created_at
                FROM ranked_snapshots WHERE rn = 1
                """;

            List<Object> paramList = new ArrayList<>(accountIds);

            List<EntrySnapshot> snapshots = jdbcTemplate.query(sql, paramList.toArray(), ENTRY_SNAPSHOT_ROW_MAPPER);

            return snapshots.stream()
                .collect(Collectors.toMap(EntrySnapshot::getAccountId, snapshot -> snapshot));

        } catch (Exception e) {
            log.error("Failed to batch find latest snapshots for {} accounts", accountIds.size(), e);
            throw new DataAccessException("Failed to batch find latest snapshots", e) {};
        }
    }

    /**
     * Async variants
     */
    public CompletableFuture<Optional<EntrySnapshot>> findLatestByAccountIdAsync(UUID accountId) {
        return CompletableFuture.supplyAsync(() -> findLatestByAccountId(accountId), asyncExecutor);
    }

    public CompletableFuture<EntrySnapshot> saveAsync(EntrySnapshot snapshot) {
        return CompletableFuture.supplyAsync(() -> save(snapshot), asyncExecutor);
    }

    public CompletableFuture<Map<UUID, EntrySnapshot>> findLatestByAccountIdsAsync(Set<UUID> accountIds) {
        return CompletableFuture.supplyAsync(() -> findLatestByAccountIds(accountIds), asyncExecutor);
    }

    /**
     * RowMapper for EntrySnapshot
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
                .build();
        }
    }
}