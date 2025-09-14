package org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.jdbc.client;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.domain.model.snapshot.EntriesSnapshot;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class EntriesSnapshotClient {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    /**
     * Retrieves the most recent snapshot for account on or before specified date.
     */
    public Optional<EntriesSnapshot> findLatestSnapshot(UUID accountId, LocalDate operationDate) {
        try {
            log.debug("Finding latest snapshot for account {} on or before {}", accountId, operationDate);

            final List<EntriesSnapshot> result = jdbcTemplate.query(
                """
                    SELECT id, account_id, operation_date, balance, 
                           last_entry_record_id, last_entry_ordinal, operations_count
                    FROM ledger.entries_snapshots
                    WHERE account_id = :accountId
                      AND operation_date <= :operationDate
                    ORDER BY operation_date DESC, snapshot_ordinal DESC
                    LIMIT 1
                """,
                Map.of(
                    "accountId", accountId,
                    "operationDate", operationDate
                ),
                this::mapSnapshotRow
            );

            if (result.isEmpty()) {
                log.debug("No snapshot found for account {} on or before {}", accountId, operationDate);
                return Optional.empty();
            }

            EntriesSnapshot snapshot = result.get(0);
            log.debug("Found snapshot for account {}: balance={}, date={}, ordinal={}",
                accountId, snapshot.balance(), snapshot.operationDay(), snapshot.lastEntryOrdinal());

            return Optional.of(snapshot);

        } catch (Exception e) {
            log.error("Error finding snapshot for account {} on date {}: {}",
                accountId, operationDate, e.getMessage(), e);
            return Optional.empty();
        }
    }

    /**
     * Retrieves snapshot for exact account and date match.
     */
    public Optional<EntriesSnapshot> findExactSnapshot(UUID accountId, LocalDate operationDate) {
        try {
            log.debug("Finding exact snapshot for account {} on {}", accountId, operationDate);

            final List<EntriesSnapshot> result = jdbcTemplate.query(
                """
                    SELECT id, account_id, operation_date, balance, 
                           last_entry_record_id, last_entry_ordinal, operations_count
                    FROM ledger.entries_snapshots
                    WHERE account_id = :accountId
                      AND operation_date = :operationDate
                    ORDER BY snapshot_ordinal DESC
                    LIMIT 1
                """,
                Map.of(
                    "accountId", accountId,
                    "operationDate", operationDate
                ),
                this::mapSnapshotRow
            );

            if (result.isEmpty()) {
                log.debug("No exact snapshot found for account {} on {}", accountId, operationDate);
                return Optional.empty();
            }

            return Optional.of(result.get(0));

        } catch (Exception e) {
            log.error("Error finding exact snapshot for account {} on date {}: {}",
                accountId, operationDate, e.getMessage(), e);
            return Optional.empty();
        }
    }

    /**
     * Legacy method for backward compatibility.
     * @deprecated Use findExactSnapshot() instead for better error handling
     */
    @Deprecated
    public EntriesSnapshot snapshot(UUID accountId, LocalDate operationDate) {
        Optional<EntriesSnapshot> snapshot = findExactSnapshot(accountId, operationDate);

        if (snapshot.isEmpty()) {
            throw new IllegalStateException(
                "No snapshot found for account " + accountId + " on date " + operationDate);
        }

        return snapshot.get();
    }

    private EntriesSnapshot mapSnapshotRow(ResultSet rs, int rowNum) throws SQLException {
        return new EntriesSnapshot(
            UUID.fromString(rs.getString("id")),
            UUID.fromString(rs.getString("account_id")),
            rs.getDate("operation_date").toLocalDate(),
            rs.getLong("balance"),
            parseUuidSafely(rs.getString("last_entry_record_id")),
            rs.getLong("last_entry_ordinal"),
            rs.getInt("operations_count")
        );
    }

    private UUID parseUuidSafely(String uuidString) {
        if (uuidString == null || uuidString.trim().isEmpty()) {
            return null;
        }
        try {
            return UUID.fromString(uuidString);
        } catch (IllegalArgumentException e) {
            log.warn("Invalid UUID format in database: {}", uuidString);
            return null;
        }
    }
}