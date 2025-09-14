package org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.jdbc.client;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.domain.model.snapshot.EntriesSnapshot;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Slf4j
@RequiredArgsConstructor
public class EntriesSnapshotInsertClient {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public void insert(EntriesSnapshot snapshot) {
        try {
            log.debug("Inserting snapshot: account={}, date={}, balance={}",
                snapshot.accountId(), snapshot.operationDay(), snapshot.balance());
            int rowsAffected = jdbcTemplate.update(
                """
                        INSERT INTO ledger.entries_snapshots 
                        (account_id, operation_date, balance, last_entry_record_id, 
                         last_entry_ordinal, operations_count, created_at)
                        VALUES (:accountId, :operationDate, :balance, :lastEntryRecordId, 
                                :lastEntryOrdinal, :operationsCount, CURRENT_TIMESTAMP)
                    """,
                Map.of(
                    "accountId", snapshot.accountId(),
                    "operationDate", snapshot.operationDay(),
                    "balance", snapshot.balance(),
                    "lastEntryRecordId", snapshot.lastEntryRecordId(),
                    "lastEntryOrdinal", snapshot.lastEntryOrdinal(),
                    "operationsCount", snapshot.operationsCount()
                )
            );
            if (rowsAffected != 1) {
                throw new RuntimeException("Expected 1 row affected, got " + rowsAffected);
            }

            log.info("Snapshot inserted successfully: account={}, date={}, balance={}",
                snapshot.accountId(), snapshot.operationDay(), snapshot.balance());

        } catch (Exception e) {
            log.error("Failed to insert snapshot for account {} on date {}: {}",
                snapshot.accountId(), snapshot.operationDay(), e.getMessage(), e);
            throw new RuntimeException("Failed to insert snapshot: " + e.getMessage(), e);
        }
    }
}
