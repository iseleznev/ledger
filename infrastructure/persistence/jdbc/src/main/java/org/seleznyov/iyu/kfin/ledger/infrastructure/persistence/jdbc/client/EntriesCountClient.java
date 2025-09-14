package org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.jdbc.client;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class EntriesCountClient {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public Integer entriesCount(UUID accountId, LocalDate operationDate) {
        return jdbcTemplate.queryForObject("""
                WITH latest_snapshot AS (
                    SELECT last_entry_ordinal
                    FROM ledger.entries_snapshots
                    WHERE account_id = :accountId
                      AND operation_date <= :operationDate
                    ORDER BY operation_date DESC, snapshot_ordinal DESC
                    LIMIT 1
                )
                SELECT COUNT(*)::INTEGER
                FROM ledger.entry_records er
                WHERE er.account_id = :accountId
                  AND er.operation_date <= :operationDate
                  AND er.entry_ordinal > COALESCE((SELECT last_entry_ordinal FROM latest_snapshot), 0)
                """,
            new MapSqlParameterSource()
                .addValue("accountId", accountId)
                .addValue("operationDate", operationDate),
            Integer.class
        );
    }
}
