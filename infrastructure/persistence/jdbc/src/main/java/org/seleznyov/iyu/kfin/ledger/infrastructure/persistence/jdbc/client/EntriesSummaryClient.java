package org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.jdbc.client;

import lombok.RequiredArgsConstructor;
import org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.api.model.result.EntriesSummaryResult;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class EntriesSummaryClient {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public EntriesSummaryResult entriesSummary(UUID accountId, LocalDate operationDate) {
        final List<EntriesSummaryResult> result = jdbcTemplate.query("""
                WITH latest_snapshot AS (
                    SELECT balance as snapshot_balance, last_entry_ordinal as snapshot_last_ordinal
                    FROM ledger.entries_snapshots
                    WHERE account_id = :accountId
                      AND operation_date <= :operationDate
                    ORDER BY operation_date DESC, snapshot_ordinal DESC
                    LIMIT 1
                ),
                operations_data AS (
                    SELECT
                        COUNT(*)::INTEGER as operations_count,
                        MAX(entry_ordinal) as max_entry_ordinal,
                        COALESCE(SUM(
                            CASE WHEN entry_type = 'CREDIT' THEN amount ELSE -amount END
                        ), 0) as operations_balance,
                        (ARRAY_AGG(id ORDER BY entry_ordinal DESC))[1] as last_entry_record_id
                    FROM ledger.entry_records er
                    WHERE er.account_id = :accountId
                      AND er.operation_date <= :operationDate
                      AND er.entry_ordinal > COALESCE((SELECT snapshot_last_ordinal FROM latest_snapshot), 0)
                )
                SELECT
                    COALESCE(ls.snapshot_balance, 0) + od.operations_balance as total_balance,
                    od.max_entry_ordinal,
                    od.last_entry_record_id as last_entry_record_id,
                    od.operations_count
                FROM latest_snapshot ls
                FULL OUTER JOIN operations_data od ON true
                WHERE od.operations_count > 0
                """,
            new MapSqlParameterSource()
                .addValue("accountId", accountId)
                .addValue("operationDate", operationDate),
            (rs, rowNum) -> EntriesSummaryResult.of(
                accountId,
                rs.getLong("total_balance"),
                UUID.fromString(rs.getString("last_entry_record_id")),
                rs.getLong("max_entry_ordinal"),
                rs.getInt("operations_count")
            )
        );
        if (result.isEmpty()) {
            throw new IllegalStateException();
        }
        return result.getFirst();
    }
}
