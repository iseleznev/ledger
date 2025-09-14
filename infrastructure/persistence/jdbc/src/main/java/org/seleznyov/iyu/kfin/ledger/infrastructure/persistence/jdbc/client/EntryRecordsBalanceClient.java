package org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.jdbc.client;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class EntryRecordsBalanceClient {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public long balance(UUID accountId, LocalDate operationDate) {
        final List<Long> result = jdbcTemplate.query(
            """
                WITH latest_snapshot AS (
                    SELECT balance, last_entry_ordinal 
                    FROM entries_snapshots
                    WHERE account_id = :accountId AND operation_date <= :operationDate
                    ORDER BY operation_date DESC, snapshot_ordinal DESC LIMIT 1
                )
                SELECT COALESCE(ls.balance, 0) + COALESCE(SUM(
                    CASE WHEN er.entry_type = 'CREDIT' THEN er.amount ELSE -er.amount END
                ), 0) as actual_balance
                FROM latest_snapshot ls
                FULL OUTER JOIN entry_records er ON (
                    er.account_id = :accountId AND er.operation_date <= :operationDate
                    AND er.entry_ordinal > COALESCE(ls.last_entry_ordinal, 0)
                )
                GROUP BY ls.balance
                """,
            Map.of("accountId", accountId, "operationDate", operationDate),
            (resultSet, index) -> resultSet.getLong(1)
        );
        if (result.isEmpty()) {
            throw new IllegalStateException();
        }
        return result.getFirst();
    }
}
