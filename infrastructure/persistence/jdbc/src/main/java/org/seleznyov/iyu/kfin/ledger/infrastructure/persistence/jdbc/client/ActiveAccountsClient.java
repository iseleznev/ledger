package org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.jdbc.client;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class ActiveAccountsClient {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public List<UUID> findAccountsWithRecentActivity(int days) {
        LocalDate cutoffDate = LocalDate.now().minusDays(days);

        return jdbcTemplate.query("""
                SELECT DISTINCT account_id
                FROM ledger.entry_records
                WHERE operation_date >= :cutoffDate
                ORDER BY account_id
                """,
            Map.of("cutoffDate", cutoffDate),
            (rs, rowNum) -> UUID.fromString(rs.getString("account_id"))
        );
    }

    public List<UUID> findActiveAccounts() {
        return jdbcTemplate.query("""
                SELECT DISTINCT account_id
                FROM ledger.entry_records
                ORDER BY account_id
                """,
            Map.of(),
            (rs, rowNum) -> UUID.fromString(rs.getString("account_id"))
        );
    }
}