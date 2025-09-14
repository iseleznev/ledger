package org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.jdbc.client;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@RequiredArgsConstructor
public class WALEntryTotalCountClient {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public long totalEntriesCount() {
        Long count = jdbcTemplate.queryForObject("""
                SELECT COUNT(*) FROM ledger.wal_entries
                """,
            Map.of(),
            Long.class
        );
        return count != null ? count : 0L;
    }
}
