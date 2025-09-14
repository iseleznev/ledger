package org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.jdbc.client;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
public class WALEntryDeleteOldClient {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public int deleteOldEntries(int olderThanDays) {
        // Вычисляем cutoff дату в Java - безопаснее и яснее
        LocalDateTime cutoffDate = LocalDateTime.now().minusDays(olderThanDays);

        return jdbcTemplate.update("""
                DELETE FROM ledger.wal_entries
                WHERE created_at < :cutoffDate
                  AND status IN ('PROCESSED', 'FAILED')
                """,
            new MapSqlParameterSource()
                .addValue("cutoffDate", Timestamp.valueOf(cutoffDate))
        );
    }
}