package org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.jdbc.client;

import lombok.RequiredArgsConstructor;
import org.seleznyov.iyu.kfin.ledger.domain.model.disruptor.WALEntry;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;

@Component
@RequiredArgsConstructor
public class WALEntryUpdateClient {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public void updateStatus(WALEntry walEntry) {
        int rowsUpdated = jdbcTemplate.update("""
                UPDATE ledger.wal_entries 
                SET status = :status,
                    processed_at = :processedAt,
                    error_message = :errorMessage
                WHERE id = :id
                """,
            new MapSqlParameterSource()
                .addValue("id", walEntry.id())
                .addValue("status", walEntry.status().name())
                .addValue("processedAt", walEntry.processedAt() != null ?
                    Timestamp.valueOf(walEntry.processedAt()) : null)
                .addValue("errorMessage", walEntry.errorMessage())
        );

        if (rowsUpdated == 0) {
            throw new IllegalStateException("Failed to update WAL entry: " + walEntry.id());
        }
    }
}