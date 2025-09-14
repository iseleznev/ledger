package org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.jdbc.client;

import lombok.RequiredArgsConstructor;
import org.seleznyov.iyu.kfin.ledger.domain.model.disruptor.WALEntry;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class WALEntryFindByIdClient {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public Optional<WALEntry> findById(UUID entryId) {
        List<WALEntry> result = jdbcTemplate.query("""
                SELECT id, sequence_number, debit_account_id, credit_account_id, amount,
                       currency_code, operation_date, transaction_id, idempotency_key,
                       status, created_at, processed_at, error_message
                FROM ledger.wal_entries
                WHERE id = :entryId
                """,
            Map.of("entryId", entryId),
            (rs, rowNum) -> new WALEntry(
                UUID.fromString(rs.getString("id")),
                rs.getLong("sequence_number"),
                UUID.fromString(rs.getString("debit_account_id")),
                UUID.fromString(rs.getString("credit_account_id")),
                rs.getLong("amount"),
                rs.getString("currency_code"),
                rs.getDate("operation_date").toLocalDate(),
                UUID.fromString(rs.getString("transaction_id")),
                UUID.fromString(rs.getString("idempotency_key")),
                WALEntry.WALStatus.valueOf(rs.getString("status")),
                rs.getTimestamp("created_at").toLocalDateTime(),
                rs.getTimestamp("processed_at") != null ?
                    rs.getTimestamp("processed_at").toLocalDateTime() : null,
                rs.getString("error_message")
            )
        );

        return result.isEmpty() ? Optional.empty() : Optional.of(result.get(0));
    }
}