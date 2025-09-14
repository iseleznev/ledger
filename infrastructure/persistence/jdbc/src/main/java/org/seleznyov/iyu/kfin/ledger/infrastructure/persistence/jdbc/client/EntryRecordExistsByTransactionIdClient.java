package org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.jdbc.client;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class EntryRecordExistsByTransactionIdClient {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public boolean existsByTransactionId(UUID transactionId) {
        Integer count = jdbcTemplate.queryForObject("""
                SELECT COUNT(*) FROM ledger.entry_records
                WHERE transaction_id = :transactionId
                """,
            Map.of("transactionId", transactionId),
            Integer.class
        );
        return count != null && count > 0;
    }
}