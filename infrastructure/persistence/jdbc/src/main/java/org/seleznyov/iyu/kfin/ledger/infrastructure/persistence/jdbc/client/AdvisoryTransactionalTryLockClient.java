package org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.jdbc.client;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.seleznyov.iyu.kfin.ledger.shared.utils.Uuid7Utils.uuidToLong;

@Component
@RequiredArgsConstructor
public class AdvisoryTransactionalTryLockClient {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public boolean tryLock(UUID accountId) {
        final long lockKey = uuidToLong(accountId);
        final List<Boolean> result = jdbcTemplate.query(
            "SELECT pg_try_advisory_xact_lock(:lockKey)",
            Map.of("lockKey", lockKey),
            (resultSet, index) -> resultSet.getBoolean(1)
        );
        if (result.isEmpty()) {
            return false;
        }
        return result.getFirst() == true;
    }
}
