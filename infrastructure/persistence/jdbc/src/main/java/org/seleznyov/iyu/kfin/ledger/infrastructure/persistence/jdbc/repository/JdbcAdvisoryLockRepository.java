package org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.jdbc.repository;

import lombok.RequiredArgsConstructor;
import org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.api.repository.AdvisoryLockRepository;
import org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.jdbc.client.AdvisoryTransactionalTryLockClient;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class JdbcAdvisoryLockRepository implements AdvisoryLockRepository {

    private final AdvisoryTransactionalTryLockClient client;

    @Override
    public boolean tryTransactionalLock(UUID accountId) {
        return client.tryLock(accountId);
    }
}
