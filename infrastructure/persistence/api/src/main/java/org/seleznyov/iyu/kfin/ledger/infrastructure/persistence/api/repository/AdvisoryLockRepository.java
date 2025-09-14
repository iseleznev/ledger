package org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.api.repository;

import java.util.UUID;

public interface AdvisoryLockRepository {

    boolean tryTransactionalLock(UUID accountId);
}
