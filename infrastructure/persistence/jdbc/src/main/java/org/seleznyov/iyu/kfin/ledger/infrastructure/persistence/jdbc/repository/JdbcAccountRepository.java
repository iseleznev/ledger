package org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.jdbc.repository;

import lombok.RequiredArgsConstructor;
import org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.api.repository.AccountRepository;
import org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.jdbc.client.ActiveAccountsClient;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class JdbcAccountRepository implements AccountRepository {

    private final ActiveAccountsClient activeAccountsClient;

    @Override
    public List<UUID> findAccountsWithRecentActivity(int days) {
        return activeAccountsClient.findAccountsWithRecentActivity(days);
    }

    @Override
    public List<UUID> findActiveAccounts() {
        return activeAccountsClient.findActiveAccounts();
    }
}