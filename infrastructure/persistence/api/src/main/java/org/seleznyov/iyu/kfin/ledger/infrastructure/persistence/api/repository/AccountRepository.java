package org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.api.repository;

import java.util.List;
import java.util.UUID;

public interface AccountRepository {

    /**
     * Finds accounts that had activity in the last N days.
     *
     * @param days Number of days to look back
     * @return List of active account IDs
     */
    List<UUID> findAccountsWithRecentActivity(int days);

    /**
     * Finds all accounts that have any entry records.
     *
     * @return List of account IDs with transactions
     */
    List<UUID> findActiveAccounts();
}