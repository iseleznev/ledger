package org.seleznyov.iyu.kfin.ledger.application.service.arenaseparated;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

// Extended interface for partition managers that handle multiple accounts
public interface PartitionAccountManager extends ColdAccountManager {
    /**
     * Check if specific account should create snapshot
     */
    boolean shouldCreateSnapshotForAccount(UUID accountId);

    /**
     * Get snapshot ordinal for specific account
     */
    long getSnapshotOrdinalForAccount(UUID accountId);

    /**
     * Notify that snapshot was created for specific account
     */
    void onSnapshotCreatedForAccount(UUID accountId);

    /**
     * Get current balance for specific account
     */
    long getCurrentBalanceForAccount(UUID accountId);

    /**
     * Get partition ID
     */
    int getPartitionId();

    /**
     * Cleanup inactive accounts
     */
    int cleanupInactiveAccounts(Duration inactiveThreshold);

    /**
     * Get partition statistics
     */
    Map<String, Object> getPartitionStatistics();
}