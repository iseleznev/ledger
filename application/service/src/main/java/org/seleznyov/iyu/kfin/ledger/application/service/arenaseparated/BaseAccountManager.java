package org.seleznyov.iyu.kfin.ledger.application.service.arenaseparated;

import java.util.UUID;

// Base interface with common operations
public interface BaseAccountManager {
    /**
     * Get current account balance including pending entries
     */
    long getCurrentBalance(AccountSnapshot lastSnapshot);

    /**
     * Check if snapshot should be created
     */
    boolean shouldCreateSnapshot();

    /**
     * Get ordinal where snapshot should be taken
     */
    long getSnapshotOrdinal();

    /**
     * Notify manager that snapshot was successfully created
     */
    void onSnapshotCreated();

    /**
     * Cleanup resources
     */
    void cleanup();

    /**
     * Get account ID
     */
    UUID getAccountId();
}