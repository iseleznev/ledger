package org.seleznyov.iyu.kfin.ledger.application.service.containeroptimized;

/**
 * Durability levels for ledger operations
 * Defines how strict the persistence requirements are
 */
public enum DurabilityLevel {

    /**
     * Memory only - fastest, no durability guarantees
     * Used for temporary calculations or high-frequency updates
     */
    MEMORY_ONLY,

    /**
     * WAL only - fast with durability
     * Operation is written to WAL but not immediately flushed to disk
     * Good balance of performance and safety
     */
    WAL_BUFFERED,

    /**
     * WAL with immediate flush - slower but guaranteed persistence
     * Operation is written to WAL and immediately flushed to disk
     * Used for critical operations
     */
    WAL_SYNC,

    /**
     * Full persistence - slowest, maximum durability
     * Operation is written to WAL, flushed, and persisted to main tables
     * Used for regulatory compliance or audit requirements
     */
    FULL_SYNC;

    public boolean requiresWAL() {
        return this != MEMORY_ONLY;
    }

    public boolean requiresSync() {
        return this == WAL_SYNC || this == FULL_SYNC;
    }

    public boolean requiresFullPersistence() {
        return this == FULL_SYNC;
    }

    /**
     * Get default durability level based on operation context
     */
    public static DurabilityLevel getDefault() {
        return WAL_BUFFERED;
    }
}