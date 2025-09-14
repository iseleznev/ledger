package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor.updatedv8;

/**
 * Durability levels configuration
 */
public enum DurabilityLevel {
    IMMEDIATE_FSYNC(true, true),     // Strongest durability, slower
    BATCH_FSYNC(true, false),        // Good durability, better performance
    ASYNC_FSYNC(false, false),       // Balanced
    OS_CACHE_ONLY(false, false);     // Fastest, least durable

    private final boolean requiresImmediate;
    private final boolean requiresReplication;

    DurabilityLevel(boolean requiresImmediate, boolean requiresReplication) {
        this.requiresImmediate = requiresImmediate;
        this.requiresReplication = requiresReplication;
    }

    public boolean requiresImmediate() {
        return requiresImmediate;
    }

    public boolean requiresReplication() {
        return requiresReplication;
    }
}
