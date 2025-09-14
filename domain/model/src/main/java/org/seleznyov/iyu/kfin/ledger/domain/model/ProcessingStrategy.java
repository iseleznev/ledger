package org.seleznyov.iyu.kfin.ledger.domain.model;

/**
 * Processing strategy enumeration for ledger operations.
 * Determines which implementation approach to use based on account characteristics.
 */
public enum ProcessingStrategy {

    /**
     * Simple processing strategy for cold accounts.
     *
     * Characteristics:
     * - Uses PostgreSQL advisory locks
     * - Synchronous processing with immediate persistence
     * - Optimized for correctness and simplicity
     * - Target performance: ~1000 operations/second
     * - Suitable for low-volume accounts
     */
    SIMPLE,

    /**
     * High-performance processing strategy for hot accounts.
     *
     * Characteristics:
     * - Uses LMAX Disruptor for lock-free processing
     * - Account affinity (single thread per account)
     * - Write-Ahead Log with asynchronous persistence
     * - Optimized for throughput and low latency
     * - Target performance: ~10,000+ operations/second
     * - Suitable for high-volume accounts
     */
    DISRUPTOR
}