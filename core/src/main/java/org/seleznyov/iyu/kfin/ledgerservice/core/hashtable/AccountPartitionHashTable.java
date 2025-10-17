package org.seleznyov.iyu.kfin.ledgerservice.core.hashtable;

import java.lang.foreign.MemorySegment;
import java.util.UUID;

/**
 * Interface for account partition hash table implementations.
 * Provides offset-based operations on memory-mapped data structures using MemorySegment.
 * All methods work with memory offsets rather than POJOs.
 */
public interface AccountPartitionHashTable {

    MemorySegment memorySegment();

    void commitBalance(long accountOffset);

    /**
     * Copy account state from hash table to target memory segment
     */
    void copyAccountState(MemorySegment targetMemorySegment, long accountOffset, long targetOffset);

    /**
     * Set committed table ordinal sequence
     */
    void committedTableOrdinal(long tableOrdinal);

    /**
     * Set current table ordinal sequence
     */
    void currentTableOrdinal(long tableOrdinal);

    /**
     * Increment and return current table ordinal sequence
     */
    long currentTableOrdinalForward();

    /**
     * Get current table ordinal sequence
     */
    long currentTableOrdinal();

    /**
     * Get committed table ordinal sequence
     */
    long committedTableOrdinal();

    /**
     * Put account with all fields into hash table
     * @return memory offset of the account entry
     */
    long put(UUID key, long balance, long ordinal, long count);

    long put(UUID accountId);

    /**
     * Update account fields at given offset
     */
    void update(long currentOffset, long balance, long ordinal, long count);

    /**
     * Update current ordinal sequence at given offset
     */
    void updateAccountTableCurrentOrdinal(long currentOffset, long currentOrdinalSequence);

    /**
     * Increment and update current ordinal sequence at given offset
     */
    void updateAccountTableCurrentOrdinalForward(long currentOffset);

    /**
     * Update staged amount at given offset
     */
    void updateStagedAmount(long currentOffset, long stagedAmount);

    /**
     * Increase staged amount by delta at given offset
     */
    void increaseStagedAmount(long currentOffset, long increaseStagedAmount);

    long getStagedAmount(long currentOffset);

    /**
     * Update balance at given offset
     */
    void updateBalance(long currentOffset, long balance);

    /**
     * Increase balance by delta at given offset
     */
    void increaseBalance(long currentOffset, long increaseAmount);

    /**
     * Update ordinal at given offset
     */
    void updateOrdinal(long currentOffset, long ordinal);

    /**
     * Update count at given offset
     */
    void updateCount(long currentOffset, long count);

    /**
     * Get memory offset for account key
     * @return memory offset or -1 if not found
     */
    long getOffset(UUID key);

    /**
     * Get balance for account key (with staged amount applied if needed)
     * @return balance or -1 if not found
     */
    long getBalance(UUID key);

    /**
     * Get account rate at given offset
     */
    short getAccountRate(long offset);

    /**
     * Get balance at given offset (with staged amount applied if needed)
     */
    long getBalance(long offset);

    /**
     * Get ordinal for account key
     * @return ordinal or -1 if not found
     */
    long getOrdinal(UUID key);

    /**
     * Get ordinal at given offset
     */
    long getOrdinal(long offset);

    long getAccountTableOrdinal(long offset);

    /**
     * Increment and return ordinal at given offset
     */
    long ordinalForward(long offset);

    /**
     * Get count for account key
     * @return count or -1 if not found
     */
    long getCount(UUID key);

    /**
     * Get count at given offset
     */
    long getCount(long offset);

    /**
     * Increment and return count at given offset
     */
    long countForward(long offset);

    /**
     * Remove account by key
     * @return true if removed, false if not found
     */
    boolean remove(UUID key);

    /**
     * Get current load factor
     */
    double loadFactor();

    /**
     * Get current size (number of entries)
     */
    int size();

    /**
     * Get current capacity
     */
    int capacity();

    /**
     * Get average probe length for statistics
     */
    double averageProbeLength();

    /**
     * Get maximum probe length for statistics
     */
    int maxProbeLength();

    /**
     * Get statistics string
     */
    String getStats();

    /**
     * Check if cleanup is needed
     */
    boolean needsCleanup();

    /**
     * Perform cleanup (remove deleted entries)
     */
    void cleanup();
}
