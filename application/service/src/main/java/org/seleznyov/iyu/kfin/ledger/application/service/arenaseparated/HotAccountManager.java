package org.seleznyov.iyu.kfin.ledger.application.service.arenaseparated;

// Hot Account Interface - Synchronous for maximum performance
public interface HotAccountManager extends BaseAccountManager {

    /**
     * Add entry synchronously - maximum performance for high RPS accounts
     *
     * @param entry Entry to add
     * @return true if entry was successfully added to batch
     */
    boolean addEntry(EntryRecord entry);

    /**
     * Check if current batch is ready for processing
     */
    boolean isBatchReady();

    /**
     * Extract current batch for ring buffer processing
     */
    BatchData extractBatch();

    /**
     * Notify that batch was successfully queued to ring buffer
     */
    void onBatchQueued(int entriesInBatch);
}
