package org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.api.repository;

import org.seleznyov.iyu.kfin.ledger.domain.model.disruptor.WALEntry;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Repository interface for Write-Ahead Log persistence operations.
 * Provides CRUD operations for WAL entries used in hot account processing.
 */
public interface WALEntryRepository {

    /**
     * Inserts new WAL entry into persistent storage.
     *
     * @param entry WAL entry to insert
     */
    void insert(WALEntry entry);

    /**
     * Updates WAL entry status after processing.
     *
     * @param entry Updated WAL entry with new status
     */
    void updateStatus(WALEntry entry);

    /**
     * Finds all WAL entries that are still pending processing.
     * Used during system recovery after crashes.
     *
     * @return List of pending WAL entries ordered by sequence number
     */
    List<WALEntry> findPendingEntries();

    /**
     * Deletes old WAL entries to prevent storage growth.
     *
     * @param olderThanDays Remove entries older than this many days
     * @return Number of entries deleted
     */
    int deleteOldEntries(int olderThanDays);

    /**
     * Gets total count of WAL entries in storage.
     *
     * @return Total count of entries
     */
    long getTotalEntries();

    /**
     * Gets count of pending WAL entries.
     *
     * @return Count of pending entries
     */
    long getPendingCount();

    /**
     * Finds WAL entry by ID for force recovery operations.
     *
     * @param entryId WAL entry ID
     * @return WAL entry if found
     */
    Optional<WALEntry> findById(UUID entryId);
}