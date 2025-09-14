package org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.api.repository;

import org.seleznyov.iyu.kfin.ledger.domain.model.snapshot.EntriesSnapshot;

import java.time.LocalDate;
import java.util.Optional;
import java.util.UUID;

/**
 * Repository interface for entries snapshot operations.
 * Provides methods for creating and retrieving account balance snapshots.
 */
public interface EntriesSnapshotRepository {

    /**
     * Inserts new snapshot into persistent storage.
     *
     * @param entriesSnapshot Snapshot to insert
     */
    void insert(EntriesSnapshot entriesSnapshot);

    /**
     * Finds the most recent snapshot for account on or before specified date.
     * Used for balance calculation optimization.
     *
     * @param accountId Account identifier
     * @param operationDate Target date (finds latest snapshot on or before this date)
     * @return Latest snapshot or empty if none found
     */
    Optional<EntriesSnapshot> findLatestSnapshot(UUID accountId, LocalDate operationDate);

    /**
     * Finds exact snapshot for account on specific date.
     * Used to check if snapshot already exists before creation.
     *
     * @param accountId Account identifier
     * @param operationDate Exact date to match
     * @return Snapshot if exists, empty otherwise
     */
    Optional<EntriesSnapshot> findExactSnapshot(UUID accountId, LocalDate operationDate);

    /**
     * Legacy method for backward compatibility.
     * @deprecated Use findExactSnapshot() for better error handling
     */
    @Deprecated
    EntriesSnapshot lastSnapshot(UUID accountId, LocalDate operationDate);
}