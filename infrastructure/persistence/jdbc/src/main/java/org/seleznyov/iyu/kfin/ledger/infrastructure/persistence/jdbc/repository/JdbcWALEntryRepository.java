package org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.jdbc.repository;

import org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.api.repository.WALEntryRepository;
import org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.jdbc.client.*;
import org.springframework.stereotype.Repository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.domain.model.disruptor.WALEntry;
import org.springframework.dao.DataAccessException;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * JDBC implementation of WAL repository for Write-Ahead Log persistence.
 * Provides durable storage for WAL entries used in hot account processing.
 *
 * Key features:
 * - Transactional consistency for WAL operations
 * - Optimized queries for recovery scenarios
 * - Bulk cleanup operations for maintenance
 * - Comprehensive error handling with specific exceptions
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class JdbcWALEntryRepository implements WALEntryRepository {

    private final WALEntryInsertClient insertClient;
    private final WALEntryUpdateClient updateClient;
    private final WALEntryClient queryClient;
    private final WALEntryPendingCountClient pendingCountClient;
    private final WALEntryTotalCountClient totalCountClient;
    private final WALEntryDeleteOldClient deleteClient;
    private final WALEntryFindByIdClient findByIdClient;  // Новый клиент

    @Override
    @Transactional
    public void insert(WALEntry entry) {
        log.debug("Inserting WAL entry: id={}, sequence={}, tx={}",
            entry.id(), entry.sequenceNumber(), entry.transactionId());

        try {
            insertClient.insert(entry);

            log.debug("WAL entry inserted successfully: id={}, sequence={}",
                entry.id(), entry.sequenceNumber());

        } catch (DataAccessException e) {
            log.error("Failed to insert WAL entry: id={}, sequence={}, error={}",
                entry.id(), entry.sequenceNumber(), e.getMessage(), e);
            throw new WALPersistenceException(
                "Failed to insert WAL entry: " + entry.id(), e
            );
        } catch (Exception e) {
            log.error("Unexpected error inserting WAL entry: id={}, sequence={}, error={}",
                entry.id(), entry.sequenceNumber(), e.getMessage(), e);
            throw new WALPersistenceException(
                "Unexpected error inserting WAL entry: " + entry.id(), e
            );
        }
    }

    @Override
    @Transactional
    public void updateStatus(WALEntry entry) {
        log.debug("Updating WAL entry status: id={}, status={}, tx={}",
            entry.id(), entry.status(), entry.transactionId());

        try {
            updateClient.updateStatus(entry);

            log.debug("WAL entry status updated successfully: id={}, status={}",
                entry.id(), entry.status());

        } catch (DataAccessException e) {
            log.error("Failed to update WAL entry status: id={}, status={}, error={}",
                entry.id(), entry.status(), e.getMessage(), e);
            throw new WALPersistenceException(
                "Failed to update WAL entry status: " + entry.id(), e
            );
        } catch (Exception e) {
            log.error("Unexpected error updating WAL entry status: id={}, status={}, error={}",
                entry.id(), entry.status(), e.getMessage(), e);
            throw new WALPersistenceException(
                "Unexpected error updating WAL entry status: " + entry.id(), e
            );
        }
    }

    @Override
    @Transactional(readOnly = true)
    public List<WALEntry> findPendingEntries() {
        log.debug("Finding pending WAL entries for recovery");

        try {
            List<WALEntry> pendingEntries = queryClient.findPendingEntries();

            log.info("Found {} pending WAL entries for recovery", pendingEntries.size());
            return pendingEntries;

        } catch (DataAccessException e) {
            log.error("Failed to find pending WAL entries: {}", e.getMessage(), e);
            throw new WALPersistenceException(
                "Failed to find pending WAL entries for recovery", e
            );
        } catch (Exception e) {
            log.error("Unexpected error finding pending WAL entries: {}", e.getMessage(), e);
            throw new WALPersistenceException(
                "Unexpected error finding pending WAL entries", e
            );
        }
    }

    @Override
    @Transactional
    public int deleteOldEntries(int olderThanDays) {
        log.info("Deleting WAL entries older than {} days", olderThanDays);

        if (olderThanDays < 1) {
            throw new IllegalArgumentException("olderThanDays must be at least 1");
        }

        try {
            int deletedCount = deleteClient.deleteOldEntries(olderThanDays);

            log.info("Deleted {} old WAL entries (older than {} days)",
                deletedCount, olderThanDays);
            return deletedCount;

        } catch (DataAccessException e) {
            log.error("Failed to delete old WAL entries: olderThanDays={}, error={}",
                olderThanDays, e.getMessage(), e);
            throw new WALPersistenceException(
                "Failed to delete old WAL entries", e
            );
        } catch (Exception e) {
            log.error("Unexpected error deleting old WAL entries: olderThanDays={}, error={}",
                olderThanDays, e.getMessage(), e);
            throw new WALPersistenceException(
                "Unexpected error deleting old WAL entries", e
            );
        }
    }

    @Override
    @Transactional(readOnly = true)
    public long getTotalEntries() {
        log.debug("Getting total WAL entries count");

        try {
            long totalCount = totalCountClient.totalEntriesCount();

            log.debug("Total WAL entries count: {}", totalCount);
            return totalCount;

        } catch (DataAccessException e) {
            log.error("Failed to get total WAL entries count: {}", e.getMessage(), e);
            throw new WALPersistenceException(
                "Failed to get total WAL entries count", e
            );
        } catch (Exception e) {
            log.error("Unexpected error getting total WAL entries count: {}", e.getMessage(), e);
            throw new WALPersistenceException(
                "Unexpected error getting total WAL entries count", e
            );
        }
    }

    @Override
    @Transactional(readOnly = true)
    public long getPendingCount() {
        log.debug("Getting pending WAL entries count");

        try {
            long pendingCount = pendingCountClient.pendingCount();

            log.debug("Pending WAL entries count: {}", pendingCount);
            return pendingCount;

        } catch (DataAccessException e) {
            log.error("Failed to get pending WAL entries count: {}", e.getMessage(), e);
            throw new WALPersistenceException(
                "Failed to get pending WAL entries count", e
            );
        } catch (Exception e) {
            log.error("Unexpected error getting pending WAL entries count: {}", e.getMessage(), e);
            throw new WALPersistenceException(
                "Unexpected error getting pending WAL entries count", e
            );
        }
    }

    /**
     * Exception thrown when WAL persistence operations fail.
     */
    public static class WALPersistenceException extends RuntimeException {
        public WALPersistenceException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<WALEntry> findById(UUID entryId) {
        log.debug("Finding WAL entry by ID: {}", entryId);

        try {
            Optional<WALEntry> entry = findByIdClient.findById(entryId);

            if (entry.isPresent()) {
                log.debug("Found WAL entry: id={}, tx={}", entryId, entry.get().transactionId());
            } else {
                log.debug("WAL entry not found: id={}", entryId);
            }

            return entry;

        } catch (DataAccessException e) {
            log.error("Failed to find WAL entry by ID: id={}, error={}", entryId, e.getMessage(), e);
            throw new WALPersistenceException("Failed to find WAL entry by ID: " + entryId, e);
        }
    }

}