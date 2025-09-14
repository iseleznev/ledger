package org.seleznyov.iyu.kfin.ledger.infrastructure.disruptor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.domain.model.disruptor.WALEntry;
import org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.api.repository.WALEntryRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Write-Ahead Log service for ensuring durability in hot account processing.
 * Provides ACID guarantees by logging operations before execution and
 * enabling recovery after crashes.
 *
 * Key features:
 * - Sequential write ordering with sequence numbers
 * - In-memory tracking of pending WAL entries
 * - Recovery support for crash scenarios
 * - Async persistence marking after DB write
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class WALService {

    private final WALEntryRepository walEntryRepository;

    // In-memory tracking of WAL entries for fast access
    private final ConcurrentHashMap<Long, WALEntry> pendingEntries = new ConcurrentHashMap<>();
    private final AtomicLong walEntriesWritten = new AtomicLong(0);

    /**
     * Writes transfer operation to WAL before processing.
     * This ensures durability - if system crashes, operation can be recovered.
     *
     * @param sequenceNumber Disruptor sequence number
     * @param debitAccountId Source account
     * @param creditAccountId Target account
     * @param amount Transfer amount
     * @param currencyCode Currency code
     * @param operationDate Operation date
     * @param transactionId Transaction identifier
     * @param idempotencyKey Idempotency key
     * @return WAL entry for tracking
     */
    public WALEntry writeTransfer(
        long sequenceNumber,
        UUID debitAccountId,
        UUID creditAccountId,
        long amount,
        String currencyCode,
        LocalDate operationDate,
        UUID transactionId,
        UUID idempotencyKey
    ) {
        log.debug("Writing transfer to WAL: tx={}, sequence={}", transactionId, sequenceNumber);

        try {
            // Create WAL entry
            WALEntry entry = WALEntry.forTransfer(
                sequenceNumber,
                debitAccountId,
                creditAccountId,
                amount,
                currencyCode,
                operationDate,
                transactionId,
                idempotencyKey
            );

            // Write to persistent WAL storage
            walEntryRepository.insert(entry);

            // Track in memory for fast access
            pendingEntries.put(sequenceNumber, entry);
            walEntriesWritten.incrementAndGet();

            log.debug("WAL entry written successfully: tx={}, sequence={}", transactionId, sequenceNumber);
            return entry;

        } catch (Exception e) {
            log.error("Failed to write WAL entry: tx={}, sequence={}, error={}",
                transactionId, sequenceNumber, e.getMessage(), e);
            throw new WALWriteException("Failed to write WAL entry", e);
        }
    }

    /**
     * Marks WAL entry as successfully persisted to database.
     * This allows WAL cleanup and indicates operation is fully durable.
     *
     * @param sequenceNumber Sequence number of completed operation
     */
    public void markPersisted(long sequenceNumber) {
        WALEntry entry = pendingEntries.get(sequenceNumber);
        if (entry == null) {
            log.warn("Attempted to mark unknown WAL entry as persisted: sequence={}", sequenceNumber);
            return;
        }

        try {
            // Update WAL entry status
            WALEntry updatedEntry = entry.markProcessed();
            walEntryRepository.updateStatus(updatedEntry);

            // Remove from pending tracking
            pendingEntries.remove(sequenceNumber);

            log.debug("WAL entry marked as persisted: tx={}, sequence={}",
                entry.transactionId(), sequenceNumber);

        } catch (Exception e) {
            log.error("Failed to mark WAL entry as persisted: sequence={}, error={}",
                sequenceNumber, e.getMessage(), e);
            // Keep in pending for retry or cleanup
        }
    }

    /**
     * Marks WAL entry as failed.
     *
     * @param sequenceNumber Sequence number of failed operation
     * @param errorMessage Error description
     */
    public void markFailed(long sequenceNumber, String errorMessage) {
        WALEntry entry = pendingEntries.get(sequenceNumber);
        if (entry == null) {
            log.warn("Attempted to mark unknown WAL entry as failed: sequence={}", sequenceNumber);
            return;
        }

        try {
            WALEntry failedEntry = entry.markFailed(errorMessage);
            walEntryRepository.updateStatus(failedEntry);
            pendingEntries.remove(sequenceNumber);

            log.warn("WAL entry marked as failed: tx={}, sequence={}, error={}",
                entry.transactionId(), sequenceNumber, errorMessage);

        } catch (Exception e) {
            log.error("Failed to mark WAL entry as failed: sequence={}, error={}",
                sequenceNumber, e.getMessage(), e);
        }
    }

    /**
     * Recovers unprocessed WAL entries after system restart.
     * Returns entries that need to be replayed.
     *
     * @return List of WAL entries requiring recovery
     */
    public java.util.List<WALEntry> recoverPendingEntries() {
        log.info("Recovering pending WAL entries from persistent storage");

        try {
            var pendingEntries = walEntryRepository.findPendingEntries();

            log.info("Found {} pending WAL entries for recovery", pendingEntries.size());

            // Load into memory tracking
            for (WALEntry entry : pendingEntries) {
                this.pendingEntries.put(entry.sequenceNumber(), entry);
            }

            return pendingEntries;

        } catch (Exception e) {
            log.error("Failed to recover WAL entries: {}", e.getMessage(), e);
            throw new WALRecoveryException("Failed to recover WAL entries", e);
        }
    }

    /**
     * Cleans up old WAL entries that are no longer needed.
     * Should be called periodically to prevent WAL growth.
     *
     * @param olderThanDays Remove entries older than this many days
     * @return Number of entries cleaned up
     */
    public int cleanupOldEntries(int olderThanDays) {
        log.info("Cleaning up WAL entries older than {} days", olderThanDays);

        try {
            int cleanedCount = walEntryRepository.deleteOldEntries(olderThanDays);

            log.info("Cleaned up {} old WAL entries", cleanedCount);
            return cleanedCount;

        } catch (Exception e) {
            log.error("Failed to cleanup old WAL entries: {}", e.getMessage(), e);
            return 0;
        }
    }

    /**
     * Gets WAL statistics for monitoring.
     */
    public WALStats getWALStats() {
        return new WALStats(
            walEntriesWritten.get(),
            pendingEntries.size(),
            walEntryRepository.getTotalEntries(),
            walEntryRepository.getPendingCount()
        );
    }

    /**
     * Exception thrown when WAL write fails.
     */
    public static class WALWriteException extends RuntimeException {
        public WALWriteException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Exception thrown when WAL recovery fails.
     */
    public static class WALRecoveryException extends RuntimeException {
        public WALRecoveryException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * WAL statistics for monitoring.
     */
    public record WALStats(
        long totalEntriesWritten,
        long pendingInMemory,
        long totalInStorage,
        long pendingInStorage
    ) {}
}