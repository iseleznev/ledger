package org.seleznyov.iyu.kfin.ledger.domain.model.disruptor;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Write-Ahead Log entry for ensuring durability in hot account processing.
 * WAL entries are written before processing and used for crash recovery.
 *
 * Design principles:
 * - Immutable record for consistency
 * - Contains all data needed for operation replay
 * - Sequence-based ordering for recovery
 * - Status tracking for persistence lifecycle
 */
public record WALEntry(
    UUID id,
    long sequenceNumber,
    UUID debitAccountId,
    UUID creditAccountId,
    long amount,
    String currencyCode,
    LocalDate operationDate,
    UUID transactionId,
    UUID idempotencyKey,
    WALStatus status,
    LocalDateTime createdAt,
    LocalDateTime processedAt,
    String errorMessage
) {

    /**
     * Creates new WAL entry for transfer operation.
     */
    public static WALEntry forTransfer(
        long sequenceNumber,
        UUID debitAccountId,
        UUID creditAccountId,
        long amount,
        String currencyCode,
        LocalDate operationDate,
        UUID transactionId,
        UUID idempotencyKey
    ) {
        return new WALEntry(
            UUID.randomUUID(),
            sequenceNumber,
            debitAccountId,
            creditAccountId,
            amount,
            currencyCode,
            operationDate,
            transactionId,
            idempotencyKey,
            WALStatus.PENDING,
            LocalDateTime.now(),
            null,
            null
        );
    }

    /**
     * Marks entry as processed successfully.
     */
    public WALEntry markProcessed() {
        return new WALEntry(
            id, sequenceNumber, debitAccountId, creditAccountId,
            amount, currencyCode, operationDate, transactionId,
            idempotencyKey, WALStatus.PROCESSED, createdAt,
            LocalDateTime.now(), errorMessage
        );
    }

    /**
     * Marks entry as failed with error message.
     */
    public WALEntry markFailed(String error) {
        return new WALEntry(
            id, sequenceNumber, debitAccountId, creditAccountId,
            amount, currencyCode, operationDate, transactionId,
            idempotencyKey, WALStatus.FAILED, createdAt,
            LocalDateTime.now(), error
        );
    }

    /**
     * WAL entry status enumeration.
     */
    public enum WALStatus {
        PENDING,      // Written to WAL, awaiting processing
        PROCESSING,   // Currently being processed
        PROCESSED,    // Successfully processed and persisted to DB
        FAILED,       // Processing failed
        RECOVERED     // Recovered from WAL during startup
    }
}