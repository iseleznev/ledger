package org.seleznyov.iyu.kfin.ledger.domain.model.event;

import lombok.Data;
import lombok.experimental.Accessors;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Event object used in LMAX Disruptor ring buffer for high-performance ledger operations.
 * Contains all necessary data for processing transfers through the disruptor pattern.
 *
 * Design considerations:
 * - Mutable fields for object reuse in ring buffer (performance optimization)
 * - All fields needed for complete transfer processing
 * - Event state tracking for async processing pipeline
 */
@Data
@Accessors(fluent = true)
public class LedgerEvent {

    // Transfer operation data
    private UUID debitAccountId;
    private UUID creditAccountId;
    private long amount;
    private String currencyCode;
    private LocalDate operationDate;
    private UUID transactionId;
    private UUID idempotencyKey;

    // Event metadata
    private EventType eventType;
    private EventStatus status;
    private LocalDateTime createdAt;
    private LocalDateTime processedAt;
    private String errorMessage;
    private long sequenceNumber;

    // Processing results
    private long debitAccountBalanceAfter;
    private long creditAccountBalanceAfter;

    /**
     * Resets event state for object reuse in ring buffer.
     * Called by disruptor framework when event is cleared.
     */
    public void clear() {
        this.debitAccountId = null;
        this.creditAccountId = null;
        this.amount = 0L;
        this.currencyCode = null;
        this.operationDate = null;
        this.transactionId = null;
        this.idempotencyKey = null;
        this.eventType = null;
        this.status = EventStatus.PENDING;
        this.createdAt = null;
        this.processedAt = null;
        this.errorMessage = null;
        this.sequenceNumber = 0L;
        this.debitAccountBalanceAfter = 0L;
        this.creditAccountBalanceAfter = 0L;
    }

    /**
     * Initializes event with transfer data.
     */
    public void initializeTransfer(
        UUID debitAccountId,
        UUID creditAccountId,
        long amount,
        String currencyCode,
        LocalDate operationDate,
        UUID transactionId,
        UUID idempotencyKey
    ) {
        this.debitAccountId = debitAccountId;
        this.creditAccountId = creditAccountId;
        this.amount = amount;
        this.currencyCode = currencyCode;
        this.operationDate = operationDate;
        this.transactionId = transactionId;
        this.idempotencyKey = idempotencyKey;
        this.eventType = EventType.TRANSFER;
        this.status = EventStatus.PENDING;
        this.createdAt = LocalDateTime.now();
    }

    /**
     * Marks event as successfully processed.
     */
    public void markProcessed(long debitBalanceAfter, long creditBalanceAfter) {
        this.status = EventStatus.PROCESSED;
        this.processedAt = LocalDateTime.now();
        this.debitAccountBalanceAfter = debitBalanceAfter;
        this.creditAccountBalanceAfter = creditBalanceAfter;
    }

    /**
     * Marks event as failed with error message.
     */
    public void markFailed(String errorMessage) {
        this.status = EventStatus.FAILED;
        this.processedAt = LocalDateTime.now();
        this.errorMessage = errorMessage;
    }

    /**
     * Event type enumeration.
     */
    public enum EventType {
        TRANSFER,
        BALANCE_INQUIRY,
        SNAPSHOT_TRIGGER
    }

    /**
     * Event processing status.
     */
    public enum EventStatus {
        PENDING,
        PROCESSING,
        PROCESSED,
        FAILED,
        WAL_PERSISTED // Written to Write-Ahead Log
    }
}