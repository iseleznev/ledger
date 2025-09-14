package org.seleznyov.iyu.kfin.ledger.application.service.containeroptimized;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * WAL Entry entity for Write-Ahead Log persistence
 * Maps to ledger.wal_entries table
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WalEntry {

    private UUID id;
    private long sequenceNumber;
    private UUID debitAccountId;
    private UUID creditAccountId;
    private long amount; // Amount in minor currency units (cents)
    private String currencyCode;
    private LocalDate operationDate;
    private UUID transactionId;
    private UUID idempotencyKey;
    private WalStatus status;
    private OffsetDateTime createdAt;
    private OffsetDateTime processedAt;
    private String errorMessage;

    public enum WalStatus {
        PENDING,     // Just written to WAL
        PROCESSING,  // Being processed
        PROCESSED,   // Successfully processed
        FAILED,      // Processing failed
        RECOVERED    // Recovered during startup
    }

    /**
     * Create new WAL entry for double-entry transaction
     */
    public static WalEntry create(UUID debitAccountId, UUID creditAccountId, long amount,
                                  String currencyCode, LocalDate operationDate, UUID transactionId,
                                  UUID idempotencyKey, long sequenceNumber) {
        return WalEntry.builder()
            .sequenceNumber(sequenceNumber)
            .debitAccountId(debitAccountId)
            .creditAccountId(creditAccountId)
            .amount(amount)
            .currencyCode(currencyCode)
            .operationDate(operationDate)
            .transactionId(transactionId)
            .idempotencyKey(idempotencyKey)
            .status(WalStatus.PENDING)
            .createdAt(OffsetDateTime.now())
            .build();
    }

    /**
     * Create new version with status change (IMMUTABLE approach)
     */
    public WalEntry createNewVersion(WalStatus newStatus, String errorMessage) {
        return WalEntry.builder()
            .sequenceNumber(this.sequenceNumber)
            .debitAccountId(this.debitAccountId)
            .creditAccountId(this.creditAccountId)
            .amount(this.amount)
            .currencyCode(this.currencyCode)
            .operationDate(this.operationDate)
            .transactionId(this.transactionId)
            .idempotencyKey(this.idempotencyKey)
            .status(newStatus)
            .createdAt(OffsetDateTime.now())
            .processedAt(WalStatus.PROCESSED.equals(newStatus) || WalStatus.FAILED.equals(newStatus) || WalStatus.RECOVERED.equals(newStatus)
                ? OffsetDateTime.now() : null)
            .errorMessage(errorMessage)
            .build();
    }

    /**
     * Mark current version as non-current (used before inserting new version)
     */
    public WalEntry markAsNonCurrent() {
        return WalEntry.builder()
            .id(this.id)
            .sequenceNumber(this.sequenceNumber)
            .debitAccountId(this.debitAccountId)
            .creditAccountId(this.creditAccountId)
            .amount(this.amount)
            .currencyCode(this.currencyCode)
            .operationDate(this.operationDate)
            .transactionId(this.transactionId)
            .idempotencyKey(this.idempotencyKey)
            .status(this.status)
            .createdAt(this.createdAt)
            .processedAt(this.processedAt)
            .errorMessage(this.errorMessage)
            .build();
    }

    /**
     * Check if entry is pending processing
     */
    public boolean isPending() {
        return WalStatus.PENDING.equals(status);
    }

    /**
     * Check if entry is being processed
     */
    public boolean isProcessing() {
        return WalStatus.PROCESSING.equals(status);
    }

    /**
     * Check if entry was successfully processed
     */
    public boolean isProcessed() {
        return WalStatus.PROCESSED.equals(status);
    }

    /**
     * Check if entry processing failed
     */
    public boolean isFailed() {
        return WalStatus.FAILED.equals(status);
    }

    /**
     * Check if entry was recovered
     */
    public boolean isRecovered() {
        return WalStatus.RECOVERED.equals(status);
    }

    /**
     * Check if entry needs processing (pending or failed)
     */
    public boolean needsProcessing() {
        return isPending() || isFailed();
    }

    /**
     * Get processing duration in milliseconds
     */
    public Long getProcessingDurationMs() {
        if (processedAt == null || createdAt == null) {
            return null;
        }
        return java.time.Duration.between(createdAt, processedAt).toMillis();
    }

    /**
     * Check if entry is stale (created but not processed for too long)
     */
    public boolean isStale(int maxAgeMinutes) {
        if (isProcessed() || isRecovered()) {
            return false;
        }

        OffsetDateTime threshold = OffsetDateTime.now().minusMinutes(maxAgeMinutes);
        return createdAt.isBefore(threshold);
    }

    /**
     * Validate WAL entry
     */
    public boolean isValid() {
        return debitAccountId != null
            && creditAccountId != null
            && !debitAccountId.equals(creditAccountId)
            && amount > 0
            && currencyCode != null && currencyCode.length() == 3
            && operationDate != null
            && transactionId != null
            && idempotencyKey != null
            && status != null
            && sequenceNumber > 0;
    }

    /**
     * Get retry count from error message (simplified)
     */
    public int getRetryCount() {
        if (errorMessage == null) {
            return 0;
        }
        // Simple retry count extraction - in real system would be more sophisticated
        return (int) errorMessage.chars().filter(ch -> ch == '|').count();
    }

    /**
     * Check if should retry based on retry count
     */
    public boolean shouldRetry(int maxRetries) {
        return isFailed() && getRetryCount() < maxRetries;
    }
}