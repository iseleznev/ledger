package org.seleznyov.iyu.kfin.ledger.application.service.containeroptimized;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

/**
 * Result of ledger operation processing
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LedgerOperationResult {

    private boolean success;
    private long sequenceNumber;
    private UUID transactionId;
    private long processingTimeNanos;
    private String errorMessage;
    private String errorCode;

    // Balance information after operation
    private Long debitAccountBalanceAfter;
    private Long creditAccountBalanceAfter;

    // WAL information
    private Long walSequenceNumber;

    // Timing information
    private long walWriteTimeNanos;
    private long balanceUpdateTimeNanos;

    public static LedgerOperationResult success(UUID transactionId, long sequenceNumber) {
        return LedgerOperationResult.builder()
            .success(true)
            .transactionId(transactionId)
            .sequenceNumber(sequenceNumber)
            .processingTimeNanos(System.nanoTime())
            .build();
    }

    public static LedgerOperationResult failure(String errorMessage, String errorCode) {
        return LedgerOperationResult.builder()
            .success(false)
            .errorMessage(errorMessage)
            .errorCode(errorCode)
            .build();
    }

    public boolean isFailure() {
        return !success;
    }

    public double getProcessingTimeMs() {
        return processingTimeNanos / 1_000_000.0;
    }
}