package org.seleznyov.iyu.kfin.ledger.presentation.rest.dto.response;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Response DTO for transfer operations.
 * Contains operation result and metadata.
 */
public record TransferResponseDto(

    @JsonProperty("transactionId")
    UUID transactionId,

    @JsonProperty("idempotencyKey")
    UUID idempotencyKey,

    @JsonProperty("success")
    boolean success,

    @JsonProperty("message")
    String message,

    @JsonProperty("failureReason")
    String failureReason,

    @JsonProperty("processingStrategy")
    String processingStrategy,

    @JsonProperty("processingTimeMs")
    Long processingTimeMs,

    @JsonProperty("timestamp")
    LocalDateTime timestamp

) {

    /**
     * Creates successful transfer response.
     */
    public static TransferResponseDto success(
        UUID transactionId,
        UUID idempotencyKey,
        String message,
        String processingStrategy,
    long processingTimeMs
    ) {
        return new TransferResponseDto(
            transactionId,
            idempotencyKey,
            true,
            message,
            null,
            processingStrategy,
            processingTimeMs,
            LocalDateTime.now()
        );
    }

    /**
     * Creates failed transfer response.
     */
    public static TransferResponseDto failure(
        UUID transactionId,
        UUID idempotencyKey,
        String failureReason,
        String message,
    long processingTimeMs
    ) {
        return new TransferResponseDto(
            transactionId,
            idempotencyKey,
            false,
            message,
            failureReason,
            null,
            processingTimeMs,
            LocalDateTime.now()
        );
    }
}