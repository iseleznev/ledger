package org.seleznyov.iyu.kfin.ledger.presentation.rest.dto.response;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Response DTO for balance inquiry operations.
 */
public record BalanceResponseDto(

    @JsonProperty("accountId")
    UUID accountId,

    @JsonProperty("balance")
    Long balance,

    @JsonProperty("operationDate")
    LocalDate operationDate,

    @JsonProperty("success")
    boolean success,

    @JsonProperty("message")
    String message,

    @JsonProperty("failureReason")
    String failureReason,

    @JsonProperty("processingStrategy")
    String processingStrategy,

    @JsonProperty("queryTimeMs")
    Long queryTimeMs,

    @JsonProperty("timestamp")
    LocalDateTime timestamp

) {

    /**
     * Creates successful balance response.
     */
    public static BalanceResponseDto success(
        UUID accountId,
        Long balance,
        LocalDate operationDate,
        String processingStrategy,
    long queryTimeMs
    ) {
        return new BalanceResponseDto(
            accountId,
            balance,
            operationDate,
            true,
            "Balance retrieved successfully",
            null,
            processingStrategy,
            queryTimeMs,
            LocalDateTime.now()
        );
    }

    /**
     * Creates failed balance response.
     */
    public static BalanceResponseDto failure(
        UUID accountId,
        LocalDate operationDate,
        String failureReason,
        String message,
    long queryTimeMs
    ) {
        return new BalanceResponseDto(
            accountId,
            null,
            operationDate,
            false,
            message,
            failureReason,
            null,
            queryTimeMs,
            LocalDateTime.now()
        );
    }
}