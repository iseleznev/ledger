package org.seleznyov.iyu.kfin.ledger.presentation.rest.dto.request;

import jakarta.validation.constraints.*;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDate;
import java.util.UUID;

/**
 * Request DTO for transfer operations.
 * Contains validation constraints for business rules.
 */
public record TransferRequestDto(

    @NotNull(message = "Debit account ID is required")
    @JsonProperty("debitAccountId")
    UUID debitAccountId,

    @NotNull(message = "Credit account ID is required")
    @JsonProperty("creditAccountId")
    UUID creditAccountId,

    @NotNull(message = "Amount is required")
    @Positive(message = "Amount must be positive")
    @Max(value = 1_000_000_00L, message = "Amount exceeds maximum limit")
    @JsonProperty("amount")
    Long amount,

    @NotBlank(message = "Currency code is required")
    @Pattern(regexp = "[A-Z]{3}", message = "Currency code must be 3 uppercase letters")
    @JsonProperty("currencyCode")
    String currencyCode,

    @JsonProperty("operationDate")
    LocalDate operationDate,

    @JsonProperty("idempotencyKey")
    UUID idempotencyKey

) {

    /**
     * Creates transfer request with current date and generated idempotency key.
     */
    public static TransferRequestDto of(
        UUID debitAccountId,
        UUID creditAccountId,
        Long amount,
        String currencyCode
    ) {
        return new TransferRequestDto(
            debitAccountId,
            creditAccountId,
            amount,
            currencyCode,
            LocalDate.now(),
            UUID.randomUUID()
        );
    }

    /**
     * Validation: debit and credit accounts must be different.
     */
    public boolean isValid() {
        return debitAccountId != null && creditAccountId != null &&
            !debitAccountId.equals(creditAccountId);
    }
}