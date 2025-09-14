package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor.updated;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.util.UUID;

/**
 * Input validation for ledger operations
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LedgerOperationValidationRequest {

    @NotNull(message = "Debit account ID cannot be null")
    private UUID debitAccountId;

    @NotNull(message = "Credit account ID cannot be null")
    private UUID creditAccountId;

    @Positive(message = "Amount must be positive")
    private long amount;

    @NotBlank(message = "Currency code cannot be blank")
    @Size(min = 3, max = 3, message = "Currency code must be exactly 3 characters")
    private String currencyCode;

    @NotNull(message = "Operation date cannot be null")
    private LocalDate operationDate;

    @NotNull(message = "Transaction ID cannot be null")
    private UUID transactionId;

    @NotNull(message = "Idempotency key cannot be null")
    private UUID idempotencyKey;

    public void validateBusinessRules() {
        if (debitAccountId.equals(creditAccountId)) {
            throw new IllegalArgumentException("Debit and credit accounts cannot be the same");
        }

        if (operationDate.isAfter(LocalDate.now())) {
            throw new IllegalArgumentException("Operation date cannot be in the future");
        }

        if (amount > 999_999_999_00L) {
            throw new IllegalArgumentException("Amount exceeds maximum allowed value");
        }
    }
}
