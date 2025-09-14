package org.seleznyov.iyu.kfin.ledger.presentation.rest.dto.request;


import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

import java.time.LocalDate;
import java.util.UUID;

/**
 * Request DTO for balance inquiry operations.
 */
public record BalanceRequestDto(

    @NotNull(message = "Account ID is required")
    @JsonProperty("accountId")
    UUID accountId,

    @JsonProperty("operationDate")
    LocalDate operationDate

) {

    /**
     * Creates balance request for current date.
     */
    public static BalanceRequestDto of(UUID accountId) {
        return new BalanceRequestDto(accountId, LocalDate.now());
    }
}