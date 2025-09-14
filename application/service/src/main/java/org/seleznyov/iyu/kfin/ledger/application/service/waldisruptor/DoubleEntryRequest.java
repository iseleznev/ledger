package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor;

import java.time.LocalDate;
import java.util.UUID;

/**
 * Request record for double-entry operations
 */
public record DoubleEntryRequest(
    UUID debitAccountId,
    UUID creditAccountId,
    long amount,
    String currencyCode,
    LocalDate operationDate,
    UUID transactionId,
    UUID idempotencyKey
) {

    // Convenience constructor with defaults
    public DoubleEntryRequest(UUID debitAccountId, UUID creditAccountId, long amount) {
        this(debitAccountId, creditAccountId, amount, "RUB", LocalDate.now(),
            UUID.randomUUID(), UUID.randomUUID());
    }
}
