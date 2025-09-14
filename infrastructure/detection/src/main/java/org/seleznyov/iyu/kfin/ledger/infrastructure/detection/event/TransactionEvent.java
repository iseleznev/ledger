package org.seleznyov.iyu.kfin.ledger.infrastructure.detection.event;

import java.time.LocalDateTime;
import java.util.UUID;

public record TransactionEvent(
    UUID accountId,
    long amount,
    boolean isDebit,
    LocalDateTime timestamp
) {

    public static TransactionEvent of(UUID accountId, long amount, boolean isDebit) {
        return new TransactionEvent(accountId, amount, isDebit, LocalDateTime.now());
    }
}