package org.seleznyov.iyu.kfin.ledgerservice.core.model;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

public record EntryRecord(
    UUID id,
    UUID accountId,
    EntryType entryType,
    UUID transactionId,
    long amount,
    LocalDateTime createdAt,
    LocalDate operationDay,
    String currencyCode,
    UUID idempotencyKey,
    long ordinal
) {

}
