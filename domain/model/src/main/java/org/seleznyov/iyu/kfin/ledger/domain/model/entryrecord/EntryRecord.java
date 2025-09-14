package org.seleznyov.iyu.kfin.ledger.domain.model.entryrecord;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

public record EntryRecord(
    UUID id,
    UUID accountId,
    EntryType entryType,
    long amount,
    LocalDateTime createdAt,
    LocalDate operationDay,
    String currencyCode,
    UUID transactionId,
    UUID idempotencyKey
) {

}
