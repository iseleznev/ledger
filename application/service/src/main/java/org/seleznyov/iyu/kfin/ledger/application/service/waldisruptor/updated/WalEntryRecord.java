package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor.updated;

import java.time.LocalDate;
import java.util.UUID;

/**
 * WAL Entry Record for processing
 */
public record WalEntryRecord(
    UUID id,
    long sequenceNumber,
    UUID debitAccountId,
    UUID creditAccountId,
    long amount,
    String currencyCode,
    LocalDate operationDate,
    UUID transactionId,
    UUID idempotencyKey
) {

}
