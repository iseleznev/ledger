package org.seleznyov.iyu.kfin.ledger.application.service.arena;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

// Entry record structure
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class EntryRecord {

    private UUID id;
    private UUID accountId;
    private UUID transactionId;
    private EntryType entryType;
    private long amount;
    private Instant createdAt;
    private LocalDate operationDate;
    private UUID idempotencyKey;
    private String currencyCode;
    private long entryOrdinal;

    public enum EntryType {
        DEBIT, CREDIT
    }
}
