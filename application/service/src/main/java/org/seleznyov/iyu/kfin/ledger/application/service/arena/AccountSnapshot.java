package org.seleznyov.iyu.kfin.ledger.application.service.arena;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

// Snapshot structure
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class AccountSnapshot {

    private UUID id;
    private UUID accountId;
    private LocalDate operationDate;
    private long balance;
    private UUID lastEntryRecordId;
    private long lastEntryOrdinal;
    private int operationsCount;
    private long snapshotOrdinal;
    private Instant createdAt;
}
