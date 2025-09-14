package org.seleznyov.iyu.kfin.ledger.domain.model.snapshot;

import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

public record EntriesSnapshot(
    UUID id,
    UUID accountId,
    LocalDate operationDay,
    long balance,
    UUID lastEntryRecordId,
    long lastEntryOrdinal,
    int operationsCount,
    EntriesSnapshotReasonType reason,
    Instant createdAt,
    long durationMillis
) {
}
