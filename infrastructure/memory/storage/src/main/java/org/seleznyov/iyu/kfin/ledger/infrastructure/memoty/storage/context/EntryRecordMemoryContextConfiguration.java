package org.seleznyov.iyu.kfin.ledger.infrastructure.memoty.storage.context;

import java.util.UUID;

public record EntryRecordMemoryContextConfiguration (
    UUID accountId,
    long bufferEntriesMaxCount,
    long totalAmount
) {

}