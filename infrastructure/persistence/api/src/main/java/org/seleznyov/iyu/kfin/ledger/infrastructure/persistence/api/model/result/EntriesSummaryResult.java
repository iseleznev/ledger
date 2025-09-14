package org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.api.model.result;

import java.util.UUID;

public record EntriesSummaryResult(
    UUID accountId,
    Long balance,
    UUID lastEntryRecordId,
    Long lastEntryOrdinal,
    Integer operationsCount
) {

    public static EntriesSummaryResult of(
        UUID accountId,
        Long balance,
        UUID lastEntryRecordId,
        Long lastEntryOrdinal,
        Integer operationsCount
    ) {
        return new EntriesSummaryResult(accountId, balance, lastEntryRecordId, lastEntryOrdinal, operationsCount);
    }
}
