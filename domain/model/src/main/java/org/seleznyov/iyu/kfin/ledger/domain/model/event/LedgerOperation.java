package org.seleznyov.iyu.kfin.ledger.domain.model.event;

import org.seleznyov.iyu.kfin.ledger.domain.model.entryrecord.EntryRecord;

public record LedgerOperation(
    LedgerOperationType ledgerOperationType,
    EntryRecord entryRecord

) {

}
