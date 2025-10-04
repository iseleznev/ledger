package org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties;

public record LedgerHotAccountsConfiguration(
    int maxCount,
    int offHeapSizeMb,
    int workerThreadsCount,
    int entryRecordsCountToSnapshot,
    int millisToSnapshot
) {

}
