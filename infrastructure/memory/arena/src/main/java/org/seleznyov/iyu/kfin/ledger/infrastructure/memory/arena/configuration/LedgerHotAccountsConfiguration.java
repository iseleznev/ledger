package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration;

public record LedgerHotAccountsConfiguration(
    int maxCount,
    int offHeapSizeMb,
    int workerThreadsCount,
    int entryRecordsCountToSnapshot,
    int millisToSnapshot
) {

}
