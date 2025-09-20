package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration;

public record LedgerWarmAccountsConfiguration(
    int maxLeastRecentlyUsedAccountsCount,
    int leastRecentlyUsedCacheTimeMillis,
    int accountsPerShard,
    int workerThreadsCount,
    int entryRecordsCountToSnapshot,
    int millisToSnapshot
) {

}
