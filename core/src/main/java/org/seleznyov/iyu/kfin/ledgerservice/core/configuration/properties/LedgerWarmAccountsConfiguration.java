package org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties;

public record LedgerWarmAccountsConfiguration(
    int maxLeastRecentlyUsedAccountsCount,
    int leastRecentlyUsedCacheTimeMillis,
    int accountsPerShard,
    int workerThreadsCount,
    int entryRecordsCountToSnapshot,
    int millisToSnapshot
) {

}
