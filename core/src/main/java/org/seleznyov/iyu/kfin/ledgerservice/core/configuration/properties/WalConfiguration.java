package org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties;

public record WalConfiguration (
    String path,
    int writeFileBufferSize,
    int alignmentSize,
    long maxFileSizeMb,
    String walFilePrefix,
    String checkpointFilePrefix,
    long batchEntriesCount,
    long minWalFlushIntervalMs,
    long maxWalFlushIntervalMs,
    int workerThreadsCount,
    long waitingEmptyRingBufferNanos,
    int emptyIterationsYieldRetries
) {

}
