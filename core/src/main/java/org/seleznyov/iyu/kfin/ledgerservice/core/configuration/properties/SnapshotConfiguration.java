package org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties;

public record SnapshotConfiguration(
    String path,
    int writeFileBufferSize,
    int alignmentSize,
    long maxStateFileSizeMb,
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
