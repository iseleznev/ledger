package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration;

public record WalConfiguration (
    String path,
    int writeFileBufferSize,
    int alignmentSize,
    long maxFileSizeMb,
    String walFilePrefix,
    String checkpointFilePrefix,
    long batchEntriesCount,
    long ringBufferBatchesCount,
    long minWalFlushIntervalMs,
    long maxWalFlushIntervalMs,
    int workerThreadsCount
) {

}
