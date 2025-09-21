package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration;

public record PostgreSQLConfiguration(
    int workerThreadsCount,
    int stageTablesPerWorkerThreadCount,
    int ringBufferCopyBatchesCountCapacity,
    int directCopyBatchRecordsCount,
    long waitingEmptyRingBufferNanos,
    int emptyIterationsYieldRetries,
    String url,
    String databaseName,
    String username,
    String password,
    String host
) {

}
