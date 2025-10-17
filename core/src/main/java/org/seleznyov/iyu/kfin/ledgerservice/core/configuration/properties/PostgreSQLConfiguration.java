package org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties;

public record PostgreSQLConfiguration(
    int workerThreadsCount,
    int stageTablesPerWorkerThreadCount,
    int copyThreadsCount,
    int batchRecordsCount,
    long waitingEmptyRingBufferNanos,
    int emptyIterationsYieldRetries,
    String url,
    String databaseName,
    String username,
    String password,
    String host
) {

}
