package org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties;

public record LedgerEntriesConfiguration(
    int threadsStripeSize,
    int batchEntriesCount,
    boolean halfBatchBarrier,
    int entryRecordsCountToSnapshot,
    int millisToSnapshot,
    int workerBoundedQueueSize
) {

}
