package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration;

public record LedgerEntriesConfiguration(
    int threadsStripeSize,
    int batchEntriesCount,
    boolean halfBatchBarrier,
    int entryRecordsCountToSnapshot,
    int millisToSnapshot,
    int workerBoundedQueueSize
) {

}
