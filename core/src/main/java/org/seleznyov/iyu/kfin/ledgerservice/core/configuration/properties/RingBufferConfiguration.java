package org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties;

public record RingBufferConfiguration(
    CommitRingBufferConfiguration commits,
    ProcessingRingBufferConfiguration processing,
    TransferRequestRingBufferConfiguration transferRequests,
    PartitionAmountRingBufferConfiguration partitionAmounts,
    InterPartitionAmountRingBufferConfiguration interPartitions,
    ActorTransferRingBufferConfiguration actorTransfers,
    SnapshotRingBufferConfiguration snapshot,
    WalRingBufferConfiguration wal
) {

}
