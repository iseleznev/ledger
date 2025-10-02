package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration;

public record RingBufferConfiguration(
    CommitRingBufferConfiguration commits,
    WalRingBufferConfiguration wal,
    PostgresRingBufferConfiguration postgres,
    AccountsFileTableRingBufferConfiguration accountsFileTable,
    TransferRequestRingBufferConfiguration transferRequests,
    PartitionAmountRingBufferConfiguration partitionAmounts,
    InterPartitionAmountRingBufferConfiguration interPartitions,
    ActorTransferRingBufferConfiguration actorTransfers
) {

}
