package org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties;

public record LedgerConfiguration(
    int accountsPartitionsCount,
    WalConfiguration wal,
    SnapshotConfiguration snapshot,
    PostgreSQLConfiguration postgres,
    RingBufferConfiguration ringBuffer,
    LedgerHotAccountsConfiguration hotAccounts,
    LedgerWarmAccountsConfiguration warmAccounts,
    LedgerEntriesConfiguration entries,
    PartitionActorConfiguration partitionActor,
    AccountPartitionConfiguration accountsPartition
) {

}
