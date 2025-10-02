package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration;

public record LedgerConfiguration(
    int accountsPartitionsCount,
    WalConfiguration wal,
    PostgreSQLConfiguration postgres,
    RingBufferConfiguration ringBuffer,
    LedgerHotAccountsConfiguration hotAccounts,
    LedgerWarmAccountsConfiguration warmAccounts,
    LedgerEntriesConfiguration entries,
    PartitionActorConfiguration partitionActor
) {

}
