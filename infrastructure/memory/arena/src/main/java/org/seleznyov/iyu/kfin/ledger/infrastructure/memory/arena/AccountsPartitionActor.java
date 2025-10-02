package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration.LedgerConfiguration;

import java.util.UUID;

public class AccountsPartitionActor {

    private final AccountsPartitionHashTable accountsPartitionHashTable;
    private final LedgerConfiguration ledgerConfiguration;
    private final AccountsPartitionActorTransferProcessor transferProcessor;
    private final AccountsPartitionActorCommitProcessor commitProcessor;
    private final AccountsPartitionActorPartitionAmountProcessor partitionAmountProcessor;

    public AccountsPartitionActor(
        AccountsPartitionHashTable accountsPartitionHashTable,
        AccountsPartitionActorTransferProcessor transferProcessor,
        AccountsPartitionActorCommitProcessor commitProcessor,
        AccountsPartitionActorPartitionAmountProcessor partitionAmountProcessor,
        LedgerConfiguration ledgerConfiguration
    ) {
        this.transferProcessor = transferProcessor;
        this.commitProcessor = commitProcessor;
        this.partitionAmountProcessor = partitionAmountProcessor;
        this.accountsPartitionHashTable = accountsPartitionHashTable;
        this.ledgerConfiguration = ledgerConfiguration;
    }

    public void processActor(long actorPartitionId) {
        transferProcessor.processTransfers(accountsPartitionHashTable, ledgerConfiguration, actorPartitionId);

        commitProcessor.processCommits(accountsPartitionHashTable, ledgerConfiguration, actorPartitionId);

        partitionAmountProcessor.processAmounts(accountsPartitionHashTable, ledgerConfiguration, actorPartitionId);
    }

    private UUID uuid(long msb, long lsb) {
        return new UUID(msb, lsb);
    }
}
