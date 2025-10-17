package org.seleznyov.iyu.kfin.ledgerservice.core.actor;

import org.seleznyov.iyu.kfin.ledgerservice.core.actor.processor.PartitionActorCommitProcessor;
import org.seleznyov.iyu.kfin.ledgerservice.core.actor.processor.PartitionActorPartitionAmountProcessor;
import org.seleznyov.iyu.kfin.ledgerservice.core.actor.processor.PartitionActorTransferProcessor;
import org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties.LedgerConfiguration;
import org.seleznyov.iyu.kfin.ledgerservice.core.hashtable.AccountPartitionHashTable;

import java.util.UUID;

public class PartitionActor {
    private final AccountPartitionHashTable accountsPartitionHashTable;
    private final LedgerConfiguration ledgerConfiguration;
    private final PartitionActorTransferProcessor transferProcessor;
    private final PartitionActorCommitProcessor commitProcessor;
    private final PartitionActorPartitionAmountProcessor partitionAmountProcessor;

    public PartitionActor(
        AccountPartitionHashTable accountsPartitionHashTable,
        PartitionActorTransferProcessor transferProcessor,
        PartitionActorCommitProcessor commitProcessor,
        PartitionActorPartitionAmountProcessor partitionAmountProcessor,
        LedgerConfiguration ledgerConfiguration
    ) {
        this.transferProcessor = transferProcessor;
        this.commitProcessor = commitProcessor;
        this.partitionAmountProcessor = partitionAmountProcessor;
        this.accountsPartitionHashTable = accountsPartitionHashTable;
        this.ledgerConfiguration = ledgerConfiguration;
    }

    public void processActor(long actorPartitionId) {
        transferProcessor.processTransfers(accountsPartitionHashTable, ledgerConfiguration);

        commitProcessor.processCommits(accountsPartitionHashTable, ledgerConfiguration, actorPartitionId);

        partitionAmountProcessor.processAmounts(accountsPartitionHashTable, ledgerConfiguration, actorPartitionId);
    }

    private UUID uuid(long msb, long lsb) {
        return new UUID(msb, lsb);
    }
}
