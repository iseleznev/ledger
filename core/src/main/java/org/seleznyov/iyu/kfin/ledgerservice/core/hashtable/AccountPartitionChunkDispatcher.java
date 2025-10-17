package org.seleznyov.iyu.kfin.ledgerservice.core.hashtable;

import org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties.LedgerConfiguration;

import java.lang.foreign.Arena;
import java.util.UUID;

import static org.seleznyov.iyu.kfin.ledgerservice.core.utils.PartitionUtils.partitionNumber;

public class AccountPartitionChunkDispatcher {

    private final Arena arena;
    private final LedgerConfiguration ledgerConfiguration;
    private final AccountPartitionLazyResizeHashTable[] chunkTables;

    public AccountPartitionChunkDispatcher(
        Arena arena,
        LedgerConfiguration ledgerConfiguration
    ) {
        this.arena = arena;
        this.ledgerConfiguration = ledgerConfiguration;
        this.chunkTables = new AccountPartitionLazyResizeHashTable[
            ledgerConfiguration.accountsPartition().partitionAccountsCount()
                / ledgerConfiguration.accountsPartition().partitionChunkAccountsCount()
            ];

    }

    public void initTables() {
        for (int index = 0; index < chunkTables.length; index++) {
            chunkTables[index] = new AccountPartitionLazyResizeHashTable(
                arena,
                ledgerConfiguration.accountsPartition().partitionChunkAccountsCount(),
                ledgerConfiguration.accountsPartition().maxPsl()
            );
        }
    }

    public int chunkTableIndex(UUID accountId) {
        return partitionNumber(accountId, chunkTables.length);
    }

    public AccountPartitionLazyResizeHashTable chunkTable(UUID accountId) {
        final int chunkIndex = partitionNumber(accountId, chunkTables.length);
        return chunkTables[chunkIndex];
    }
}
