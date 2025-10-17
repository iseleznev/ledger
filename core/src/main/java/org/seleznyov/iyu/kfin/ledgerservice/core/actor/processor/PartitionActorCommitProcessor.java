package org.seleznyov.iyu.kfin.ledgerservice.core.actor.processor;

import org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties.LedgerConfiguration;
import org.seleznyov.iyu.kfin.ledgerservice.core.hashtable.AccountPartitionHashTable;
import org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.handler.SimpleSWSRRingBufferHandler;

import static org.seleznyov.iyu.kfin.ledgerservice.core.constants.CommitConfirmOffsetConstants.COMMIT_CONFIRM_PARTITION_HASH_TABLE_COMMIT_ORDINAL_OFFSET;
import static org.seleznyov.iyu.kfin.ledgerservice.core.constants.CommitConfirmOffsetConstants.COMMIT_CONFIRM_PARTITION_HASH_TABLE_COMMIT_ORDINAL_TYPE;
import static org.seleznyov.iyu.kfin.ledgerservice.core.constants.EntryRecordOffsetConstants.POSTGRES_ENTRY_RECORD_SIZE;

public class PartitionActorCommitProcessor {

    private final SimpleSWSRRingBufferHandler commitRingBufferHandler;

    public PartitionActorCommitProcessor(
        SimpleSWSRRingBufferHandler commitRingBufferHandler
    ) {
        this.commitRingBufferHandler = commitRingBufferHandler;
    }

    public void processCommits(AccountPartitionHashTable accountsPartitionHashTable, LedgerConfiguration ledgerConfiguration, long actorPartitionNumber) {
        final long commitProcessedSize = commitRingBufferHandler.readyToProcess(ledgerConfiguration.partitionActor().commitsCountQuota() * POSTGRES_ENTRY_RECORD_SIZE, POSTGRES_ENTRY_RECORD_SIZE);
        if (commitProcessedSize >= 0) {
            long commitProcessOffset = commitRingBufferHandler.processOffset();
            for (long offset = commitRingBufferHandler.processOffset(); offset < commitProcessOffset + commitProcessedSize; offset += POSTGRES_ENTRY_RECORD_SIZE) {
                final long commitTableOrdinal = commitRingBufferHandler.memorySegment().get(COMMIT_CONFIRM_PARTITION_HASH_TABLE_COMMIT_ORDINAL_TYPE, offset + COMMIT_CONFIRM_PARTITION_HASH_TABLE_COMMIT_ORDINAL_OFFSET);
                accountsPartitionHashTable.committedTableOrdinal(commitTableOrdinal);
            }
            commitRingBufferHandler.processedForward(commitProcessedSize);
        }
    }
}
