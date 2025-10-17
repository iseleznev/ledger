package org.seleznyov.iyu.kfin.ledgerservice.core.snapshot;

import org.seleznyov.iyu.kfin.ledgerservice.core.constants.SnapshotOffsetConstants;
import org.seleznyov.iyu.kfin.ledgerservice.core.hashtable.AccountPartitionHashTable;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.UUID;

public class SnapshotWriter {

    private final AccountPartitionHashTable accountPartitionHashTable;
    private final SnapshotWalWriter snapshotWalWriter;
    private final SnapshotIndexWriter snapshotIndexWriter;

    public SnapshotWriter(
        AccountPartitionHashTable accountPartitionHashTable,
        SnapshotWalWriter snapshotWalWriter,
        SnapshotIndexWriter snapshotIndexWriter
    ) {
        this.accountPartitionHashTable = accountPartitionHashTable;
        this.snapshotWalWriter = snapshotWalWriter;
        this.snapshotIndexWriter = snapshotIndexWriter;
    }

    public void write(
        MemorySegment memorySegment,
        long processOffset,
        long stateSequenceId,
        long walSequenceId
    ) {
        final UUID accountId = new UUID(
            memorySegment.get(ValueLayout.JAVA_LONG, processOffset + SnapshotOffsetConstants.SNAPSHOT_ACCOUNT_ID_MSB_OFFSET),
            memorySegment.get(ValueLayout.JAVA_LONG, processOffset + SnapshotOffsetConstants.SNAPSHOT_ACCOUNT_ID_LSB_OFFSET)
        );
        long accountOffset = accountPartitionHashTable.getOffset(accountId);
        if (accountOffset == -1) {
            accountOffset = accountPartitionHashTable.put(accountId);
        }
        snapshotWalWriter.write(memorySegment, stateSequenceId, walSequenceId);;
        snapshotIndexWriter.write(accountPartitionHashTable.memorySegment(), walSequenceId, accountOffset);
    }
}
