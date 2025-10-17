package org.seleznyov.iyu.kfin.ledgerservice.core.actor;

import org.seleznyov.iyu.kfin.ledgerservice.core.hashtable.AccountPartitionLazyResizeHashTable;
import org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.handler.SWSRRingBufferHandler;

public class StateRingBufferStampHelper {

    public void stamp(
        AccountPartitionLazyResizeHashTable partitionHashTable,
        SWSRRingBufferHandler ringBufferHandler,
        long partitionHashTableOffset,
        long stampOffset
    ) {
        partitionHashTable.copyAccountState(
            ringBufferHandler.memorySegment(),
            partitionHashTableOffset,
            stampOffset
        );
    }
}
