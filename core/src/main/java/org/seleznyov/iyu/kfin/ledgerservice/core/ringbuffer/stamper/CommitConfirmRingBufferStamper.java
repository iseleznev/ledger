package org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.stamper;

import java.lang.foreign.MemorySegment;

import static org.seleznyov.iyu.kfin.ledgerservice.core.constants.CommitConfirmOffsetConstants.*;
import static org.seleznyov.iyu.kfin.ledgerservice.core.constants.EntryRecordOffsetConstants.POSTGRES_ENTRY_RECORD_SIZE;

public class CommitConfirmRingBufferStamper implements RingBufferStamper {

    @Override
    public void stamp(MemorySegment memorySegment, long stampOffset, long availableSize, long walSequenceId, long stateSequenceId) {
        if (availableSize < POSTGRES_ENTRY_RECORD_SIZE) {
            throw new IllegalArgumentException("Not enough space in ring buffer to stamp entry record");
        }
        memorySegment.set(
            COMMIT_CONFIRM_PARTITION_HASH_TABLE_COMMIT_ORDINAL_TYPE,
            stampOffset + COMMIT_CONFIRM_PARTITION_HASH_TABLE_COMMIT_ORDINAL_OFFSET,
            stateSequenceId
        );
    }

    @Override
    public long stampRecordSize() {
        return COMMIT_CONFIRM_SIZE;
    }
}
