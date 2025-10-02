package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.prevprocessor;

import java.lang.foreign.MemorySegment;

public interface RingBufferProcessor {

    long processOffset();

    void processOffset(long offset);

    int beforeBatchOperationGap();

//    long process(RingBufferHandler ringBufferHandler, long batchSlotOffset, long batchRawSize);
//
    long process(MemorySegment memorySegment, long processOffset, long expectedProcessSize);

    long processBatch(MemorySegment memorySegment, long processOffset);
}
