package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler;

import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.prevprocessor.RingBufferProcessor;

import java.lang.foreign.MemorySegment;

public interface RingBufferHandler {

    long tryProcessBatch(RingBufferProcessor ringBufferProcessor, long currentReadOffset, long maxBatchSlotSize);

    long batchChecksum(long batchOffset, long batchSize);

    MemorySegment memorySegment();

    long batchElementsCount(long batchOffset);
}
