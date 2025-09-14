package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler;

import java.lang.foreign.MemorySegment;

public interface BatchRingBufferHandler {

    long batchChecksum(long batchOffset);

    MemorySegment ringBufferSegment();

    long batchSize(long batchOffset);

    long batchElementsCount(long batchOffset);
}
