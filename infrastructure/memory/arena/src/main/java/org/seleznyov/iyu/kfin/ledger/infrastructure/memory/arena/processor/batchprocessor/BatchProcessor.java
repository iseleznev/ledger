package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.processor.batchprocessor;

import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.BatchRingBufferHandler;

@FunctionalInterface
public interface BatchProcessor {

    boolean processBatch(BatchRingBufferHandler ringBufferHandler, long batchSlotOffset, long batchRawSize);
}
