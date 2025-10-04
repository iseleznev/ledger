package org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.processor;

import java.lang.foreign.MemorySegment;

public interface PipelineRingBufferProcessor {

    long processOffset();

    int beforeBatchOperationGap();

    long process(MemorySegment memorySegment, long processOffset, long expectedProcessSize, long walSequenceId, long stateSequenceId);
}
