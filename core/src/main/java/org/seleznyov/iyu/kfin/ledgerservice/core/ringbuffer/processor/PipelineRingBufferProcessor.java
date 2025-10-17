package org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.processor;

import java.lang.foreign.MemorySegment;

public interface PipelineRingBufferProcessor {

    int beforeBatchOperationGap();

    long pipelineProcessOffset();

    long process(
        MemorySegment memorySegment,
        long arenaSize,
        long stampOffset,
        int processSize,
        long walSequenceId,
        long stateSequenceId
    );
}
