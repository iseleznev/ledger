package org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.processor;

import java.lang.foreign.MemorySegment;

public interface RingBufferProcessor {

    long processOffset();

    int beforeBatchOperationGap();

    long process(MemorySegment memorySegment, long processOffset, long processSize, long walSequenceId, long stateSequenceId);
}
