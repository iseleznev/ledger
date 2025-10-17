package org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.stamper;

import org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.handler.SWSRRingBufferHandler;

import java.lang.foreign.MemorySegment;

public class MemoryCopyRingBufferStamper implements RingBufferStamper {

    private final MemorySegment sourceMemorySegment;
    private long sourceOffset;


    public MemoryCopyRingBufferStamper(SWSRRingBufferHandler transactionRingBuffer) {
        this.sourceMemorySegment = transactionRingBuffer.memorySegment();
    }

    public void sourceOffset(long offset) {
        this.sourceOffset = offset;
    }

    @Override
    public void stamp(MemorySegment memorySegment, long stampOffset, long availableSize, long walSequenceId, long stateSequenceId) {
        MemorySegment.copy(
            sourceMemorySegment,
            sourceOffset,
            memorySegment,
            stampOffset,
            availableSize
        );
    }

    @Override
    public long stampRecordSize() {
        return 0;
    }
}
