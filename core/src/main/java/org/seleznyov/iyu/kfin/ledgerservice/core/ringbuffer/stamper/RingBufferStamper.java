package org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.stamper;

import java.lang.foreign.MemorySegment;

public interface RingBufferStamper {
    void stamp(MemorySegment memorySegment, long stampOffset);

    long stampRecordSize();
}
