package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.stamper;

import java.lang.foreign.MemorySegment;

public interface RingBufferStamper {
    void stamp(MemorySegment memorySegment, long stampOffset);

    long stampRecordSize();
}
