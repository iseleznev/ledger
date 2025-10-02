package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.ringbuffer.MWSRRingBufferHandler;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.stamper.RingBufferStamper;

import java.lang.foreign.MemorySegment;

public class TransferEventLoopStamper implements RingBufferStamper {

    private final MemorySegment transferMemorySegment;
    private long transferOffset;

    public TransferEventLoopStamper(MWSRRingBufferHandler transferRingBufferHandler) {
        this.transferMemorySegment = transferRingBufferHandler.memorySegment();
    }

    public void transferOffset(long transferOffset) {
        this.transferOffset = transferOffset;
    }

    @Override
    public void stamp(MemorySegment memorySegment, long stampOffset) {
//        VarHandle.acquireFence();
        MemorySegment.copy(
            transferMemorySegment,
            transferOffset,
            memorySegment,
            stampOffset,
            TransferRequestOffsetConstants.TRANSFER_STAMP_SIZE
        );
//        transferOffset += TransferRequestOffsetConstants.TRANSFER_STAMP_SIZE;
    }

    @Override
    public long stampRecordSize() {
        return TransferRequestOffsetConstants.TRANSFER_STAMP_SIZE;
    }
}
