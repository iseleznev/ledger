package org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.processor;

import org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.handler.SWSRRingBufferHandler;
import org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.stamper.MemoryCopyRingBufferStamper;

import java.lang.foreign.MemorySegment;

public class SWSRStripeEventLoopProcessor implements RingBufferProcessor {

    private static final int BEFORE_BATCH_OPERATION_GAP = 0;

    private final SWSRRingBufferHandler[] ringBufferHandlers;
    private final MemoryCopyRingBufferStamper memoryCopyStamper;
    private final int writeAttempts;
    int stripeIndex = 0;

    public SWSRStripeEventLoopProcessor(
        SWSRRingBufferHandler[] ringBufferHandlers,
        MemoryCopyRingBufferStamper memoryCopyStamper,
        int writeAttempts
    ) {
        this.ringBufferHandlers = ringBufferHandlers;
        this.memoryCopyStamper = memoryCopyStamper;
        this.writeAttempts = writeAttempts;
    }

    @Override
    public long processOffset() {
        throw new UnsupportedOperationException("Not supported for WAL-process.");
    }

    @Override
    public int beforeBatchOperationGap() {
        return BEFORE_BATCH_OPERATION_GAP;
    }

    @Override
    public long process(MemorySegment memorySegment, long processOffset, long processSize, long walSequenceId, long stateSequenceId) {
        final SWSRRingBufferHandler stripeRingBufferHandler = ringBufferHandlers[stripeIndex];
        memoryCopyStamper.sourceOffset(processOffset);
        final boolean stamped = stripeRingBufferHandler.tryStampForward(
            memoryCopyStamper,
            walSequenceId,
            stateSequenceId,
            writeAttempts
        );
        stripeIndex++;
        if (stripeIndex >= ringBufferHandlers.length) {
            stripeIndex = 0;
        }
        if (!stamped) {
            return -1;
        }
        return 0;
    }
}
