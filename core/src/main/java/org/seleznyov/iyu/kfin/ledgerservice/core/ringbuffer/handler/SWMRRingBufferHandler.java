package org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.handler;

import org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties.LedgerConfiguration;
import org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.processor.RingBufferProcessor;
import org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.stamper.RingBufferStamper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

public class SWMRRingBufferHandler {

    static final Logger log = LoggerFactory.getLogger(SWMRRingBufferHandler.class);

    private static final VarHandle STAMP_OFFSET_VAR_HANDLE;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            STAMP_OFFSET_VAR_HANDLE = lookup.findVarHandle(
                SWMRRingBufferHandler.class, "stampOffset", long.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final MemorySegment memorySegment;
    private final long arenaHalfSize;

    private long stampOffset;
    private final RingBufferProcessor[] processors;
    private final LedgerConfiguration ledgerConfiguration;

    //    private long offset;
    private long arenaSize;

    public SWMRRingBufferHandler(
        List<RingBufferProcessor> processors,
        LedgerConfiguration ledgerConfiguration,
        MemorySegment memorySegment
    ) {
        this.processors = new RingBufferProcessor[processors.size()];
        for (int i = 0; i < processors.size(); i++) {
            final RingBufferProcessor reader = processors.get(i);
            this.processors[i] = reader;
        }
        this.memorySegment = memorySegment;
        this.arenaSize = memorySegment.byteSize();
        this.arenaHalfSize = arenaSize >> 1;
        this.ledgerConfiguration = ledgerConfiguration;

        this.stampOffset = 0;
    }

    public MemorySegment memorySegment() {
        return memorySegment;
    }

    public long readyToProcess(long currentProcessOffset, long expectedProcessSize) {
        long currentStampOffset = (long) STAMP_OFFSET_VAR_HANDLE.getAcquire(this);

        final long batchSize = Math.min(
            Math.min(
                expectedProcessSize,
                currentProcessOffset >= this.arenaHalfSize
                    ? this.arenaSize - currentProcessOffset
                    : this.arenaHalfSize - currentProcessOffset
            ),
            currentStampOffset >= currentProcessOffset
                ? currentStampOffset - currentProcessOffset
                : expectedProcessSize
        );
        // Проверяем есть ли данные
        if (currentProcessOffset >= currentStampOffset && !(currentProcessOffset >= this.arenaHalfSize && currentStampOffset < this.arenaHalfSize)) {
            return 0;
        }

        return batchSize;
    }

    public boolean tryStampForward(RingBufferStamper stamper, int maxAttempts) {
        int spinAttempts = ledgerConfiguration.ringBuffer().processing().writeSpinAttempts();
        int yieldAttempts = ledgerConfiguration.ringBuffer().processing().writeYieldAttempts();
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            // Single writer reading own field - plain read OK
            long currentStampOffset = stampOffset;
            long nextStampOffset = (currentStampOffset + stamper.stampRecordSize()) % this.arenaSize;

            // Find slowest reader
            // IMPORTANT: processors[i].processOffset() MUST use volatile or VarHandle.getVolatile()
            // to ensure writer sees reader updates
            long minimalProcessOffset = this.arenaSize;
            for (int i = 0; i < processors.length; i++) {
                final long processOffset = processors[i].processOffset() - processors[i].beforeBatchOperationGap();
                if (minimalProcessOffset > processOffset) {
                    minimalProcessOffset = processOffset;
                }
            }

            if (currentStampOffset > minimalProcessOffset - stamper.stampRecordSize()) {
                if (attempt < spinAttempts) {
                    Thread.onSpinWait();
                } else if (attempt < spinAttempts + yieldAttempts) {
                    Thread.yield();
                } else {
                    final int parkAttempt = attempt - spinAttempts - yieldAttempts;
                    final long parkNanos = 1_000L << Math.min(parkAttempt, 16);
                    LockSupport.parkNanos(parkNanos);
                }
                continue;
            }

            try {
                // Write data to MemorySegment
                stamper.stamp(memorySegment, currentStampOffset);

                // Publish offset to readers with release semantics
                STAMP_OFFSET_VAR_HANDLE.setRelease(this, nextStampOffset);

                if (currentStampOffset < this.arenaHalfSize && nextStampOffset >= this.arenaHalfSize) {
                    return true;
                }
                if (currentStampOffset > this.arenaHalfSize && nextStampOffset < this.arenaHalfSize) {
                    return true;
                }
                return false;
            } catch (Exception exception) {
                throw new RuntimeException("Failed to publish batch", exception);
            }
        }
        throw new IllegalStateException("Failed to publish batch after " + maxAttempts + " attempts");
    }

    public void offsetForward(long batchRawSize) {
        final long nextOffset = stampOffset + batchRawSize < arenaSize
            ? stampOffset + batchRawSize
            : 0;
        STAMP_OFFSET_VAR_HANDLE.setRelease(this, nextOffset);
    }

    public boolean offsetBarrierPassed(long batchRawSize) {
        return stampOffset == 0 || (stampOffset + batchRawSize) == (arenaSize >> 1);
    }

    public void resetStampOffset() {
        STAMP_OFFSET_VAR_HANDLE.setRelease(this, 0L);
    }
}
