package org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.eventloop;

import org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.handler.MWSRRingBufferHandler;
import org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.processor.RingBufferProcessor;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

/**
 * Event loop for MWSR (Multiple Writer Single Reader) ring buffer using Memory Segment approach.
 * Minimal GC pressure design - no allocations in hot path.
 */
public class MWSREventLoop implements Runnable {

    private static final VarHandle RUNNING_VAR_HANDLE;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            RUNNING_VAR_HANDLE = lookup.findVarHandle(
                MWSREventLoop.class, "running", boolean.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final MWSRRingBufferHandler ringBuffer;
    private final RingBufferProcessor processor;
    private final long expectedBatchSize;
    private final long recordSize;
    private final int maxIdleSpins;
    private final long parkNanos;

    private boolean running;

    public MWSREventLoop(
        MWSRRingBufferHandler ringBuffer,
        RingBufferProcessor processor,
        long expectedBatchSize,
        long recordSize,
        int maxIdleSpins,
        long parkNanos
    ) {
        this.ringBuffer = ringBuffer;
        this.processor = processor;
        this.expectedBatchSize = expectedBatchSize;
        this.recordSize = recordSize;
        this.maxIdleSpins = maxIdleSpins;
        this.parkNanos = parkNanos;
        this.running = false;
    }

    @Override
    public void run() {
        RUNNING_VAR_HANDLE.setRelease(this, true);
        
        int idleSpins = 0;
        
        while ((boolean) RUNNING_VAR_HANDLE.getAcquire(this)) {
            long processed = ringBuffer.tryProcess(processor, expectedBatchSize, recordSize);
            
            if (processed > 0) {
                idleSpins = 0;
            } else {
                idleSpins++;
                handleIdle(idleSpins);
            }
        }
    }

    public void stop() {
        RUNNING_VAR_HANDLE.setRelease(this, false);
    }

    public boolean isRunning() {
        return (boolean) RUNNING_VAR_HANDLE.getAcquire(this);
    }

    private void handleIdle(int idleSpins) {
        if (idleSpins < maxIdleSpins) {
            Thread.onSpinWait();
        } else {
            LockSupport.parkNanos(parkNanos);
        }
    }

    public MWSRRingBufferHandler getRingBuffer() {
        return ringBuffer;
    }
}
