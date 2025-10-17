package org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.handler;

import org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.processor.RingBufferProcessor;
import org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.stamper.RingBufferStamper;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

import static org.seleznyov.iyu.kfin.ledgerservice.core.constants.EntryRecordOffsetConstants.POSTGRES_ENTRY_RECORD_SIZE;

public class SimpleSWSRRingBufferHandler {

    private static final VarHandle STAMP_OFFSET_VAR_HANDLE;
    private static final VarHandle PROCESS_OFFSET_VAR_HANDLE;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            STAMP_OFFSET_VAR_HANDLE = lookup.findVarHandle(
                SimpleSWSRRingBufferHandler.class, "stampOffset", long.class);
            PROCESS_OFFSET_VAR_HANDLE = lookup.findVarHandle(
                SimpleSWSRRingBufferHandler.class, "processOffset", long.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final MemorySegment memorySegment;
    private final long arenaSize;
    private final long halfArenaSize;

    private long stampOffset;
    private long processOffset;

    public SimpleSWSRRingBufferHandler(MemorySegment memorySegment) {
        this.memorySegment = memorySegment;
        this.arenaSize = memorySegment.byteSize();
        this.halfArenaSize = this.arenaSize >> 1;
        this.stampOffset = 0;
        this.processOffset = 0;
    }

    /**
     * Single writer - fence'ы достаточно, CAS не нужен
     */
    public boolean tryStampForward(RingBufferStamper ringBufferStamper, int maxAttempts) {
        long recordSize = ringBufferStamper.stampRecordSize();
        if (recordSize <= 0) {
            recordSize = arenaSize;
        }

        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            // Читаем положение читателя
//            VarHandle.acquireFence();

            long currentStampOffset = (long) STAMP_OFFSET_VAR_HANDLE.getAcquire(this);
            long nextStampOffset = (stampOffset + recordSize) % arenaSize;
            long currentProcessOffset = (long) PROCESS_OFFSET_VAR_HANDLE.get(this);


            long availableSize;
            if (currentStampOffset > currentProcessOffset) {
                availableSize = currentStampOffset < this.halfArenaSize
                    ? this.halfArenaSize - currentStampOffset
                    : this.arenaSize - currentStampOffset;
            } else if (currentProcessOffset > currentStampOffset) {
                availableSize = currentProcessOffset - currentStampOffset;
            } else {
                availableSize = 0;
            }
            if (availableSize > recordSize) {
                availableSize = recordSize;
            }

            if (availableSize < recordSize) {
                if (attempt < 20) {
                    Thread.onSpinWait();
                } else if (attempt < 50) {
                    Thread.yield();
                } else {
                    LockSupport.parkNanos(1_000L << Math.min(attempt - 50, 10));
                }
                continue;
            }

            ringBufferStamper.stamp(memorySegment, currentProcessOffset, availableSize, 0, 0);
            STAMP_OFFSET_VAR_HANDLE.setRelease(this, nextStampOffset);

            return true;
        }

        return false;
    }

    /**
     * Single reader - fence'ы достаточно
     */
    public long tryProcess(RingBufferProcessor processor, int expectedSize, int recordSize) {
        final long currentStampOffset = (long) STAMP_OFFSET_VAR_HANDLE.getAcquire(this);
        final long currentProcessOffset = (long) PROCESS_OFFSET_VAR_HANDLE.get(this);

        long availableSize;
        if (currentProcessOffset > currentStampOffset) {
            availableSize = currentProcessOffset < this.halfArenaSize
                ? this.halfArenaSize - currentProcessOffset
                : this.arenaSize - currentProcessOffset;
        } else if (currentStampOffset > currentProcessOffset) {
            availableSize = currentStampOffset - currentProcessOffset;
        } else {
            availableSize = 0;
        }
        if (availableSize < recordSize) {
            availableSize = 0;
        }
        if (availableSize > expectedSize) {
            availableSize = expectedSize;
        }

        if (availableSize <= 0) {
            return availableSize;
        }

        final long processedSize = processor.process(memorySegment, currentProcessOffset, availableSize, 0, 0);

        final long nextProcessOffset = (currentProcessOffset + processedSize) % arenaSize;

        PROCESS_OFFSET_VAR_HANDLE.setRelease(this, nextProcessOffset);

        return processedSize;
    }

    public long processOffset() {
        return (long) PROCESS_OFFSET_VAR_HANDLE.getAcquire(this);
    }

    public long stampOffset() {
        return (long) STAMP_OFFSET_VAR_HANDLE.getAcquire(this);
    }

    public long readyToProcess(int expectedSize, int recordSize) {
        final long currentStampOffset = (long) STAMP_OFFSET_VAR_HANDLE.getAcquire(this);
        final long currentProcessOffset = (long) PROCESS_OFFSET_VAR_HANDLE.get(this);

        long availableSize;
        if (currentProcessOffset > currentStampOffset) {
            availableSize = currentProcessOffset < this.halfArenaSize
                ? this.halfArenaSize - currentProcessOffset
                : this.arenaSize - currentProcessOffset;
        } else if (currentStampOffset > currentProcessOffset) {
            availableSize = currentStampOffset - currentProcessOffset;
        } else {
            availableSize = 0;
        }
        if (availableSize < recordSize) {
            availableSize = 0;
        }
        if (availableSize > expectedSize) {
            availableSize = expectedSize;
        }

        if (availableSize <= 0) {
            return availableSize;
        }
        return availableSize;
    }

    public long processedForward(long processedSize) {
        final long nextProcessOffset = ((long) PROCESS_OFFSET_VAR_HANDLE.get(this) + processedSize) % arenaSize;
        PROCESS_OFFSET_VAR_HANDLE.setRelease(this, nextProcessOffset);
        return nextProcessOffset;
    }

    public MemorySegment memorySegment() {
        return memorySegment;
    }

    public long availableToRead() {
        VarHandle.acquireFence();
        long currentStampOffset = stampOffset;

        if (currentStampOffset >= processOffset) {
            return currentStampOffset - processOffset;
        } else {
            return arenaSize - processOffset + currentStampOffset;
        }
    }

    public long availableToWrite() {
        VarHandle.acquireFence();
        long currentReadOffset = processOffset;

        if (currentReadOffset > stampOffset) {
            return currentReadOffset - stampOffset;
        } else {
            return arenaSize - stampOffset + currentReadOffset;
        }
    }
}