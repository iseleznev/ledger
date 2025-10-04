package org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.handler;

import org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.processor.RingBufferProcessor;
import org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.stamper.RingBufferStamper;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

public class MWSRRingBufferHandler {

    private static final VarHandle STAMP_OFFSET_VAR_HANDLE;
    private static final VarHandle PROCESS_OFFSET_VAR_HANDLE;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            STAMP_OFFSET_VAR_HANDLE = lookup.findVarHandle(
                MWSRRingBufferHandler.class, "stampOffset", long.class);
            PROCESS_OFFSET_VAR_HANDLE = lookup.findVarHandle(
                MWSRRingBufferHandler.class, "processOffset", long.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final MemorySegment memorySegment;
    private final long arenaSize;
    private final long halfArenaSize;

    private long stampOffset = 0;
    private long processOffset = 0;

    public MWSRRingBufferHandler(MemorySegment memorySegment) {
        this.memorySegment = memorySegment;
        this.arenaSize = memorySegment.byteSize();
        this.halfArenaSize = arenaSize >> 1;
    }

    public MemorySegment memorySegment() {
        return this.memorySegment;
    }

    /**
     * Multiple writers - CAS через VarHandle для stampOffset
     */
    public boolean tryStampForward(RingBufferStamper stamper, int maxAttempts) {
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            // Читаем текущее положение читателя
//            VarHandle.acquireFence();

            final long recordSize = stamper.stampRecordSize();

            // Читаем текущее положение для записи через VarHandle
            final long currentStampOffset = (long) STAMP_OFFSET_VAR_HANDLE.getAcquire(this);
            final long nextStampOffset = (currentStampOffset + recordSize) % arenaSize;
            final long currentProcessOffset = (long) PROCESS_OFFSET_VAR_HANDLE.get(this);

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

            // Пытаемся атомарно захватить слот через CAS
            final boolean success = STAMP_OFFSET_VAR_HANDLE.compareAndSet(
                this,
                currentStampOffset,
                nextStampOffset
            );

            if (!success) {
                // Другой писатель опередил нас
                Thread.onSpinWait();
                continue;
            }

            // Успешно захватили слот - пишем данные
            try {
                stamper.stamp(memorySegment, currentStampOffset);
                VarHandle.releaseFence();
                return true;
            } catch (Exception e) {
                // Откатываем stampOffset в случае ошибки (best effort)
                STAMP_OFFSET_VAR_HANDLE.compareAndSet(this, nextStampOffset, currentStampOffset);
                throw new RuntimeException("Failed to stamp record", e);
            }
        }

        return false;
    }

    public long readyToProcess(int expectedSize, int recordSize) {
//        VarHandle.acquireFence();
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

        return currentStampOffset > currentProcessOffset && currentStampOffset - currentProcessOffset < expectedSize
            ? currentStampOffset - currentProcessOffset
            : expectedSize;
    }

    public long processedForward(long processedSize) {
        final long nextProcessOffset = ((long)PROCESS_OFFSET_VAR_HANDLE.get(this) + processedSize) % arenaSize;
        PROCESS_OFFSET_VAR_HANDLE.setRelease(this, nextProcessOffset);
        return nextProcessOffset;
    }

    /**
     * Single reader - fence'ы достаточно для readOffset
     */
    public long tryProcess(RingBufferProcessor processor, long expectedSize, long recordSize) {
//        VarHandle.acquireFence();
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

        final long processedSize = processor.process(memorySegment, currentProcessOffset, availableSize);

        final long nextProcessOffset = (currentProcessOffset + processedSize) % arenaSize;
        PROCESS_OFFSET_VAR_HANDLE.setRelease(this, nextProcessOffset);

        return processedSize;
    }

    public long processOffset() {
        return (long) PROCESS_OFFSET_VAR_HANDLE.getAcquire(this);
    }
}