package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.ringbuffer;

import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.stamper.RingBufferStamper;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.prevprocessor.RingBufferProcessor;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

public class MWSRRingBufferHandler {

    private static final VarHandle STAMP_OFFSET_VAR_HANDLE;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            STAMP_OFFSET_VAR_HANDLE = lookup.findVarHandle(
                MWSRRingBufferHandler.class, "stampOffset", long.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final MemorySegment memorySegment;
    private final long arenaSize;
    private final long halfArenaSize;

    private long readOffset;

    public MWSRRingBufferHandler(MemorySegment memorySegment) {
        this.memorySegment = memorySegment;
        this.arenaSize = memorySegment.byteSize();
        this.halfArenaSize = arenaSize >> 1;
        this.readOffset = 0;
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
            VarHandle.acquireFence();
//            final long currentReadOffset = readOffset - processor.beforeBatchOperationGap();

            final long recordSize = stamper.stampRecordSize();

            // Читаем текущее положение для записи через VarHandle
            final long currentStampOffset = (long) STAMP_OFFSET_VAR_HANDLE.getAcquire(this);
            final long nextStampOffset = (currentStampOffset + recordSize) % arenaSize;

//            final long availableSize = currentReadOffset > currentStampOffset
//                ? currentReadOffset - currentStampOffset
//                : arenaSize - currentStampOffset + currentReadOffset;

            long availableSize;
            if (currentStampOffset > readOffset) {
                availableSize = currentStampOffset < this.halfArenaSize
                    ? this.halfArenaSize - currentStampOffset
                    : this.arenaSize - currentStampOffset;
            } else if (readOffset > currentStampOffset) {
                availableSize = readOffset - currentStampOffset;
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
        VarHandle.acquireFence();
        final long currentStampOffset = (long) STAMP_OFFSET_VAR_HANDLE.getAcquire(this);

        long availableSize;
        if (readOffset > currentStampOffset) {
            availableSize = readOffset < this.halfArenaSize
                ? this.halfArenaSize - readOffset
                : this.arenaSize - readOffset;
        } else if (currentStampOffset > readOffset) {
            availableSize = currentStampOffset - readOffset;
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

        return currentStampOffset > readOffset && currentStampOffset - readOffset < expectedSize
            ? currentStampOffset - readOffset
            : expectedSize;
    }

    public long processedForward(long processedSize) {
        readOffset = (readOffset + processedSize) % arenaSize;
        VarHandle.releaseFence();
        return readOffset;
    }

    /**
     * Single reader - fence'ы достаточно для readOffset
     */
    public long tryProcess(RingBufferProcessor processor, long expectedSize, long recordSize) {
        VarHandle.acquireFence();
        final long currentStampOffset = (long) STAMP_OFFSET_VAR_HANDLE.getAcquire(this);

//        long availableSize = readOffset > this.halfArenaSize && currentStampOffset < this.halfArenaSize
//            ? this.arenaSize - readOffset
//            : currentStampOffset - readOffset;
//        if (readOffset < this.halfArenaSize && readOffset + availableSize > this.halfArenaSize) {
//            availableSize = this.halfArenaSize - readOffset;
//        }
//        if (availableSize > expectedSize) {
//            availableSize = expectedSize;
//        }
//
//        if (availableSize <= 0) {
//            return false;
//        }

        long availableSize;
        if (readOffset > currentStampOffset) {
            availableSize = readOffset < this.halfArenaSize
                ? this.halfArenaSize - readOffset
                : this.arenaSize - readOffset;
        } else if (currentStampOffset > readOffset) {
            availableSize = currentStampOffset - readOffset;
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

        final long processedSize = processor.process(memorySegment, readOffset, availableSize);

        readOffset = (readOffset + processedSize) % arenaSize;
        VarHandle.releaseFence();

        return processedSize;
    }

    public long readOffset() {
        return readOffset;
    }

    /**
     * Батчевое чтение для эффективности
     */
    public long tryProcessBatch(RingBufferProcessor processor, int maxSize) {
        VarHandle.acquireFence();
        final long currentStampOffset = (long) STAMP_OFFSET_VAR_HANDLE.getAcquire(this);

        final long readSize = readOffset + maxSize > this.arenaSize
            ? this.arenaSize - readOffset
            : maxSize;

        if (readOffset > currentStampOffset - readSize) {
            return 0;
        }

        final long processedSize = processor.processBatch(memorySegment, readOffset);

        readOffset = (readOffset + processedSize) % arenaSize;
        VarHandle.releaseFence();
        return processedSize;
    }
}