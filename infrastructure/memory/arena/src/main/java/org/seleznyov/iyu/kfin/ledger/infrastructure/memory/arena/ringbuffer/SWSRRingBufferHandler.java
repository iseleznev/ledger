package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.ringbuffer;

import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.prevprocessor.RingBufferProcessor;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.stamper.RingBufferStamper;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

public class SWSRRingBufferHandler {
    private final MemorySegment memorySegment;
    private final long arenaSize;
    private final long halfArenaSize;

    private long stampOffset;
    private long readOffset;

    public SWSRRingBufferHandler(MemorySegment memorySegment) {
        this.memorySegment = memorySegment;
        this.arenaSize = memorySegment.byteSize();
        this.halfArenaSize = this.arenaSize >> 1;
        this.stampOffset = 0;
        this.readOffset = 0;
    }

    /**
     * Single writer - fence'ы достаточно, CAS не нужен
     */
    public boolean tryStampForward(RingBufferStamper ringBufferStamper, int maxAttempts) {
        long recordSize = ringBufferStamper.stampRecordSize();

        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            // Читаем положение читателя
            VarHandle.acquireFence();

            long currentStampOffset = stampOffset;
            long nextStampOffset = (stampOffset + recordSize) % arenaSize;


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

            ringBufferStamper.stamp(memorySegment, stampOffset);

            stampOffset = nextStampOffset;
            VarHandle.releaseFence();

            return true;
        }

        return false;
    }

    /**
     * Single reader - fence'ы достаточно
     */
    public long tryProcess(RingBufferProcessor processor, int expectedSize, int recordSize) {
        VarHandle.acquireFence();
        final long currentStampOffset = stampOffset;

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

    public long readyToProcess(int expectedSize, int recordSize) {
        VarHandle.acquireFence();
        final long currentStampOffset = stampOffset;

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
        return availableSize;
    }

    public long processedForward(long processedSize) {
        readOffset = (readOffset + processedSize) % arenaSize;
        VarHandle.releaseFence();
        return readOffset;
    }

    /**
     * Батчевое чтение для эффективности
     */
    public long tryProcessBatch(RingBufferProcessor processor, int maxSize) {
        VarHandle.acquireFence();
        long currentStampOffset = stampOffset;

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

    public MemorySegment memorySegment() {
        return memorySegment;
    }

    public long availableToRead() {
        VarHandle.acquireFence();
        long currentStampOffset = stampOffset;

        if (currentStampOffset >= readOffset) {
            return currentStampOffset - readOffset;
        } else {
            return arenaSize - readOffset + currentStampOffset;
        }
    }

    public long availableToWrite() {
        VarHandle.acquireFence();
        long currentReadOffset = readOffset;

        if (currentReadOffset > stampOffset) {
            return currentReadOffset - stampOffset;
        } else {
            return arenaSize - stampOffset + currentReadOffset;
        }
    }
}