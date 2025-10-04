package org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.handler;

import org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.processor.PipelineRingBufferProcessor;
import org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.stamper.RingBufferStamper;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

import static org.seleznyov.iyu.kfin.ledgerservice.core.constants.EntryRecordOffsetConstants.POSTGRES_ENTRY_RECORD_SIZE;

public class PipelineSWSRRingBufferHandler {

    private static final VarHandle STAMP_OFFSET_VAR_HANDLE;
    private static final VarHandle PROCESS_OFFSET_VAR_HANDLE;
    private static final VarHandle WAL_SEQUENCE_ID_VAR_HANDLE;
    private static final VarHandle STATE_SEQUENCE_ID_VAR_HANDLE;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            STAMP_OFFSET_VAR_HANDLE = lookup.findVarHandle(
                PipelineSWSRRingBufferHandler.class, "stampOffset", long.class);
            PROCESS_OFFSET_VAR_HANDLE = lookup.findVarHandle(
                PipelineSWSRRingBufferHandler.class, "processOffset", long.class);
            WAL_SEQUENCE_ID_VAR_HANDLE = lookup.findVarHandle(
                PipelineSWSRRingBufferHandler.class, "walSequenceId", long.class);
            STATE_SEQUENCE_ID_VAR_HANDLE = lookup.findVarHandle(
                PipelineSWSRRingBufferHandler.class, "stateSequenceId", long.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final MemorySegment memorySegment;
    private final long arenaSize;
    private final long halfArenaSize;

    private long stampOffset;
    private long processOffset;
    private long walSequenceId;
    private long stateSequenceId;

    public PipelineSWSRRingBufferHandler(
        MemorySegment memorySegment
    ) {
        this.memorySegment = memorySegment;
        this.arenaSize = memorySegment.byteSize();
        this.halfArenaSize = this.arenaSize >> 1;
        this.stampOffset = 0;
        this.processOffset = 0;
        this.stateSequenceId = 0;
        this.walSequenceId = 0;
    }

    /**
     * Single writer - fence'ы достаточно, CAS не нужен
     */
    public boolean tryStampForward(RingBufferStamper ringBufferStamper, long walSequenceId, long stateSequenceId, int maxAttempts) {
        long recordSize = ringBufferStamper.stampRecordSize();

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

            ringBufferStamper.stamp(memorySegment, currentProcessOffset);

            if (currentProcessOffset >= this.halfArenaSize && nextStampOffset <= this.halfArenaSize && availableSize > 0) {
                final long bufferStartSequenceId = (long) WAL_SEQUENCE_ID_VAR_HANDLE.get(this);
                final long bufferStartEntriesAhead = nextStampOffset / POSTGRES_ENTRY_RECORD_SIZE;
                if (bufferStartSequenceId < bufferStartEntriesAhead) {
                    throw new IllegalStateException("Buffer start wal sequence id is less than entries count in the buffer for buffer start sequence id = " + bufferStartSequenceId + ", entries count ahead = " + bufferStartEntriesAhead);
                }
                WAL_SEQUENCE_ID_VAR_HANDLE.set(this, walSequenceId - bufferStartEntriesAhead);
            }

            STATE_SEQUENCE_ID_VAR_HANDLE.set(this, stateSequenceId);
            STAMP_OFFSET_VAR_HANDLE.setRelease(this, nextStampOffset);

            return true;
        }

        return false;
    }

    public long tryProcess(
        long dependencyProcessOffset,
        PipelineRingBufferProcessor processor,
        int expectedSize,
        int recordSize
    ) {
        final long currentStampOffset = (long) STAMP_OFFSET_VAR_HANDLE.getAcquire(this);
        final long currentProcessOffset = (long) PROCESS_OFFSET_VAR_HANDLE.get(this);
        final long stateSequenceId = (long) STATE_SEQUENCE_ID_VAR_HANDLE.get(this);
        final long walSequenceId = (long) WAL_SEQUENCE_ID_VAR_HANDLE.getAcquire(this);

        long stampAvailableSize;
        if (currentProcessOffset > currentStampOffset) {
            stampAvailableSize = currentProcessOffset < this.halfArenaSize
                ? this.halfArenaSize - currentProcessOffset
                : this.arenaSize - currentProcessOffset;
        } else if (currentStampOffset > currentProcessOffset) {
            stampAvailableSize = currentStampOffset - currentProcessOffset;
        } else {
            stampAvailableSize = 0;
        }
        if (stampAvailableSize < recordSize) {
            stampAvailableSize = 0;
        }
        if (stampAvailableSize > expectedSize) {
            stampAvailableSize = expectedSize;
        }

        long availableSize;
        if (currentProcessOffset > dependencyProcessOffset) {
            availableSize = currentProcessOffset < this.halfArenaSize
                ? this.halfArenaSize - currentProcessOffset
                : this.arenaSize - currentProcessOffset;
        } else if (currentStampOffset > currentProcessOffset) {
            availableSize = dependencyProcessOffset - currentProcessOffset;
        } else {
            availableSize = 0;
        }
        if (availableSize < recordSize) {
            availableSize = 0;
        }
        if (availableSize > expectedSize) {
            availableSize = expectedSize;
        }

        if (availableSize > stampAvailableSize) {
            availableSize = stampAvailableSize;
        }

        if (availableSize > expectedSize) {
            availableSize = expectedSize;
        }

        if (availableSize <= 0) {
            return availableSize;
        }

        final long processedSize = processor.process(memorySegment, currentProcessOffset, availableSize, walSequenceId, stateSequenceId);

        final long nextProcessOffset = (currentProcessOffset + processedSize) % arenaSize;

        PROCESS_OFFSET_VAR_HANDLE.setRelease(this, nextProcessOffset);

        return processedSize;
    }

    public long tryProcess(
        long currentProcessOffset,
        long dependencyProcessOffset,
        PipelineRingBufferProcessor processor,
        int expectedSize,
        int recordSize
    ) {
        final long currentStampOffset = (long) STAMP_OFFSET_VAR_HANDLE.getAcquire(this);
        final long stateSequenceId = (long) STATE_SEQUENCE_ID_VAR_HANDLE.get(this);
        final long walSequenceId = (long) WAL_SEQUENCE_ID_VAR_HANDLE.getAcquire(this);

        long stampAvailableSize;
        if (currentProcessOffset > currentStampOffset) {
            stampAvailableSize = currentProcessOffset < this.halfArenaSize
                ? this.halfArenaSize - currentProcessOffset
                : this.arenaSize - currentProcessOffset;
        } else if (currentStampOffset > currentProcessOffset) {
            stampAvailableSize = currentStampOffset - currentProcessOffset;
        } else {
            stampAvailableSize = 0;
        }
        if (stampAvailableSize < recordSize) {
            stampAvailableSize = 0;
        }
        if (stampAvailableSize > expectedSize) {
            stampAvailableSize = expectedSize;
        }

        long availableSize;
        if (currentProcessOffset > dependencyProcessOffset) {
            availableSize = currentProcessOffset < this.halfArenaSize
                ? this.halfArenaSize - currentProcessOffset
                : this.arenaSize - currentProcessOffset;
        } else if (currentStampOffset > currentProcessOffset) {
            availableSize = dependencyProcessOffset - currentProcessOffset;
        } else {
            availableSize = 0;
        }
        if (availableSize < recordSize) {
            availableSize = 0;
        }
        if (availableSize > expectedSize) {
            availableSize = expectedSize;
        }

        if (availableSize > stampAvailableSize) {
            availableSize = stampAvailableSize;
        }

        if (availableSize > expectedSize) {
            availableSize = expectedSize;
        }

        if (availableSize <= 0) {
            return availableSize;
        }

        final long processedSize = processor.process(memorySegment, currentProcessOffset, availableSize, walSequenceId, stateSequenceId);

        return (currentProcessOffset + processedSize) % arenaSize;
    }

    public long tryProcess(
        PipelineRingBufferProcessor processor,
        int expectedSize,
        int recordSize
    ) {
        final long currentStampOffset = (long) STAMP_OFFSET_VAR_HANDLE.getAcquire(this);
        final long currentProcessOffset = (long) PROCESS_OFFSET_VAR_HANDLE.get(this);
        final long stateSequenceId = (long) STATE_SEQUENCE_ID_VAR_HANDLE.get(this);
        final long walSequenceId = (long) WAL_SEQUENCE_ID_VAR_HANDLE.getAcquire(this);

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

        if (availableSize > expectedSize) {
            availableSize = expectedSize;
        }

        if (availableSize <= 0) {
            return availableSize;
        }

        final long processedSize = processor.process(memorySegment, currentProcessOffset, availableSize, walSequenceId, stateSequenceId);

        final long nextProcessOffset = (currentProcessOffset + processedSize) % arenaSize;

        PROCESS_OFFSET_VAR_HANDLE.setRelease(this, nextProcessOffset);

        return processedSize;
    }

    public long walSequenceId() {
        return (long) WAL_SEQUENCE_ID_VAR_HANDLE.getAcquire(this);
    }

    public long stateSequenceId() {
        return (long) STATE_SEQUENCE_ID_VAR_HANDLE.getAcquire(this);
    }

    public long processOffset() {
        return (long) PROCESS_OFFSET_VAR_HANDLE.getAcquire(this);
    }

    public long stampOffset() {
        return (long) STAMP_OFFSET_VAR_HANDLE.getAcquire(this);
    }

    public long readyToProcess(
        long currentProcessOffset,
        long dependencyProcessOffset,
        int expectedSize,
        int recordSize
    ) {
        final long currentStampOffset = (long) STAMP_OFFSET_VAR_HANDLE.getAcquire(this);

        long stampAvailableSize;
        if (currentProcessOffset > currentStampOffset) {
            stampAvailableSize = currentProcessOffset < this.halfArenaSize
                ? this.halfArenaSize - currentProcessOffset
                : this.arenaSize - currentProcessOffset;
        } else if (currentStampOffset > currentProcessOffset) {
            stampAvailableSize = currentStampOffset - currentProcessOffset;
        } else {
            stampAvailableSize = 0;
        }
        if (stampAvailableSize < recordSize) {
            stampAvailableSize = 0;
        }
        if (stampAvailableSize > expectedSize) {
            stampAvailableSize = expectedSize;
        }

        long availableSize;
        if (currentProcessOffset > dependencyProcessOffset) {
            availableSize = currentProcessOffset < this.halfArenaSize
                ? this.halfArenaSize - currentProcessOffset
                : this.arenaSize - currentProcessOffset;
        } else if (currentStampOffset > currentProcessOffset) {
            availableSize = dependencyProcessOffset - currentProcessOffset;
        } else {
            availableSize = 0;
        }
        if (availableSize < recordSize) {
            availableSize = 0;
        }
        if (availableSize > expectedSize) {
            availableSize = expectedSize;
        }

        if (availableSize > stampAvailableSize) {
            availableSize = stampAvailableSize;
        }

        if (availableSize <= 0) {
            return availableSize;
        }
        return availableSize;
    }

    public long readyToProcess(
        long dependencyProcessOffset,
        int expectedSize,
        int recordSize
    ) {
        final long currentStampOffset = (long) STAMP_OFFSET_VAR_HANDLE.getAcquire(this);
        final long ownerProcessOffset = (long) PROCESS_OFFSET_VAR_HANDLE.get(this);
        final long currentProcessOffset = dependencyProcessOffset < currentStampOffset
            ? dependencyProcessOffset
            : ownerProcessOffset;

        long stampAvailableSize;
        if (currentProcessOffset > currentStampOffset) {
            stampAvailableSize = currentProcessOffset < this.halfArenaSize
                ? this.halfArenaSize - currentProcessOffset
                : this.arenaSize - currentProcessOffset;
        } else if (currentStampOffset > currentProcessOffset) {
            stampAvailableSize = currentStampOffset - currentProcessOffset;
        } else {
            stampAvailableSize = 0;
        }
        if (stampAvailableSize < recordSize) {
            stampAvailableSize = 0;
        }
        if (stampAvailableSize > expectedSize) {
            stampAvailableSize = expectedSize;
        }

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

        if (availableSize > stampAvailableSize) {
            availableSize = stampAvailableSize;
        }

        if (availableSize <= 0) {
            return availableSize;
        }
        return availableSize;
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