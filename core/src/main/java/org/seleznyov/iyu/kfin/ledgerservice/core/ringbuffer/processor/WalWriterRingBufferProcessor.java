package org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.processor;

import org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties.LedgerConfiguration;
import org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.handler.SWSRRingBufferHandler;
import org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.stamper.CommitConfirmRingBufferStamper;
import org.seleznyov.iyu.kfin.ledgerservice.core.wal.WalWriter;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public class WalWriterRingBufferProcessor implements PipelineRingBufferProcessor {

    private static final VarHandle PROCESS_OFFSET_VAR_HANDLE;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            PROCESS_OFFSET_VAR_HANDLE = lookup.findVarHandle(
                WalWriterRingBufferProcessor.class, "processOffset", long.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final LedgerConfiguration ledgerConfiguration;
    private final WalWriter walWriter;
    private final SWSRRingBufferHandler commitRingBufferHandler;
    private final CommitConfirmRingBufferStamper commitRingBufferStamper;
    private long processOffset;

    public WalWriterRingBufferProcessor(
        LedgerConfiguration ledgerConfiguration,
        SWSRRingBufferHandler commitRingBufferHandler,
        CommitConfirmRingBufferStamper commitRingBufferStamper,
        WalWriter walWriter
    ) {
        this.ledgerConfiguration = ledgerConfiguration;
        this.commitRingBufferHandler = commitRingBufferHandler;
        this.commitRingBufferStamper = commitRingBufferStamper;
        this.walWriter = walWriter;
    }

    @Override
    public long pipelineProcessOffset() {
        return (long) PROCESS_OFFSET_VAR_HANDLE.get(this);
    }

    @Override
    public int beforeBatchOperationGap() {
        return 0;
    }

    @Override
    public long process(
        MemorySegment memorySegment,
        long arenaSize,
        long stampOffset,
        int processSize,
        long walSequenceId,
        long stateSequenceId
    ) {
        long halfArenaSize = arenaSize >> 1;
        long availableSize;
        long currentProcessOffset = (long) PROCESS_OFFSET_VAR_HANDLE.get(this);
        if (currentProcessOffset > stampOffset) {
            availableSize = currentProcessOffset < halfArenaSize
                ? halfArenaSize - currentProcessOffset
                : arenaSize - currentProcessOffset;
        } else if (stampOffset > currentProcessOffset) {
            availableSize = stampOffset - currentProcessOffset;
        } else {
            availableSize = 0;
        }
        if (availableSize < processSize) {
            availableSize = 0;
        }
        if (availableSize > processSize) {
            availableSize = processSize;
        }

        if (availableSize <= 0) {
            return availableSize;
        }

        final long totalWritten = walWriter.write(
            memorySegment,
            walSequenceId,
            currentProcessOffset,
            availableSize
        );
        commitRingBufferHandler.tryStampForward(
            commitRingBufferStamper,
            walSequenceId,
            stateSequenceId,
            ledgerConfiguration.ringBuffer().commits().writeAttempts()
        );
        currentProcessOffset += totalWritten;
        if (currentProcessOffset >= arenaSize) {
            currentProcessOffset = 0;
        }
        PROCESS_OFFSET_VAR_HANDLE.setRelease(this, currentProcessOffset);

        return totalWritten;
    }
}
