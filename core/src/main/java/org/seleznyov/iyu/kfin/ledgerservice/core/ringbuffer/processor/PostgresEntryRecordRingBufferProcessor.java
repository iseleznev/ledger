package org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.processor;

import org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties.LedgerConfiguration;
import org.seleznyov.iyu.kfin.ledgerservice.core.constants.EntryRecordOffsetConstants;
import org.seleznyov.iyu.kfin.ledgerservice.core.constants.PostgresConstants;
import org.seleznyov.iyu.kfin.ledgerservice.core.postgres.EntryRecordPostgresWriter;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public class PostgresEntryRecordRingBufferProcessor implements PipelineRingBufferProcessor {

    private static final int BEFORE_BATCH_OPERATION_GAP = PostgresConstants.POSTGRES_HEADER_SIZE;

    private static final VarHandle PROCESS_OFFSET_VAR_HANDLE;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            PROCESS_OFFSET_VAR_HANDLE = lookup.findVarHandle(
                PostgresEntryRecordRingBufferProcessor.class, "processOffset", long.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final LedgerConfiguration ledgerConfiguration;
    private final EntryRecordPostgresWriter postgresWriter;
    private final WalWriterRingBufferProcessor walProcessor;
    private long processOffset;

    public PostgresEntryRecordRingBufferProcessor(
        LedgerConfiguration ledgerConfiguration,
        EntryRecordPostgresWriter postgresWriter,
        WalWriterRingBufferProcessor walProcessor
    ) {
        this.ledgerConfiguration = ledgerConfiguration;
        this.postgresWriter = postgresWriter;
        this.walProcessor = walProcessor;
    }

    @Override
    public int beforeBatchOperationGap() {
        return BEFORE_BATCH_OPERATION_GAP;
    }

    @Override
    public long pipelineProcessOffset() {
        return (long) PROCESS_OFFSET_VAR_HANDLE.get(this);
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
        int availableBeforeStampOffsetSize;
        long dependencyProcessOffset = walProcessor.pipelineProcessOffset();
        if (processOffset > stampOffset) {
            availableBeforeStampOffsetSize = processOffset < halfArenaSize
                ? (int) (halfArenaSize - processOffset)
                : (int) (arenaSize - processOffset);
        } else if (stampOffset > processOffset) {
            availableBeforeStampOffsetSize = (int) (stampOffset - processOffset);
        } else {
            availableBeforeStampOffsetSize = 0;
        }

        int availableSize;

        if (processOffset > dependencyProcessOffset) {
            availableSize = processOffset < halfArenaSize
                ? (int) (halfArenaSize - processOffset)
                : (int) (arenaSize - processOffset);
        } else if (dependencyProcessOffset > processOffset) {
            availableSize = (int) (dependencyProcessOffset - processOffset);
        } else {
            availableSize = 0;
        }

        if (availableSize > availableBeforeStampOffsetSize) {
            availableSize = availableBeforeStampOffsetSize;
        }

        if (availableSize < EntryRecordOffsetConstants.POSTGRES_ENTRY_RECORD_SIZE) {
            availableSize = 0;
        }
        if (availableSize > processSize) {
            availableSize = processSize;
        }

        if (availableSize <= 0) {
            return availableSize;
        }

        final long totalWritten = postgresWriter.write(
            memorySegment,
            processOffset,
            availableSize
        );

        long nextProcessOffset = processOffset + totalWritten;
        if (nextProcessOffset >= arenaSize) {
            nextProcessOffset = 0;
        }

        PROCESS_OFFSET_VAR_HANDLE.setRelease(this, nextProcessOffset);

        return totalWritten;
    }
}
