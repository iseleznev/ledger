package org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.processor;

import org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties.LedgerConfiguration;
import org.seleznyov.iyu.kfin.ledgerservice.core.wal.WalWriter;

import java.lang.foreign.MemorySegment;

public class WalRingBufferProcessor implements PipelineRingBufferProcessor {

    private static final int BEFORE_BATCH_OPERATION_GAP = 0;

    private final LedgerConfiguration ledgerConfiguration;
    private final WalWriter walWriter;

    public WalRingBufferProcessor(
        LedgerConfiguration ledgerConfiguration,
        WalWriter walWriter
    ) {
        this.ledgerConfiguration = ledgerConfiguration;
        this.walWriter = walWriter;
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
    public long process(MemorySegment memorySegment, long processOffset, long expectedProcessSize, long walSequenceId, long stateSequenceId) {
        final long totalWritten = walWriter.write(memorySegment, walSequenceId, processOffset, expectedProcessSize);
        return totalWritten;
    }
}
