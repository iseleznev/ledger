package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration.LedgerConfiguration;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.prevprocessor.RingBufferProcessor;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.ringbuffer.MWSRRingBufferHandler;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.ringbuffer.SWSRRingBufferHandler;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.UUID;

import static org.seleznyov.iyu.kfin.ledger.shared.utils.AccountRouteUtils.partitionNumber;

public class TransferRingBufferProcessor implements RingBufferProcessor {

    private final LedgerConfiguration ledgerConfiguration;
    private final SWSRRingBufferHandler[] partitionRingBufferHandlers;
    private final TransferEventLoopStamper stamper;

    private long processOffset;

    public TransferRingBufferProcessor(
        LedgerConfiguration ledgerConfiguration,
        MWSRRingBufferHandler ringBufferHandler,
        SWSRRingBufferHandler[] partitionRingBufferHandlers
    ) {
        this.ledgerConfiguration = ledgerConfiguration;
        this.partitionRingBufferHandlers = partitionRingBufferHandlers;
        this.stamper = new TransferEventLoopStamper(ringBufferHandler);
    }


    @Override
    public long processOffset() {
        return processOffset;
    }

    @Override
    public void processOffset(long offset) {
        this.processOffset = offset;
    }

    @Override
    public int beforeBatchOperationGap() {
        return 0;
    }

    @Override
    public long process(MemorySegment memorySegment, long processOffset, long expectedProcessSize) {
//        final int transferRequestsCount = (int) (expectedProcessSize / TransferRequestOffsetConstants.TRANSFER_STAMP_SIZE);
        for (long transferOffset = processOffset; transferOffset < processOffset + expectedProcessSize; transferOffset += TransferRequestOffsetConstants.TRANSFER_STAMP_SIZE) {
            final UUID debitAccount = new UUID(
                memorySegment.get(ValueLayout.JAVA_LONG, transferOffset + TransferRequestOffsetConstants.TRANSFER_DEBIT_ACCOUNT_ID_MSB_OFFSET),
                memorySegment.get(ValueLayout.JAVA_LONG, transferOffset + TransferRequestOffsetConstants.TRANSFER_DEBIT_ACCOUNT_ID_LSB_OFFSET)
            );
            final int partitionNumber = partitionNumber(debitAccount, ledgerConfiguration.accountsPartitionsCount());
            final SWSRRingBufferHandler partitionRingBufferHandler = partitionRingBufferHandlers[partitionNumber];
            stamper.transferOffset(transferOffset);
            partitionRingBufferHandler.tryStampForward(stamper, ledgerConfiguration.ringBuffer().partitionAmounts().writeAttempts());
        }
        ;
        return expectedProcessSize;
    }

    @Override
    public long processBatch(MemorySegment memorySegment, long processOffset) {
        return 0;
    }
}
