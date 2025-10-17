package org.seleznyov.iyu.kfin.ledgerservice.core.actor;

import org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties.LedgerConfiguration;
import org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.handler.PipelineSWSRRingBufferHandler;
import org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.handler.SimpleSWSRRingBufferHandler;

public class ActorFactory {

    public PartitionActor newPartitionActor(
        LedgerConfiguration ledgerConfiguration,
        int partitionNumber
    ) {
        final PipelineSWSRRingBufferHandler walRingBufferHandler;
        final SimpleSWSRRingBufferHandler walStripeRingBufferHandler;
        final PipelineSWSRRingBufferHandler snapshotRingBufferHandler;
        final SimpleSWSRRingBufferHandler snapshotStripeRingBufferHandler;
        final SimpleSWSRRingBufferHandler snapshotWalRingBufferHandler;
        final SimpleSWSRRingBufferHandler commitRingBufferHandler;
        final SimpleSWSRRingBufferHandler partitionAmountRingBufferHandler;
        final SimpleSWSRRingBufferHandler transferRequestRingBufferHandler;
    }
}
