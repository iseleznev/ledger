package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor;

import com.lmax.disruptor.EventFactory;

/**
 * Event Factory for Disruptor - zero allocation object creation
 */
public final class LedgerWalEventFactory implements EventFactory<LedgerWalEvent> {

    @Override
    public LedgerWalEvent newInstance() {
        return new LedgerWalEvent();
    }
}
