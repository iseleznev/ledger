package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor.updated;

import com.lmax.disruptor.EventFactory;

public final class LedgerWalEventFactory implements EventFactory<LedgerWalEvent> {

    @Override
    public LedgerWalEvent newInstance() {
        return new LedgerWalEvent();
    }
}
