package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor.updated;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
public final class WalOperationBatch {

    private final List<LedgerWalEvent> operations;
    private final int maxSize;

    public WalOperationBatch(int maxSize) {
        this.operations = new ArrayList<>(maxSize);
        this.maxSize = maxSize;
    }

    public boolean add(LedgerWalEvent event) {
        if (operations.size() < maxSize) {
            operations.add(event.copy());
            return true;
        }
        return false;
    }

    public boolean isFull() {
        return operations.size() >= maxSize;
    }

    public boolean isEmpty() {
        return operations.isEmpty();
    }

    public int size() {
        return operations.size();
    }

    public List<LedgerWalEvent> getOperations() {
        return new ArrayList<>(operations);
    }

    public void clear() {
        operations.clear();
    }
}
