package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * High-performance batch container for WAL operations
 * Optimized for Virtual Threads and minimal allocations
 */
@Slf4j
@RequiredArgsConstructor
public final class WalOperationBatch {

    private final List<LedgerWalEvent> operations;
    private final int maxSize;

    public WalOperationBatch(int maxSize) {
        // Pre-allocate with expected capacity
        this.operations = new ArrayList<>(maxSize);
        this.maxSize = maxSize;
    }

    public boolean add(LedgerWalEvent event) {
        if (operations.size() < maxSize) {
            operations.add(event.copy()); // Copy for safety outside ring buffer
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
        return new ArrayList<>(operations); // Defensive copy
    }

    public void clear() {
        operations.clear();
    }
}
