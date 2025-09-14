package org.seleznyov.iyu.kfin.ledger.application.service.percore;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-Memory WAL - быстрый буфер для immediate записи операций
 * Используется для minimize risk window между operation и disk flush
 */
@Slf4j
@Component
public class InMemoryWAL {

    private static final int DEFAULT_CAPACITY = 100_000; // 100K operations in memory
    private static final int FLUSH_BATCH_SIZE = 1_000;   // Flush in 1K batches

    // Ring buffer для lock-free операций
    private final TransactionEntry[] buffer;
    private final AtomicLong writePosition = new AtomicLong(0);
    private final AtomicLong flushedPosition = new AtomicLong(0);
    private final int capacity;
    private final int mask; // For power-of-2 optimization

    // Performance tracking
    private final AtomicLong totalOperations = new AtomicLong(0);
    private final AtomicLong totalFlushes = new AtomicLong(0);
    private final AtomicLong unflushedOperations = new AtomicLong(0);

    public InMemoryWAL() {
        this(DEFAULT_CAPACITY);
    }

    public InMemoryWAL(int capacity) {
        // Ensure power of 2 for efficient ring buffer
        this.capacity = Integer.highestOneBit(capacity) == capacity ? capacity : Integer.highestOneBit(capacity) * 2;
        this.mask = this.capacity - 1;
        this.buffer = new TransactionEntry[this.capacity];

        // Initialize buffer entries
        for (int i = 0; i < this.capacity; i++) {
            buffer[i] = new TransactionEntry();
        }

        log.info("Initialized InMemoryWAL with capacity {}", this.capacity);
    }

    /**
     * Append operation to in-memory WAL (lock-free, ultra-fast)
     */
    public long append(UUID accountId, long delta, UUID operationId) {
        long sequence = writePosition.getAndIncrement();
        int index = (int) (sequence & mask);

        // Check for buffer overflow
        if (sequence - flushedPosition.get() >= capacity) {
            throw new IllegalStateException("InMemoryWAL buffer overflow. Increase flush frequency.");
        }

        TransactionEntry entry = buffer[index];
        entry.setTimestamp(System.currentTimeMillis());
        entry.setAccountId(accountId);
        entry.setDelta(delta);
        entry.setOperationId(operationId);
        entry.setSequenceNumber(sequence);
        entry.setChecksum(entry.calculateChecksum());

        totalOperations.incrementAndGet();
        unflushedOperations.incrementAndGet();

        return sequence;
    }

    /**
     * Get unflushed entries for batch writing to disk
     */
    public List<TransactionEntry> getUnflushedEntries(int maxEntries) {
        long currentWrite = writePosition.get();
        long currentFlushed = flushedPosition.get();

        int entriesToFlush = (int) Math.min(maxEntries, currentWrite - currentFlushed);
        if (entriesToFlush <= 0) {
            return Collections.emptyList();
        }

        List<TransactionEntry> entries = new ArrayList<>(entriesToFlush);

        for (int i = 0; i < entriesToFlush; i++) {
            int index = (int) ((currentFlushed + i) & mask);
            TransactionEntry entry = buffer[index];

            // Create copy to avoid concurrent modification
            entries.add(TransactionEntry.builder()
                .timestamp(entry.getTimestamp())
                .accountId(entry.getAccountId())
                .delta(entry.getDelta())
                .operationId(entry.getOperationId())
                .sequenceNumber(entry.getSequenceNumber())
                .checksum(entry.getChecksum())
                .build());
        }

        return entries;
    }

    /**
     * Mark entries as flushed to disk
     */
    public void markFlushed(List<TransactionEntry> entries) {
        if (entries.isEmpty()) return;

        long maxSequence = entries.stream()
            .mapToLong(TransactionEntry::getSequenceNumber)
            .max()
            .orElse(flushedPosition.get());

        flushedPosition.updateAndGet(current -> Math.max(current, maxSequence + 1));
        unflushedOperations.addAndGet(-entries.size());
        totalFlushes.incrementAndGet();

        log.debug("Marked {} entries as flushed, new flushed position: {}",
            entries.size(), flushedPosition.get());
    }

    /**
     * Get entries for recovery (all entries after specific sequence)
     */
    public List<TransactionEntry> getEntriesAfterSequence(long afterSequence) {
        long currentWrite = writePosition.get();
        List<TransactionEntry> recoveryEntries = new ArrayList<>();

        for (long seq = Math.max(afterSequence + 1, flushedPosition.get()); seq < currentWrite; seq++) {
            int index = (int) (seq & mask);
            TransactionEntry entry = buffer[index];

            if (entry.getSequenceNumber() == seq) {
                recoveryEntries.add(TransactionEntry.builder()
                    .timestamp(entry.getTimestamp())
                    .accountId(entry.getAccountId())
                    .delta(entry.getDelta())
                    .operationId(entry.getOperationId())
                    .sequenceNumber(entry.getSequenceNumber())
                    .checksum(entry.getChecksum())
                    .build());
            }
        }

        return recoveryEntries;
    }

    public Map<String, Object> getStatistics() {
        return Map.of(
            "capacity", capacity,
            "totalOperations", totalOperations.get(),
            "totalFlushes", totalFlushes.get(),
            "unflushedOperations", unflushedOperations.get(),
            "utilizationPercent", (double) unflushedOperations.get() / capacity * 100.0,
            "writePosition", writePosition.get(),
            "flushedPosition", flushedPosition.get()
        );
    }
}
