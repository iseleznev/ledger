package org.seleznyov.iyu.kfin.ledger.application.service.arenaseparated;

import lombok.extern.slf4j.Slf4j;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;

import static java.lang.foreign.ValueLayout.*;

// High-Performance Hot Account Implementation
@Slf4j
public class HighPerformanceHotAccountManager implements HotAccountManager {

    private final UUID accountId;
    private final int maxBatchSize;
    private final int snapshotEveryNOperations;
    private final Arena batchArena;

    // NO ATOMICS - maximum performance for single thread per account
    private int currentBatchSize = 0;
    private long lastSnapshotOrdinal = 0;
    private int operationsSinceSnapshot = 0;

    // Balance calculation tracking
    private final ConcurrentLinkedDeque<EntryRecord> entriesSinceSnapshot = new ConcurrentLinkedDeque<>();
    private int entriesPointer = 0;

    private volatile MemorySegment currentBatch;
    private volatile boolean batchReady = false;

    public HighPerformanceHotAccountManager(UUID accountId, int maxBatchSize, int snapshotEveryNOperations) {
        this.accountId = accountId;
        this.maxBatchSize = maxBatchSize;
        this.snapshotEveryNOperations = snapshotEveryNOperations;
        this.batchArena = Arena.ofConfined(); // Single thread guarantee
        allocateNewBatch();
        log.debug("Created HIGH-PERFORMANCE hot account manager for {} (batch_size={})",
            accountId, maxBatchSize);
    }

    @Override
    public UUID getAccountId() {
        return accountId;
    }

    @Override
    public boolean addEntry(EntryRecord entry) {
        // PURE SYNCHRONOUS - no CompletableFuture overhead
        if (currentBatchSize >= maxBatchSize) {
            log.warn("Batch full for hot account {}, rejecting entry", accountId);
            return false;
        }

        // Direct memory write - maximum performance
        int position = currentBatchSize++;
        writeEntryToMemory(currentBatch, position, entry);
        entriesSinceSnapshot.addLast(entry);
        operationsSinceSnapshot++;

        log.trace("Added entry to hot account {} at position {}", accountId, position);

        // Snapshot logic
        if (operationsSinceSnapshot >= snapshotEveryNOperations) {
            triggerSnapshotCreation(entry);
            operationsSinceSnapshot = 0;
        }

        // Batch ready check
        if (currentBatchSize == maxBatchSize) {
            batchReady = true;
            log.debug("Hot batch ready for account {} ({} entries)", accountId, maxBatchSize);
        }

        return true;
    }

    @Override
    public boolean isBatchReady() {
        return batchReady;
    }

    @Override
    public BatchData extractBatch() {
        if (!batchReady) {
            return null;
        }

        int size = currentBatchSize;
        byte[] batchData = new byte[(int) (size * LedgerMemoryLayouts.ENTRY_SIZE)];
        MemorySegment.copy(currentBatch, 0, MemorySegment.ofArray(batchData), 0, batchData.length);

        List<EntryRecord> currentEntries = new ArrayList<>(entriesSinceSnapshot);
        BatchData batch = new BatchData(accountId, batchData, size, currentEntries);

        log.debug("Extracted hot batch for account {} ({} entries)", accountId, size);

        // Reset for next batch
        int processedEntries = Math.min(currentEntries.size(), size);
        entriesPointer = Math.max(0, entriesPointer - processedEntries);
        allocateNewBatch();

        return batch;
    }

    @Override
    public void onBatchQueued(int entriesInBatch) {
        entriesPointer += entriesInBatch;
        log.debug("Hot account {} batch queued ({} entries), pointer now at {}",
            accountId, entriesInBatch, entriesPointer);
    }

    @Override
    public long getCurrentBalance(AccountSnapshot lastSnapshot) {
        long balance = lastSnapshot != null ? lastSnapshot.getBalance() : 0L;

        int pointer = entriesPointer;
        List<EntryRecord> entries = new ArrayList<>(entriesSinceSnapshot);

        // Apply unprocessed entries
        for (int i = pointer; i < entries.size(); i++) {
            EntryRecord entry = entries.get(i);
            balance += (entry.getEntryType() == EntryRecord.EntryType.DEBIT)
                ? -entry.getAmount()
                : entry.getAmount();
        }

        return balance;
    }

    @Override
    public boolean shouldCreateSnapshot() {
        return operationsSinceSnapshot == 0 && lastSnapshotOrdinal > 0;
    }

    @Override
    public long getSnapshotOrdinal() {
        return lastSnapshotOrdinal;
    }

    @Override
    public void onSnapshotCreated() {
        lastSnapshotOrdinal = 0;
        log.debug("Snapshot created for hot account {}", accountId);
    }

    private void triggerSnapshotCreation(EntryRecord triggerEntry) {
        lastSnapshotOrdinal = triggerEntry.getEntryOrdinal();
        log.debug("Triggering snapshot for hot account {} at ordinal {}",
            accountId, lastSnapshotOrdinal);
    }

    private void allocateNewBatch() {
        currentBatch = batchArena.allocate(LedgerMemoryLayouts.ENTRY_SIZE * maxBatchSize);
        currentBatchSize = 0;
        batchReady = false;
        entriesSinceSnapshot.clear();
        entriesPointer = 0;
        lastSnapshotOrdinal = 0;
    }

    private void writeEntryToMemory(MemorySegment batch, int position, EntryRecord entry) {
        long offset = position * LedgerMemoryLayouts.ENTRY_SIZE;

        writeUuidToMemory(batch, offset, entry.getId());
        writeUuidToMemory(batch, offset + 16, entry.getAccountId());
        writeUuidToMemory(batch, offset + 32, entry.getTransactionId());

        batch.set(JAVA_BYTE, offset + 48, (byte) entry.getEntryType().ordinal());
        batch.set(JAVA_LONG, offset + 49, entry.getAmount());
        batch.set(JAVA_LONG, offset + 57, entry.getCreatedAt().toEpochMilli());
        batch.set(JAVA_INT, offset + 65, (int) entry.getOperationDate().toEpochDay());

        writeUuidToMemory(batch, offset + 69, entry.getIdempotencyKey());
        writeCurrencyCodeToMemory(batch, offset + 85, entry.getCurrencyCode());
        batch.set(JAVA_LONG, offset + 93, entry.getEntryOrdinal());
    }

    private void writeUuidToMemory(MemorySegment segment, long offset, UUID uuid) {
        segment.set(JAVA_LONG, offset, uuid.getMostSignificantBits());
        segment.set(JAVA_LONG, offset + 8, uuid.getLeastSignificantBits());
    }

    private void writeCurrencyCodeToMemory(MemorySegment segment, long offset, String currencyCode) {
        byte[] bytes = currencyCode.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < 8; i++) {
            if (i < bytes.length) {
                segment.set(JAVA_BYTE, offset + i, bytes[i]);
            } else {
                segment.set(JAVA_BYTE, offset + i, (byte) 0);
            }
        }
    }

    @Override
    public void cleanup() {
        try {
            if (batchArena != null) {
                batchArena.close();
                log.debug("Closed arena for hot account {}", accountId);
            }
        } catch (Exception e) {
            log.error("Error closing arena for hot account {}", accountId, e);
        }
    }
}
