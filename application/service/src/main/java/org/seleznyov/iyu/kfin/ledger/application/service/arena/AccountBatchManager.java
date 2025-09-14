package org.seleznyov.iyu.kfin.ledger.application.service.arena;

// Required imports for Foreign Function & Memory API
import java.lang.foreign.*;
import static java.lang.foreign.ValueLayout.*;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

import lombok.*;
import lombok.extern.slf4j.Slf4j;

// Optimized Account-specific batch manager - single thread per account
@Slf4j
@Getter
@RequiredArgsConstructor
public class AccountBatchManager {

    private final UUID accountId;
    private final int maxBatchSize;
    private final int snapshotEveryNOperations;
    private final Arena batchArena;

    // NO ATOMICS - single thread per account (confined Arena)
    private int currentBatchSize = 0;
    private long lastSnapshotOrdinal = 0;
    private int operationsSinceSnapshot = 0;

    // For balance calculation - hot accounts specific
    private final ConcurrentLinkedDeque<EntryRecord> entriesSinceSnapshot = new ConcurrentLinkedDeque<>();
    private int entriesPointer = 0; // Simple int instead of AtomicInteger

    private volatile MemorySegment currentBatch; // volatile for visibility
    private volatile boolean batchReady = false; // volatile for visibility

    public AccountBatchManager(UUID accountId, int maxBatchSize, int snapshotEveryNOperations) {
        this.accountId = accountId;
        this.maxBatchSize = maxBatchSize;
        this.snapshotEveryNOperations = snapshotEveryNOperations;
        this.batchArena = Arena.ofConfined(); // Single thread access
        allocateNewBatch();
    }

    private void allocateNewBatch() {
        currentBatch = batchArena.allocate(LedgerMemoryLayouts.ENTRY_SIZE * maxBatchSize);
        currentBatchSize = 0;
        batchReady = false;
        // Clear entries list for new batch
        entriesSinceSnapshot.clear();
        entriesPointer = 0;
        log.debug("Allocated new batch for account {} with size {}", accountId, maxBatchSize);
    }

    // NO CompletableFuture - synchronous since single thread per account
    public boolean addEntry(EntryRecord entry) {
        // NO atomic operations - single thread guarantee
        if (currentBatchSize >= maxBatchSize) {
            return false; // Batch full
        }

        int position = currentBatchSize++;
        writeEntryToMemory(currentBatch, position, entry);

        // Add entry to our tracking list for balance calculation
        entriesSinceSnapshot.addLast(entry);

        // Check if batch is ready
        if (currentBatchSize == maxBatchSize) {
            batchReady = true;
            log.debug("Batch ready for account {} with {} entries", accountId, maxBatchSize);
        }

        return true;
    }

    private void writeEntryToMemory(MemorySegment batch, int position, EntryRecord entry) {
        long offset = position * LedgerMemoryLayouts.ENTRY_SIZE;

        // Write UUID as bytes
        writeUuidToMemory(batch, offset, entry.getId());
        writeUuidToMemory(batch, offset + 16, entry.getAccountId());
        writeUuidToMemory(batch, offset + 32, entry.getTransactionId());

        // Write primitive fields
        batch.set(JAVA_BYTE, offset + 48, (byte) entry.getEntryType().ordinal());
        batch.set(JAVA_LONG, offset + 49, entry.getAmount());
        batch.set(JAVA_LONG, offset + 57, entry.getCreatedAt().toEpochMilli());
        batch.set(JAVA_INT, offset + 65, (int) entry.getOperationDate().toEpochDay());

        writeUuidToMemory(batch, offset + 69, entry.getIdempotencyKey());
        writeCurrencyCodeToMemory(batch, offset + 85, entry.getCurrencyCode());
        batch.set(JAVA_LONG, offset + 93, entry.getEntryOrdinal());
    }

    private void writeUuidToMemory(MemorySegment segment, long offset, UUID uuid) {
        long mostSigBits = uuid.getMostSignificantBits();
        long leastSigBits = uuid.getLeastSignificantBits();
        segment.set(JAVA_LONG, offset, mostSigBits);
        segment.set(JAVA_LONG, offset + 8, leastSigBits);
    }

    private void writeCurrencyCodeToMemory(MemorySegment segment, long offset, String currencyCode) {
        byte[] bytes = currencyCode.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < Math.min(8, bytes.length); i++) {
            segment.set(JAVA_BYTE, offset + i, bytes[i]);
        }
    }

    public boolean isBatchReady() {
        return batchReady;
    }

    public BatchData extractBatch() {
        if (!batchReady) {
            return null;
        }

        int size = currentBatchSize;
        byte[] batchData = new byte[(int) (size * LedgerMemoryLayouts.ENTRY_SIZE)];
        MemorySegment.copy(currentBatch, 0, MemorySegment.ofArray(batchData), 0, batchData.length);

        // Create batch with current entries
        List<EntryRecord> currentEntries = new ArrayList<>(entriesSinceSnapshot);
        BatchData batch = new BatchData(accountId, batchData, size, currentEntries);

        // Reset pointer to entries that didn't make it to this batch
        int processedEntries = Math.min(currentEntries.size(), size);
        entriesPointer = Math.max(0, entriesPointer - processedEntries);

        allocateNewBatch();
        return batch;
    }

    // Get balance for hot account: snapshot + entries since snapshot starting from pointer position
    public long getCurrentBalance(AccountSnapshot lastSnapshot) {
        long balance = lastSnapshot != null ? lastSnapshot.getBalance() : 0L;

        int pointer = entriesPointer; // NO atomic read
        List<EntryRecord> entries = new ArrayList<>(entriesSinceSnapshot);

        // Apply entries starting from pointer position
        for (int i = pointer; i < entries.size(); i++) {
            EntryRecord entry = entries.get(i);
            if (entry.getEntryType() == EntryRecord.EntryType.DEBIT) {
                balance -= entry.getAmount();
            } else {
                balance += entry.getAmount();
            }
        }

        return balance;
    }

    // Called when batch is successfully inserted into ring buffer
    public void onBatchQueued(int entriesInBatch) {
        // Simple increment - no atomic needed
        entriesPointer += entriesInBatch;
    }

    public void cleanup() {
        try {
            batchArena.close();
        } catch (Exception e) {
            log.error("Error closing batch arena for account {}", accountId, e);
        }
    }
}

