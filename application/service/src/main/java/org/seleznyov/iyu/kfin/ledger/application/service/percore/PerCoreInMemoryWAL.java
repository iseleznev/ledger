package org.seleznyov.iyu.kfin.ledger.application.service.percore;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ExecutorService;

/**
 * Per-Core InMemory WAL - согласован с PhysicalCoreBalanceStorage архитектурой
 * Каждое физическое ядро имеет свой WAL buffer без atomic операций
 */
@Slf4j
@Component
public class PerCoreInMemoryWAL {

    private static final int BUFFER_SIZE_PER_CORE = 10_000; // 10K operations per core

    private final int physicalCores;
    private final int coreMask;

    // Per-core buffers - no synchronization needed within core
    private final TransactionEntry[][] coreBuffers; // [coreId][entryIndex]
    private final long[] writePositions; // Simple int, no atomic needed
    private final long[] flushedPositions; // Simple int, no atomic needed

    // Reuse executors from PhysicalCoreBalanceStorage for consistency
    private final ExecutorService[] coreExecutors;

    // Statistics per core
    private final long[] totalOperations;
    private final long[] totalFlushes;

    public PerCoreInMemoryWAL(ExecutorService[] coreExecutors) {
        this.physicalCores = coreExecutors.length;
        this.coreMask = physicalCores - 1;
        this.coreExecutors = coreExecutors;

        // Initialize per-core structures
        this.coreBuffers = new TransactionEntry[physicalCores][BUFFER_SIZE_PER_CORE];
        this.writePositions = new long[physicalCores];
        this.flushedPositions = new long[physicalCores];
        this.totalOperations = new long[physicalCores];
        this.totalFlushes = new long[physicalCores];

        // Initialize buffer entries
        for (int coreId = 0; coreId < physicalCores; coreId++) {
            for (int i = 0; i < BUFFER_SIZE_PER_CORE; i++) {
                coreBuffers[coreId][i] = new TransactionEntry();
            }
        }

        log.info("Initialized PerCoreInMemoryWAL with {} cores, {}K entries per core",
            physicalCores, BUFFER_SIZE_PER_CORE / 1000);
    }

    /**
     * UUID to physical core mapping (same as PhysicalCoreBalanceStorage)
     */
    private int getPhysicalCoreId(UUID accountId) {
        long high = accountId.getMostSignificantBits();
        long low = accountId.getLeastSignificantBits();
        int hash = (int) (high ^ low ^ (high >>> 32) ^ (low >>> 32));
        return hash & coreMask;
    }

    /**
     * Append operation to specific core WAL (no atomic operations)
     */
    public void append(UUID accountId, long delta, UUID operationId) {
        int coreId = getPhysicalCoreId(accountId);

        // Execute on specific core - single thread, no contention
        coreExecutors[coreId].submit(() -> {
            long position = writePositions[coreId];
            int bufferIndex = (int)(position % BUFFER_SIZE_PER_CORE);

            // Check for buffer overflow within this core
            if (position - flushedPositions[coreId] >= BUFFER_SIZE_PER_CORE) {
                throw new IllegalStateException(
                    "Core " + coreId + " WAL buffer overflow. Increase flush frequency.");
            }

            TransactionEntry entry = coreBuffers[coreId][bufferIndex];
            entry.setTimestamp(System.currentTimeMillis());
            entry.setAccountId(accountId);
            entry.setDelta(delta);
            entry.setOperationId(operationId);
            entry.setSequenceNumber(position); // Simple incrementing counter
            entry.setChecksum(entry.calculateChecksum());

            writePositions[coreId]++; // Simple increment, no atomic needed
            totalOperations[coreId]++;
        });
    }

    /**
     * Get unflushed entries from specific core for batch writing
     */
    public void getUnflushedEntries(int coreId, int maxEntries, UnflushedEntriesCallback callback) {
        if (coreId < 0 || coreId >= physicalCores) {
            callback.onResult(Collections.emptyList());
            return;
        }

        coreExecutors[coreId].submit(() -> {
            long currentWrite = writePositions[coreId];
            long currentFlushed = flushedPositions[coreId];

            int entriesToFlush = Math.min(maxEntries, (int)(currentWrite - currentFlushed));
            if (entriesToFlush <= 0) {
                callback.onResult(Collections.emptyList());
                return;
            }

            List<TransactionEntry> entries = new ArrayList<>(entriesToFlush);

            for (int i = 0; i < entriesToFlush; i++) {
                long position = currentFlushed + i;
                int bufferIndex = (int) (position % BUFFER_SIZE_PER_CORE);
                TransactionEntry entry = coreBuffers[coreId][bufferIndex];

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

            callback.onResult(entries);
        });
    }

    /**
     * Mark entries as flushed for specific core
     */
    public void markFlushed(int coreId, List<TransactionEntry> entries) {
        if (coreId < 0 || coreId >= physicalCores || entries.isEmpty()) {
            return;
        }

        coreExecutors[coreId].submit(() -> {
            long maxSequence = entries.stream()
                .mapToLong(TransactionEntry::getSequenceNumber)
                .max()
                .orElse(flushedPositions[coreId] - 1);

            flushedPositions[coreId] = Math.max(flushedPositions[coreId], maxSequence + 1);
            totalFlushes[coreId]++;

            log.debug("Core {}: marked {} entries as flushed, new flushed position: {}",
                coreId, entries.size(), flushedPositions[coreId]);
        });
    }

    /**
     * Get all unflushed entries from all cores for recovery
     */
    public void getAllUnflushedEntries(AllUnflushedEntriesCallback callback) {
        List<List<TransactionEntry>> allCoreEntries = new ArrayList<>(physicalCores);

        // Initialize result list
        for (int i = 0; i < physicalCores; i++) {
            allCoreEntries.add(null);
        }

        // Counter to track completion
        int[] completedCores = {0};

        for (int coreId = 0; coreId < physicalCores; coreId++) {
            final int currentCore = coreId;

            getUnflushedEntries(coreId, Integer.MAX_VALUE, entries -> {
                synchronized (allCoreEntries) {
                    allCoreEntries.set(currentCore, entries);
                    completedCores[0]++;

                    // Check if all cores completed
                    if (completedCores[0] == physicalCores) {
                        // Flatten and sort by timestamp for recovery ordering
                        List<TransactionEntry> allEntries = allCoreEntries.stream()
                            .flatMap(List::stream)
                            .sorted(Comparator.comparing(TransactionEntry::getTimestamp))
                            .toList();

                        callback.onResult(allEntries);
                    }
                }
            });
        }
    }

    /**
     * Get entries from specific core after certain sequence number
     */
    public void getCoreEntriesAfterSequence(int coreId, int afterSequence,
                                            UnflushedEntriesCallback callback) {
        if (coreId < 0 || coreId >= physicalCores) {
            callback.onResult(Collections.emptyList());
            return;
        }

        coreExecutors[coreId].submit(() -> {
            long currentWrite = writePositions[coreId];
            List<TransactionEntry> recoveryEntries = new ArrayList<>();

            for (long seq = Math.max(afterSequence + 1, flushedPositions[coreId]); seq < currentWrite; seq++) {
                int bufferIndex = (int)(seq % BUFFER_SIZE_PER_CORE);
                TransactionEntry entry = coreBuffers[coreId][bufferIndex];

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

            callback.onResult(recoveryEntries);
        });
    }

    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new HashMap<>();

        stats.put("physicalCores", physicalCores);
        stats.put("bufferSizePerCore", BUFFER_SIZE_PER_CORE);

        // Aggregate statistics
        long totalOps = Arrays.stream(totalOperations).sum();
        long totalFlushCount = Arrays.stream(totalFlushes).sum();
        int totalUnflushed = 0;

        for (int i = 0; i < physicalCores; i++) {
            totalUnflushed += writePositions[i] - flushedPositions[i];
        }

        stats.put("totalOperations", totalOps);
        stats.put("totalFlushes", totalFlushCount);
        stats.put("totalUnflushed", totalUnflushed);

        // Per-core statistics
        Map<String, Object> coreStats = new HashMap<>();
        for (int i = 0; i < physicalCores; i++) {
            Map<String, Object> coreStat = new HashMap<>();
            coreStat.put("operations", totalOperations[i]);
            coreStat.put("flushes", totalFlushes[i]);
            coreStat.put("unflushed", writePositions[i] - flushedPositions[i]);
            coreStat.put("utilizationPercent",
                (double) (writePositions[i] - flushedPositions[i]) / BUFFER_SIZE_PER_CORE * 100.0);
            coreStats.put("core" + i, coreStat);
        }
        stats.put("coreStatistics", coreStats);

        return stats;
    }

    // Callback interfaces для async operations
    @FunctionalInterface
    public interface UnflushedEntriesCallback {
        void onResult(List<TransactionEntry> entries);
    }

    @FunctionalInterface
    public interface AllUnflushedEntriesCallback {
        void onResult(List<TransactionEntry> entries);
    }
}