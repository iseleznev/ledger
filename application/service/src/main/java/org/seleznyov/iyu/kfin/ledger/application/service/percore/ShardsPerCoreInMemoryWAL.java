package org.seleznyov.iyu.kfin.ledger.application.service.percore;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * High-Throughput PerCore InMemory WAL - compatible with multi-threaded core architecture
 * Uses sub-shard level parallelization for maximum throughput
 */
@Slf4j
@Component
public class ShardsPerCoreInMemoryWAL {

    private static final int BUFFER_SIZE_PER_SUB_SHARD = 2_500; // 2.5K operations per sub-shard
    private static final int SUB_SHARDS_PER_CORE = 4;

    private final int physicalCores;
    private final int coreMask;
    private final int subShardMask = SUB_SHARDS_PER_CORE - 1;

    // Per sub-shard buffers for lock-free operations within sub-shard
    private final ConcurrentLinkedQueue<TransactionEntry>[][] coreSubShardBuffers; // [coreId][subShardId]
    private final AtomicLong[][] writePositions; // Changed to AtomicLong for thread safety
    private final AtomicLong[][] flushedPositions; // Changed to AtomicLong for thread safety

    // Use executors from HighThroughputBalanceStorage
    private final ExecutorService[][] coreSubShardExecutors;

    // Statistics per sub-shard
    private final AtomicLong[][] totalOperations;
    private final AtomicLong[][] totalFlushes;

    @SuppressWarnings("unchecked")
    public ShardsPerCoreInMemoryWAL(ExecutorService[][] coreSubShardExecutors) {
        this.coreSubShardExecutors = coreSubShardExecutors;
        this.physicalCores = coreSubShardExecutors.length;
        this.coreMask = physicalCores - 1;

        // Initialize per sub-shard structures
        this.coreSubShardBuffers = new ConcurrentLinkedQueue[physicalCores][SUB_SHARDS_PER_CORE];
        this.writePositions = new AtomicLong[physicalCores][SUB_SHARDS_PER_CORE];
        this.flushedPositions = new AtomicLong[physicalCores][SUB_SHARDS_PER_CORE];
        this.totalOperations = new AtomicLong[physicalCores][SUB_SHARDS_PER_CORE];
        this.totalFlushes = new AtomicLong[physicalCores][SUB_SHARDS_PER_CORE];

        // Initialize all structures
        for (int coreId = 0; coreId < physicalCores; coreId++) {
            for (int subShardId = 0; subShardId < SUB_SHARDS_PER_CORE; subShardId++) {
                coreSubShardBuffers[coreId][subShardId] = new ConcurrentLinkedQueue<>();
                writePositions[coreId][subShardId] = new AtomicLong(0);
                flushedPositions[coreId][subShardId] = new AtomicLong(0);
                totalOperations[coreId][subShardId] = new AtomicLong(0);
                totalFlushes[coreId][subShardId] = new AtomicLong(0);
            }
        }

        log.info("Initialized HighThroughput PerCoreInMemoryWAL: {} cores × {} sub-shards = {} total WAL buffers",
            physicalCores, SUB_SHARDS_PER_CORE, physicalCores * SUB_SHARDS_PER_CORE);
    }

    /**
     * Account location calculation matching HighThroughputBalanceStorage
     */
    private CoreShardLocation getAccountLocation(UUID accountId) {
        long high = accountId.getMostSignificantBits();
        long low = accountId.getLeastSignificantBits();
        int hash = (int) (high ^ low ^ (high >>> 32) ^ (low >>> 32));

        int coreId = hash & coreMask;
        int subShardId = (hash >>> 8) & subShardMask;

        return new CoreShardLocation(coreId, subShardId);
    }

    /**
     * Append operation to specific sub-shard WAL (thread-safe)
     */
    public void append(UUID accountId, long delta, UUID operationId) {
        CoreShardLocation location = getAccountLocation(accountId);

        // Submit to appropriate executor for this sub-shard
        coreSubShardExecutors[location.coreId][location.subShardId].submit(() -> {
            long position = writePositions[location.coreId][location.subShardId].getAndIncrement();

            // Check for buffer overflow within this sub-shard
            ConcurrentLinkedQueue<TransactionEntry> buffer =
                coreSubShardBuffers[location.coreId][location.subShardId];

            if (buffer.size() >= BUFFER_SIZE_PER_SUB_SHARD) {
                throw new IllegalStateException(
                    String.format("Sub-shard WAL buffer overflow: core %d, sub-shard %d. Size: %d",
                        location.coreId, location.subShardId, buffer.size()));
            }

            TransactionEntry entry = TransactionEntry.builder()
                .timestamp(System.currentTimeMillis())
                .accountId(accountId)
                .delta(delta)
                .operationId(operationId)
                .sequenceNumber(position)
                .checksum(0) // Will be calculated
                .build();

            entry.setChecksum(entry.calculateChecksum());

            buffer.offer(entry);
            totalOperations[location.coreId][location.subShardId].incrementAndGet();
        });
    }

    /**
     * Get unflushed entries from specific sub-shard
     */
    public void getUnflushedEntries(int coreId, int subShardId, int maxEntries,
                                    UnflushedEntriesCallback callback) {
        if (coreId < 0 || coreId >= physicalCores || subShardId < 0 || subShardId >= SUB_SHARDS_PER_CORE) {
            callback.onResult(Collections.emptyList());
            return;
        }

        coreSubShardExecutors[coreId][subShardId].submit(() -> {
            ConcurrentLinkedQueue<TransactionEntry> buffer = coreSubShardBuffers[coreId][subShardId];
            List<TransactionEntry> entries = new ArrayList<>();

            int collected = 0;
            while (collected < maxEntries && !buffer.isEmpty()) {
                TransactionEntry entry = buffer.peek();
                if (entry != null && entry.getSequenceNumber() >= flushedPositions[coreId][subShardId].get()) {
                    entries.add(entry);
                    collected++;
                }
                if (buffer.size() <= collected) break;
            }

            callback.onResult(entries);
        });
    }

    /**
     * Mark entries as flushed for specific sub-shard
     */
    public void markFlushed(int coreId, int subShardId, List<TransactionEntry> entries) {
        if (coreId < 0 || coreId >= physicalCores || subShardId < 0 || subShardId >= SUB_SHARDS_PER_CORE || entries.isEmpty()) {
            return;
        }

        coreSubShardExecutors[coreId][subShardId].submit(() -> {
            long maxSequence = entries.stream()
                .mapToLong(TransactionEntry::getSequenceNumber)
                .max()
                .orElse(flushedPositions[coreId][subShardId].get() - 1);

            flushedPositions[coreId][subShardId].updateAndGet(current -> Math.max(current, maxSequence + 1));

            // Remove flushed entries from queue
            ConcurrentLinkedQueue<TransactionEntry> buffer = coreSubShardBuffers[coreId][subShardId];
            Set<Long> flushedSequences = entries.stream()
                .map(TransactionEntry::getSequenceNumber)
                .collect(Collectors.toSet());

            buffer.removeIf(entry -> flushedSequences.contains(entry.getSequenceNumber()));

            totalFlushes[coreId][subShardId].incrementAndGet();

            log.debug("Sub-shard {}-{}: marked {} entries as flushed, new flushed position: {}",
                coreId, subShardId, entries.size(), flushedPositions[coreId][subShardId].get());
        });
    }

    /**
     * Get all unflushed entries from all sub-shards for recovery
     */
    public void getAllUnflushedEntries(AllUnflushedEntriesCallback callback) {
        List<List<TransactionEntry>> allSubShardEntries = new ArrayList<>(physicalCores * SUB_SHARDS_PER_CORE);

        // Initialize result list
        for (int i = 0; i < physicalCores * SUB_SHARDS_PER_CORE; i++) {
            allSubShardEntries.add(null);
        }

        // Counter to track completion
        int[] completedSubShards = {0};
        int totalSubShards = physicalCores * SUB_SHARDS_PER_CORE;

        for (int coreId = 0; coreId < physicalCores; coreId++) {
            for (int subShardId = 0; subShardId < SUB_SHARDS_PER_CORE; subShardId++) {
                final int currentCore = coreId;
                final int currentSubShard = subShardId;
                final int linearIndex = coreId * SUB_SHARDS_PER_CORE + subShardId;

                getUnflushedEntries(coreId, subShardId, Integer.MAX_VALUE, entries -> {
                    synchronized (allSubShardEntries) {
                        allSubShardEntries.set(linearIndex, entries);
                        completedSubShards[0]++;

                        // Check if all sub-shards completed
                        if (completedSubShards[0] == totalSubShards) {
                            // Flatten and sort by timestamp for recovery ordering
                            List<TransactionEntry> allEntries = allSubShardEntries.stream()
                                .filter(Objects::nonNull)
                                .flatMap(List::stream)
                                .sorted(Comparator.comparing(TransactionEntry::getTimestamp))
                                .toList();

                            callback.onResult(allEntries);
                        }
                    }
                });
            }
        }
    }

    /**
     * Get entries from specific sub-shard after certain sequence number
     */
    public void getSubShardEntriesAfterSequence(int coreId, int subShardId, long afterSequence,
                                                UnflushedEntriesCallback callback) {
        if (coreId < 0 || coreId >= physicalCores || subShardId < 0 || subShardId >= SUB_SHARDS_PER_CORE) {
            callback.onResult(Collections.emptyList());
            return;
        }

        coreSubShardExecutors[coreId][subShardId].submit(() -> {
            ConcurrentLinkedQueue<TransactionEntry> buffer = coreSubShardBuffers[coreId][subShardId];
            List<TransactionEntry> recoveryEntries = new ArrayList<>();

            for (TransactionEntry entry : buffer) {
                if (entry.getSequenceNumber() > afterSequence) {
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
        stats.put("subShardsPerCore", SUB_SHARDS_PER_CORE);
        stats.put("bufferSizePerSubShard", BUFFER_SIZE_PER_SUB_SHARD);
        stats.put("totalSubShards", physicalCores * SUB_SHARDS_PER_CORE);

        // Aggregate statistics
        long totalOps = 0;
        long totalFlushCount = 0;
        int totalUnflushed = 0;

        for (int coreId = 0; coreId < physicalCores; coreId++) {
            for (int subShardId = 0; subShardId < SUB_SHARDS_PER_CORE; subShardId++) {
                totalOps += totalOperations[coreId][subShardId].get();
                totalFlushCount += totalFlushes[coreId][subShardId].get();
                totalUnflushed += coreSubShardBuffers[coreId][subShardId].size();
            }
        }

        stats.put("totalOperations", totalOps);
        stats.put("totalFlushes", totalFlushCount);
        stats.put("totalUnflushed", totalUnflushed);

        // Per-core statistics
        Map<String, Object> coreStats = new HashMap<>();
        for (int coreId = 0; coreId < physicalCores; coreId++) {
            Map<String, Object> coreStat = new HashMap<>();

            long coreOps = 0;
            long coreFlushes = 0;
            int coreUnflushed = 0;

            Map<String, Object> subShardStats = new HashMap<>();
            for (int subShardId = 0; subShardId < SUB_SHARDS_PER_CORE; subShardId++) {
                long subShardOps = totalOperations[coreId][subShardId].get();
                long subShardFlushes = totalFlushes[coreId][subShardId].get();
                int subShardUnflushed = coreSubShardBuffers[coreId][subShardId].size();

                coreOps += subShardOps;
                coreFlushes += subShardFlushes;
                coreUnflushed += subShardUnflushed;

                double utilizationPercent = (double) subShardUnflushed / BUFFER_SIZE_PER_SUB_SHARD * 100.0;

                subShardStats.put("subShard" + subShardId, Map.of(
                    "operations", subShardOps,
                    "flushes", subShardFlushes,
                    "unflushed", subShardUnflushed,
                    "utilizationPercent", utilizationPercent
                ));
            }

            coreStat.put("totalOperations", coreOps);
            coreStat.put("totalFlushes", coreFlushes);
            coreStat.put("totalUnflushed", coreUnflushed);
            coreStat.put("subShards", subShardStats);

            coreStats.put("core" + coreId, coreStat);
        }
        stats.put("coreStatistics", coreStats);

        return stats;
    }

    // Callback interfaces for async operations
    @FunctionalInterface
    public interface UnflushedEntriesCallback {
        void onResult(List<TransactionEntry> entries);
    }

    @FunctionalInterface
    public interface AllUnflushedEntriesCallback {
        void onResult(List<TransactionEntry> entries);
    }

    // Helper class for account location (matching HighThroughputBalanceStorage)
    private static class CoreShardLocation {
        final int coreId;
        final int subShardId;

        CoreShardLocation(int coreId, int subShardId) {
            this.coreId = coreId;
            this.subShardId = subShardId;
        }
    }
}