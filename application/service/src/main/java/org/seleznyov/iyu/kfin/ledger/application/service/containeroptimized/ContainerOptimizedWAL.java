package org.seleznyov.iyu.kfin.ledger.application.service.containeroptimized;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Container-Optimized WAL using simple sharding
 */
@Component
@Slf4j
public class ContainerOptimizedWAL {

    private final int shardCount;
    private final int shardMask;
    private final ConcurrentLinkedQueue<TransactionEntry>[] shardQueues;
    private final ExecutorService[] shardExecutors;
    private final AtomicLong[] operationCounts;

    private static final int QUEUE_SIZE_PER_SHARD = 10_000;

    @SuppressWarnings("unchecked")
    public ContainerOptimizedWAL() {
        this.shardCount = Runtime.getRuntime().availableProcessors();
        this.shardMask = shardCount - 1;

        this.shardQueues = new ConcurrentLinkedQueue[shardCount];
        this.shardExecutors = new ExecutorService[shardCount];
        this.operationCounts = new AtomicLong[shardCount];

        initializeShards();

        log.info("Initialized ContainerOptimizedWAL with {} shards", shardCount);
    }

    private void initializeShards() {
        for (int shard = 0; shard < shardCount; shard++) {
            shardQueues[shard] = new ConcurrentLinkedQueue<>();
            shardExecutors[shard] = Executors.newSingleThreadExecutor(
                Thread.ofVirtual().name("wal-shard-" + shard + "-").factory());
            operationCounts[shard] = new AtomicLong(0);
        }
    }

    private int getShardId(UUID accountId) {
        return accountId.hashCode() & shardMask;
    }

    public CompletableFuture<Void> append(UUID accountId, long delta, UUID operationId) {
        int shardId = getShardId(accountId);

        return CompletableFuture.runAsync(() -> {
                if (shardQueues[shardId].size() >= QUEUE_SIZE_PER_SHARD) {
                    throw new IllegalStateException("WAL shard " + shardId + " queue full");
                }

                TransactionEntry entry = TransactionEntry.builder()
                    .timestamp(System.currentTimeMillis())
                    .accountId(accountId)
                    .delta(delta)
                    .operationId(operationId)
                    .sequenceNumber(operationCounts[shardId].getAndIncrement())
                    .build();

                entry.setChecksum(entry.calculateChecksum());
                shardQueues[shardId].offer(entry);

            },
            shardExecutors[shardId]
        );
    }

    public CompletableFuture<List<TransactionEntry>>  getUnflushedEntries(int maxEntries) {
        List<CompletableFuture<List<TransactionEntry>>> shardFutures = new ArrayList<>();

        for (int shardId = 0; shardId < shardCount; shardId++) {
            final int currentShard = shardId;

            CompletableFuture<List<TransactionEntry>> shardFuture = CompletableFuture.supplyAsync(() -> {
                List<TransactionEntry> entries = new ArrayList<>();
                ConcurrentLinkedQueue<TransactionEntry> queue = shardQueues[currentShard];

                int collected = 0;
                int shardMax = maxEntries / shardCount;

                while (collected < shardMax && !queue.isEmpty()) {
                    TransactionEntry entry = queue.poll();
                    if (entry != null) {
                        entries.add(entry);
                        collected++;
                    }
                }

                return entries;
            }, shardExecutors[currentShard]);

            shardFutures.add(shardFuture);
        }

        return CompletableFuture.allOf(shardFutures.toArray(new CompletableFuture[0]))
            .thenApply(ignored -> {
                List<TransactionEntry> allEntries = shardFutures.stream()
                    .flatMap(future -> future.join().stream())
                    .sorted(Comparator.comparing(TransactionEntry::getTimestamp))
                    .collect(Collectors.toList());

                return allEntries;
            });
    }

    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("shardCount", shardCount);

        long totalOps = Arrays.stream(operationCounts).mapToLong(AtomicLong::get).sum();
        int totalQueued = Arrays.stream(shardQueues).mapToInt(ConcurrentLinkedQueue::size).sum();

        stats.put("totalOperations", totalOps);
        stats.put("totalQueued", totalQueued);

        return stats;
    }
}
