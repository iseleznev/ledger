package org.seleznyov.iyu.kfin.ledger.application.service.containeroptimized;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Container-Optimized Balance Storage
 * Uses simple sharding with ConcurrentHashMap for AWS/Kubernetes environments
 */
@Component
@Slf4j
public class ContainerOptimizedBalanceStorage implements AutoCloseable {

    // Configuration
    private final int workerCount;
    private final int shardMask;

    // Storage - using ConcurrentHashMap for thread safety
    private final ConcurrentHashMap<UUID, AtomicLong>[] shardedBalances;
    private final ExecutorService[] workerExecutors;

    // Performance tracking
    private final AtomicLong[] operationCounts;
    private final AtomicLong totalAccounts = new AtomicLong(0);

    @SuppressWarnings("unchecked")
    public ContainerOptimizedBalanceStorage() {
        // Use available processors (respects container limits)
        this.workerCount = getOptimalWorkerCount();
        this.shardMask = workerCount - 1;

        // Initialize sharded storage
        this.shardedBalances = new ConcurrentHashMap[workerCount];
        this.workerExecutors = new ExecutorService[workerCount];
        this.operationCounts = new AtomicLong[workerCount];

        initializeShards();

        log.info("Initialized ContainerOptimizedBalanceStorage: {} workers, {} shards",
            workerCount, workerCount);
    }

    private int getOptimalWorkerCount() {
        int available = Runtime.getRuntime().availableProcessors();

        // Ensure power of 2 for efficient bit masking
        int powerOfTwo = 1;
        while (powerOfTwo < available) {
            powerOfTwo <<= 1;
        }

        // Use actual available or next lower power of 2
        return available == powerOfTwo ? available : powerOfTwo >> 1;
    }

    private void initializeShards() {
        for (int shard = 0; shard < workerCount; shard++) {
            // Each shard gets its own ConcurrentHashMap
            shardedBalances[shard] = new ConcurrentHashMap<>(100_000);

            // Each shard gets its own executor
            workerExecutors[shard] = Executors.newFixedThreadPool(2, // 2 threads per shard
                Thread.ofVirtual().name("worker-" + shard + "-").factory());

            operationCounts[shard] = new AtomicLong(0);
        }
    }

    /**
     * Simple hash-based sharding without core affinity
     */
    private int getShardId(UUID accountId) {
        return accountId.hashCode() & shardMask;
    }

    /**
     * Get account balance - high performance
     */
    public CompletableFuture<OptionalLong> getBalance(UUID accountId) {
        int shardId = getShardId(accountId);

        return CompletableFuture.supplyAsync(() -> {
            AtomicLong balance = shardedBalances[shardId].get(accountId);
            operationCounts[shardId].incrementAndGet();

            return balance != null ? OptionalLong.of(balance.get()) : OptionalLong.empty();
        }, workerExecutors[shardId]);
    }

    /**
     * Update account balance
     */
    public CompletableFuture<Void> updateBalance(UUID accountId, long newBalance) {
        int shardId = getShardId(accountId);

        return CompletableFuture.runAsync(() -> {
            AtomicLong balance = shardedBalances[shardId].computeIfAbsent(accountId,
                k -> {
                    totalAccounts.incrementAndGet();
                    return new AtomicLong(0);
                });

            balance.set(newBalance);
            operationCounts[shardId].incrementAndGet();
        }, workerExecutors[shardId]);
    }

    /**
     * Add delta to balance (optimized for frequent updates)
     */
    public CompletableFuture<Long> addToBalance(UUID accountId, long delta) {
        int shardId = getShardId(accountId);

        return CompletableFuture.supplyAsync(() -> {
            AtomicLong balance = shardedBalances[shardId].computeIfAbsent(accountId,
                k -> {
                    totalAccounts.incrementAndGet();
                    return new AtomicLong(0);
                });

            long newBalance = balance.addAndGet(delta);
            operationCounts[shardId].incrementAndGet();

            return newBalance;
        }, workerExecutors[shardId]);
    }

    /**
     * Batch balance read with optimal parallelization
     */
    public CompletableFuture<Map<UUID, Long>> getBalances(Set<UUID> accountIds) {
        if (accountIds.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }

        // Group accounts by shard
        Map<Integer, List<UUID>> accountsByShard = accountIds.stream()
            .collect(Collectors.groupingBy(this::getShardId));

        // Process each shard in parallel
        List<CompletableFuture<Map<UUID, Long>>> shardFutures = accountsByShard.entrySet()
            .stream()
            .map(entry -> {
                int shardId = entry.getKey();
                List<UUID> shardAccounts = entry.getValue();

                return CompletableFuture.supplyAsync(() -> {
                    Map<UUID, Long> shardResults = new HashMap<>();

                    for (UUID accountId : shardAccounts) {
                        AtomicLong balance = shardedBalances[shardId].get(accountId);
                        if (balance != null) {
                            shardResults.put(accountId, balance.get());
                        }
                    }

                    operationCounts[shardId].addAndGet(shardAccounts.size());
                    return shardResults;
                }, workerExecutors[shardId]);
            })
            .collect(Collectors.toList());

        // Combine results from all shards
        return CompletableFuture.allOf(shardFutures.toArray(new CompletableFuture[0]))
            .thenApply(ignored -> {
                Map<UUID, Long> allResults = new HashMap<>();
                shardFutures.forEach(future -> allResults.putAll(future.join()));
                return allResults;
            });
    }

    /**
     * Create system snapshot
     */
    public CompletableFuture<Map<UUID, Long>> createSnapshot() {
        List<CompletableFuture<Map<UUID, Long>>> shardFutures = new ArrayList<>();

        for (int shardId = 0; shardId < workerCount; shardId++) {
            final int currentShard = shardId;

            CompletableFuture<Map<UUID, Long>> shardFuture = CompletableFuture.supplyAsync(() -> {
                Map<UUID, Long> shardSnapshot = new HashMap<>();

                shardedBalances[currentShard].forEach((accountId, balance) -> {
                    shardSnapshot.put(accountId, balance.get());
                });

                return shardSnapshot;
            }, workerExecutors[currentShard]);

            shardFutures.add(shardFuture);
        }

        return CompletableFuture.allOf(shardFutures.toArray(new CompletableFuture[0]))
            .thenApply(ignored -> {
                Map<UUID, Long> fullSnapshot = new HashMap<>();
                shardFutures.forEach(future -> fullSnapshot.putAll(future.join()));

                log.info("Created snapshot with {} accounts across {} shards",
                    fullSnapshot.size(), workerCount);
                return fullSnapshot;
            });
    }

    /**
     * Restore from snapshot
     */
    public CompletableFuture<Integer> restoreFromSnapshot(Map<UUID, Long> snapshot) {
        // Group balances by shard
        Map<Integer, Map<UUID, Long>> balancesByShard = snapshot.entrySet().stream()
            .collect(Collectors.groupingBy(
                entry -> getShardId(entry.getKey()),
                Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)
            ));

        List<CompletableFuture<Integer>> restoreFutures = balancesByShard.entrySet()
            .stream()
            .map(entry -> {
                int shardId = entry.getKey();
                Map<UUID, Long> shardBalances = entry.getValue();

                return CompletableFuture.supplyAsync(() -> {
                    // Clear shard
                    shardedBalances[shardId].clear();

                    // Restore balances
                    shardBalances.forEach((accountId, balance) -> {
                        shardedBalances[shardId].put(accountId, new AtomicLong(balance));
                    });

                    return shardBalances.size();
                }, workerExecutors[shardId]);
            })
            .collect(Collectors.toList());

        return CompletableFuture.allOf(restoreFutures.toArray(new CompletableFuture[0]))
            .thenApply(ignored -> {
                int totalRestored = restoreFutures.stream()
                    .mapToInt(CompletableFuture::join)
                    .sum();

                totalAccounts.set(totalRestored);

                log.info("Restored {} accounts across {} shards", totalRestored, workerCount);
                return totalRestored;
            });
    }

    /**
     * System statistics
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new HashMap<>();

        stats.put("workerCount", workerCount);
        stats.put("totalAccounts", totalAccounts.get());

        // Aggregate operations
        long totalOps = Arrays.stream(operationCounts)
            .mapToLong(AtomicLong::get)
            .sum();
        stats.put("totalOperations", totalOps);

        // Per-shard statistics
        Map<String, Object> shardStats = new HashMap<>();
        for (int i = 0; i < workerCount; i++) {
            Map<String, Object> shardStat = new HashMap<>();
            shardStat.put("accounts", shardedBalances[i].size());
            shardStat.put("operations", operationCounts[i].get());

            shardStats.put("shard" + i, shardStat);
        }
        stats.put("shardStatistics", shardStats);

        return stats;
    }

    public boolean isHealthy() {
        return totalAccounts.get() >= 0;
    }

    @Override
    public void close() {
        log.info("Closing ContainerOptimizedBalanceStorage...");

        for (ExecutorService executor : workerExecutors) {
            if (executor != null) {
                executor.shutdown();
            }
        }

        log.info("ContainerOptimizedBalanceStorage closed. Final stats: {}", getStatistics());
    }
}

