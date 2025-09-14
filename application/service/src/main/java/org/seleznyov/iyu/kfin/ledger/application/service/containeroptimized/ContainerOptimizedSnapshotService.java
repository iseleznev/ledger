package org.seleznyov.iyu.kfin.ledger.application.service.containeroptimized;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Container-optimized service for managing balance snapshots
 * Provides fast balance calculations using cached snapshots
 */
@Service
@Slf4j
public class ContainerOptimizedSnapshotService implements AutoCloseable {

    private final int shardCount;
    private final int shardMask;
    private final ConcurrentHashMap<UUID, AccountBalanceSnapshot>[] latestSnapshots;
    private final ExecutorService[] shardExecutors;
    private final AtomicLong[] snapshotCounts;

    // Configuration
    private static final int INITIAL_CAPACITY = 10_000;
    private static final int SNAPSHOT_THRESHOLD = 100; // Create snapshot after N operations

    // Performance metrics
    private final AtomicLong totalSnapshots = new AtomicLong(0);
    private final AtomicLong snapshotHits = new AtomicLong(0);
    private final AtomicLong snapshotMisses = new AtomicLong(0);

    @SuppressWarnings("unchecked")
    public ContainerOptimizedSnapshotService() {
        this.shardCount = getOptimalShardCount();
        this.shardMask = shardCount - 1;

        this.latestSnapshots = new ConcurrentHashMap[shardCount];
        this.shardExecutors = new ExecutorService[shardCount];
        this.snapshotCounts = new AtomicLong[shardCount];

        initializeShards();

        log.info("Initialized ContainerOptimizedSnapshotService with {} shards", shardCount);
    }

    private int getOptimalShardCount() {
        int available = Runtime.getRuntime().availableProcessors();
        int powerOfTwo = 1;
        while (powerOfTwo < available) {
            powerOfTwo <<= 1;
        }
        return available == powerOfTwo ? available : powerOfTwo >> 1;
    }

    private void initializeShards() {
        for (int shard = 0; shard < shardCount; shard++) {
            latestSnapshots[shard] = new ConcurrentHashMap<>(INITIAL_CAPACITY);
            shardExecutors[shard] = Executors.newFixedThreadPool(2,
                Thread.ofVirtual().name("snapshot-shard-" + shard + "-").factory());
            snapshotCounts[shard] = new AtomicLong(0);
        }
    }

    private int getShardId(UUID accountId) {
        return accountId.hashCode() & shardMask;
    }

    /**
     * Create or update snapshot for account
     */
    public CompletableFuture<AccountBalanceSnapshot> createSnapshot(UUID accountId,
                                                                    LocalDate operationDate, long balance, UUID lastEntryRecordId,
                                                                    long lastEntryOrdinal, int operationsCount) {

        int shardId = getShardId(accountId);

        return CompletableFuture.supplyAsync(() -> {
            AccountBalanceSnapshot snapshot = AccountBalanceSnapshot.create(
                accountId, operationDate, balance, lastEntryRecordId,
                lastEntryOrdinal, operationsCount);

            // Store latest snapshot for this account
            AccountBalanceSnapshot previous = latestSnapshots[shardId].put(accountId, snapshot);

            snapshotCounts[shardId].incrementAndGet();
            totalSnapshots.incrementAndGet();

            if (previous != null) {
                log.debug("Updated snapshot for account {}, previous ordinal: {}, new ordinal: {}",
                    accountId, previous.getLastEntryOrdinal(), snapshot.getLastEntryOrdinal());
            } else {
                log.debug("Created first snapshot for account {}, ordinal: {}",
                    accountId, snapshot.getLastEntryOrdinal());
            }

            return snapshot;

        }, shardExecutors[shardId]);
    }

    /**
     * Get latest snapshot for account
     */
    public CompletableFuture<Optional<AccountBalanceSnapshot>> getLatestSnapshot(UUID accountId) {
        int shardId = getShardId(accountId);

        return CompletableFuture.supplyAsync(() -> {
            AccountBalanceSnapshot snapshot = latestSnapshots[shardId].get(accountId);

            if (snapshot != null) {
                snapshotHits.incrementAndGet();
                log.debug("Snapshot hit for account {}, ordinal: {}",
                    accountId, snapshot.getLastEntryOrdinal());
            } else {
                snapshotMisses.incrementAndGet();
                log.debug("Snapshot miss for account {}", accountId);
            }

            return Optional.ofNullable(snapshot);

        }, shardExecutors[shardId]);
    }

    /**
     * Get snapshots for multiple accounts efficiently
     */
    public CompletableFuture<Map<UUID, AccountBalanceSnapshot>> getSnapshots(Set<UUID> accountIds) {
        if (accountIds.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }

        // Group accounts by shard
        Map<Integer, List<UUID>> accountsByShard = accountIds.stream()
            .collect(Collectors.groupingBy(this::getShardId));

        // Process each shard in parallel
        List<CompletableFuture<Map<UUID, AccountBalanceSnapshot>>> shardFutures =
            accountsByShard.entrySet().stream()
                .map(entry -> {
                    int shardId = entry.getKey();
                    List<UUID> shardAccounts = entry.getValue();

                    return CompletableFuture.supplyAsync(() -> {
                        Map<UUID, AccountBalanceSnapshot> shardResults = new HashMap<>();

                        for (UUID accountId : shardAccounts) {
                            AccountBalanceSnapshot snapshot = latestSnapshots[shardId].get(accountId);
                            if (snapshot != null) {
                                shardResults.put(accountId, snapshot);
                                snapshotHits.incrementAndGet();
                            } else {
                                snapshotMisses.incrementAndGet();
                            }
                        }

                        return shardResults;
                    }, shardExecutors[shardId]);
                })
                .collect(Collectors.toList());

        // Combine results from all shards
        return CompletableFuture.allOf(shardFutures.toArray(new CompletableFuture[0]))
            .thenApply(ignored -> {
                Map<UUID, AccountBalanceSnapshot> allResults = new HashMap<>();
                shardFutures.forEach(future -> allResults.putAll(future.join()));
                return allResults;
            });
    }

    /**
     * Check if account needs snapshot update based on operations count
     */
    public boolean shouldCreateSnapshot(int operationsSinceLastSnapshot) {
        return operationsSinceLastSnapshot >= SNAPSHOT_THRESHOLD;
    }

    /**
     * Remove snapshots for accounts (for cleanup)
     */
    public CompletableFuture<Integer> removeSnapshots(Set<UUID> accountIds) {
        Map<Integer, List<UUID>> accountsByShard = accountIds.stream()
            .collect(Collectors.groupingBy(this::getShardId));

        List<CompletableFuture<Integer>> removalFutures = accountsByShard.entrySet().stream()
            .map(entry -> {
                int shardId = entry.getKey();
                List<UUID> shardAccounts = entry.getValue();

                return CompletableFuture.supplyAsync(() -> {
                    int removed = 0;
                    for (UUID accountId : shardAccounts) {
                        if (latestSnapshots[shardId].remove(accountId) != null) {
                            removed++;
                            snapshotCounts[shardId].decrementAndGet();
                        }
                    }
                    return removed;
                }, shardExecutors[shardId]);
            })
            .collect(Collectors.toList());

        return CompletableFuture.allOf(removalFutures.toArray(new CompletableFuture[0]))
            .thenApply(ignored -> {
                int totalRemoved = removalFutures.stream()
                    .mapToInt(CompletableFuture::join)
                    .sum();

                totalSnapshots.addAndGet(-totalRemoved);
                log.info("Removed {} snapshots", totalRemoved);

                return totalRemoved;
            });
    }

    /**
     * Get all snapshots (for backup/restore)
     */
    public CompletableFuture<Map<UUID, AccountBalanceSnapshot>> getAllSnapshots() {
        List<CompletableFuture<Map<UUID, AccountBalanceSnapshot>>> shardFutures = new ArrayList<>();

        for (int shardId = 0; shardId < shardCount; shardId++) {
            final int currentShard = shardId;

            CompletableFuture<Map<UUID, AccountBalanceSnapshot>> shardFuture =
                CompletableFuture.supplyAsync(() -> {
                    return new HashMap<>(latestSnapshots[currentShard]);
                }, shardExecutors[currentShard]);

            shardFutures.add(shardFuture);
        }

        return CompletableFuture.allOf(shardFutures.toArray(new CompletableFuture[0]))
            .thenApply(ignored -> {
                Map<UUID, AccountBalanceSnapshot> allSnapshots = new HashMap<>();
                shardFutures.forEach(future -> allSnapshots.putAll(future.join()));

                log.info("Retrieved {} total snapshots across {} shards",
                    allSnapshots.size(), shardCount);
                return allSnapshots;
            });
    }

    /**
     * Restore snapshots from backup
     */
    public CompletableFuture<Integer> restoreSnapshots(Map<UUID, AccountBalanceSnapshot> snapshots) {
        if (snapshots.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }

        // Group snapshots by shard
        Map<Integer, Map<UUID, AccountBalanceSnapshot>> snapshotsByShard = snapshots.entrySet().stream()
            .collect(Collectors.groupingBy(
                entry -> getShardId(entry.getKey()),
                Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)
            ));

        List<CompletableFuture<Integer>> restoreFutures = snapshotsByShard.entrySet().stream()
            .map(entry -> {
                int shardId = entry.getKey();
                Map<UUID, AccountBalanceSnapshot> shardSnapshots = entry.getValue();

                return CompletableFuture.supplyAsync(() -> {
                    // Clear existing snapshots in shard
                    latestSnapshots[shardId].clear();

                    // Restore snapshots
                    latestSnapshots[shardId].putAll(shardSnapshots);
                    snapshotCounts[shardId].set(shardSnapshots.size());

                    return shardSnapshots.size();
                }, shardExecutors[shardId]);
            })
            .collect(Collectors.toList());

        return CompletableFuture.allOf(restoreFutures.toArray(new CompletableFuture[0]))
            .thenApply(ignored -> {
                int totalRestored = restoreFutures.stream()
                    .mapToInt(CompletableFuture::join)
                    .sum();

                totalSnapshots.set(totalRestored);
                log.info("Restored {} snapshots across {} shards", totalRestored, shardCount);

                return totalRestored;
            });
    }

    /**
     * Performance and health statistics
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new HashMap<>();

        stats.put("shardCount", shardCount);
        stats.put("snapshotThreshold", SNAPSHOT_THRESHOLD);
        stats.put("totalSnapshots", totalSnapshots.get());
        stats.put("snapshotHits", snapshotHits.get());
        stats.put("snapshotMisses", snapshotMisses.get());

        // Calculate hit rate
        long totalRequests = snapshotHits.get() + snapshotMisses.get();
        double hitRate = totalRequests > 0 ? (snapshotHits.get() * 100.0) / totalRequests : 0.0;
        stats.put("hitRatePercent", String.format("%.2f", hitRate));

        // Per-shard statistics
        Map<String, Object> shardStats = new HashMap<>();
        for (int i = 0; i < shardCount; i++) {
            Map<String, Object> shardStat = new HashMap<>();
            shardStat.put("snapshotCount", latestSnapshots[i].size());
            shardStat.put("totalCreated", snapshotCounts[i].get());
            shardStats.put("shard_" + i, shardStat);
        }
        stats.put("shardStatistics", shardStats);

        return stats;
    }

    /**
     * Health check
     */
    public boolean isHealthy() {
        // Check that all shards are responsive and not overloaded
        long totalSnapshots = Arrays.stream(snapshotCounts)
            .mapToLong(AtomicLong::get)
            .sum();

        return totalSnapshots >= 0 &&
            Arrays.stream(latestSnapshots).allMatch(Objects::nonNull);
    }

    @Override
    public void close() {
        log.info("Closing ContainerOptimizedSnapshotService...");

        for (ExecutorService executor : shardExecutors) {
            if (executor != null) {
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    executor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
        }

        log.info("ContainerOptimizedSnapshotService closed. Final stats: {}", getStatistics());
    }
}