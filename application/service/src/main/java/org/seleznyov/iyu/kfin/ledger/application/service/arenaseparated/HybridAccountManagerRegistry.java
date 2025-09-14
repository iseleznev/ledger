package org.seleznyov.iyu.kfin.ledger.application.service.arenaseparated;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

// Hybrid Account Manager Registry - Static partitioning for cold accounts
@Slf4j
@Component
public class HybridAccountManagerRegistry {

    private static final int COLD_PARTITION_COUNT = 256; // Static partitions for cold accounts

    // Hot accounts: always in memory with dedicated shards
    private final Map<UUID, HotAccountManager>[] hotAccountShards;
    private final Executor[] hotAccountExecutors;

    // Cold accounts: static partitioned managers (no cache needed!)
    private final HybridPartitionedColdAccountManager[] coldPartitions;
    private final Executor coldAccountExecutor;

    // Scheduled cleanup for inactive accounts
    private final ScheduledExecutorService cleanupExecutor;

    private final Set<UUID> hotAccountIds;
    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Value("${ledger.batch.maxSize:1000}")
    private int maxBatchSize;

    @Value("${ledger.snapshot.everyNOperations:5000}")
    private int snapshotEveryNOperations;

    @Value("${ledger.cold.cleanup.inactiveMinutes:30}")
    private int inactiveMinutes;

    @Value("${ledger.cold.cleanup.intervalMinutes:5}")
    private int cleanupIntervalMinutes;

    @SuppressWarnings("unchecked")
    public HybridAccountManagerRegistry(Set<UUID> hotAccountIds,
                                        NamedParameterJdbcTemplate jdbcTemplate) {
        this.hotAccountIds = hotAccountIds;
        this.jdbcTemplate = jdbcTemplate;

        // Initialize hot account infrastructure (unchanged)
        this.hotAccountShards = new Map[256];
        this.hotAccountExecutors = new Executor[256];

        for (int i = 0; i < 256; i++) {
            hotAccountShards[i] = new ConcurrentHashMap<>();
            hotAccountExecutors[i] = Executors.newVirtualThreadPerTaskExecutor();
        }

        // Initialize static cold partitions - NO CACHE!
        this.coldPartitions = new HybridPartitionedColdAccountManager[COLD_PARTITION_COUNT];
        for (int i = 0; i < COLD_PARTITION_COUNT; i++) {
            coldPartitions[i] = new HybridPartitionedColdAccountManager(i, snapshotEveryNOperations, jdbcTemplate);
        }

        this.coldAccountExecutor = Executors.newVirtualThreadPerTaskExecutor();

        // Setup periodic cleanup of inactive accounts
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor();
        startPeriodicCleanup();

        log.info("Initialized HYBRID Account Manager Registry: {} hot accounts, {} cold partitions",
            hotAccountIds.size(), COLD_PARTITION_COUNT);
    }

    public HotAccountManager getHotAccountManager(UUID accountId) {
        if (!hotAccountIds.contains(accountId)) {
            throw new IllegalArgumentException("Account " + accountId + " is not configured as hot");
        }

        int shard = getAccountShard(accountId);
        return hotAccountShards[shard].computeIfAbsent(accountId, id ->
            new HighPerformanceHotAccountManager(id, maxBatchSize, snapshotEveryNOperations));
    }

    // SUPER FAST - just array lookup, no cache needed!
    public ColdAccountManager getColdAccountManager(UUID accountId) {
        if (hotAccountIds.contains(accountId)) {
            throw new IllegalArgumentException("Account " + accountId + " is configured as hot");
        }

        // Static partitioning - O(1) array access
        int partition = getColdAccountPartition(accountId);
        return coldPartitions[partition];
    }

    // Get partition manager for specific account (for snapshot operations)
    public PartitionAccountManager getColdPartitionManager(UUID accountId) {
        if (hotAccountIds.contains(accountId)) {
            throw new IllegalArgumentException("Account " + accountId + " is configured as hot");
        }

        int partition = getColdAccountPartition(accountId);
        return coldPartitions[partition]; // Returns PartitionAccountManager interface
    }

    public boolean isHotAccount(UUID accountId) {
        return hotAccountIds.contains(accountId);
    }

    public Executor getHotAccountExecutor(UUID accountId) {
        return hotAccountExecutors[getAccountShard(accountId)];
    }

    public Executor getColdAccountExecutor() {
        return coldAccountExecutor;
    }

    private int getAccountShard(UUID accountId) {
        return Math.abs(accountId.hashCode()) % 256;
    }

    private int getColdAccountPartition(UUID accountId) {
        return Math.abs(accountId.hashCode()) % COLD_PARTITION_COUNT;
    }

    private void startPeriodicCleanup() {
        cleanupExecutor.scheduleWithFixedDelay(
            this::cleanupInactiveAccounts,
            cleanupIntervalMinutes,
            cleanupIntervalMinutes,
            TimeUnit.MINUTES
        );

        log.info("Started periodic cleanup: every {} minutes, inactive threshold {} minutes",
            cleanupIntervalMinutes, inactiveMinutes);
    }

    private void cleanupInactiveAccounts() {
        try {
            Duration inactiveThreshold = Duration.ofMinutes(inactiveMinutes);
            int totalCleaned = 0;

            for (int i = 0; i < COLD_PARTITION_COUNT; i++) {
                int cleaned = coldPartitions[i].cleanupInactiveAccounts(inactiveThreshold);
                totalCleaned += cleaned;
            }

            if (totalCleaned > 0) {
                log.info("Cleanup completed: removed {} inactive cold accounts from {} partitions",
                    totalCleaned, COLD_PARTITION_COUNT);
            }

        } catch (Exception e) {
            log.error("Error during periodic cleanup of inactive accounts", e);
        }
    }

    public Map<String, Object> getStatistics() {
        // Hot account statistics
        int totalHotAccounts = 0;
        for (Map<UUID, HotAccountManager> shard : hotAccountShards) {
            totalHotAccounts += shard.size();
        }

        // Cold partition statistics
        int totalActiveColdAccounts = 0;
        long totalColdOperations = 0;
        double avgCacheHitRate = 0.0;

        for (HybridPartitionedColdAccountManager partition : coldPartitions) {
            Map<String, Object> partitionStats = partition.getPartitionStatistics();
            totalActiveColdAccounts += (Integer) partitionStats.get("activeAccountsCount");
            totalColdOperations += (Long) partitionStats.get("totalOperations");
            avgCacheHitRate += (Double) partitionStats.get("cacheHitRate");
        }
        avgCacheHitRate /= COLD_PARTITION_COUNT;

        return Map.of(
            "hotAccountsConfigured", hotAccountIds.size(),
            "hotAccountsActive", totalHotAccounts,
            "hotAccountShards", hotAccountShards.length,
            "coldPartitionsCount", COLD_PARTITION_COUNT,
            "coldAccountsActive", totalActiveColdAccounts,
            "coldTotalOperations", totalColdOperations,
            "coldAvgCacheHitRate", avgCacheHitRate,
            "memoryModel", "hybrid-partitioned"
        );
    }

    // Get detailed statistics per partition (for monitoring)
    public List<Map<String, Object>> getDetailedColdPartitionStats() {
        return Arrays.stream(coldPartitions)
            .map(HybridPartitionedColdAccountManager::getPartitionStatistics)
            .collect(Collectors.toList());
    }

    // Force cleanup of inactive accounts (for admin operations)
    public Map<String, Object> forceCleanupInactiveAccounts(Duration inactiveThreshold) {
        int totalCleaned = 0;
        Map<Integer, Integer> partitionCleanup = new HashMap<>();

        for (int i = 0; i < COLD_PARTITION_COUNT; i++) {
            int cleaned = coldPartitions[i].cleanupInactiveAccounts(inactiveThreshold);
            totalCleaned += cleaned;
            partitionCleanup.put(i, cleaned);
        }

        log.info("Force cleanup completed: removed {} inactive cold accounts from {} partitions",
            totalCleaned, COLD_PARTITION_COUNT);

        return Map.of(
            "totalAccountsCleaned", totalCleaned,
            "partitionBreakdown", partitionCleanup,
            "inactiveThresholdMinutes", inactiveThreshold.toMinutes(),
            "timestamp", Instant.now()
        );
    }

    @PreDestroy
    public void cleanup() {
        log.info("Shutting down Hybrid Account Manager Registry...");

        // Cleanup hot accounts
        for (Map<UUID, HotAccountManager> shard : hotAccountShards) {
            shard.values().forEach(BaseAccountManager::cleanup);
        }

        // Cleanup cold partitions
        for (HybridPartitionedColdAccountManager partition : coldPartitions) {
            partition.cleanup();
        }

        // Shutdown cleanup executor
        cleanupExecutor.shutdown();
        try {
            if (!cleanupExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            cleanupExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        log.info("Hybrid Account Manager Registry shutdown complete");
    }
}