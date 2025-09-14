package org.seleznyov.iyu.kfin.ledger.application.service.arenaseparatedold;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.application.service.arenaseparated.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

// Manager Registry with separated interfaces
@Slf4j
@Component
public class SeparatedAccountManagerRegistry {

    private static final int MAX_COLD_MANAGERS_IN_MEMORY = 100_000;

    // Hot accounts: always in memory with dedicated shards
    private final Map<UUID, HotAccountManager>[] hotAccountShards;
    private final Executor[] hotAccountExecutors;

    // Cold accounts: LRU cache for resource efficiency
    private final Cache<UUID, ColdAccountManager> coldAccountCache;
    private final Executor coldAccountExecutor;

    private final Set<UUID> hotAccountIds;
    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Value("${ledger.batch.maxSize:1000}")
    private int maxBatchSize;

    @Value("${ledger.snapshot.everyNOperations:5000}")
    private int snapshotEveryNOperations;

    @SuppressWarnings("unchecked")
    public SeparatedAccountManagerRegistry(Set<UUID> hotAccountIds,
                                           NamedParameterJdbcTemplate jdbcTemplate) {
        this.hotAccountIds = hotAccountIds;
        this.jdbcTemplate = jdbcTemplate;

        // Initialize hot account infrastructure
        this.hotAccountShards = new Map[256];
        this.hotAccountExecutors = new Executor[256];

        for (int i = 0; i < 256; i++) {
            hotAccountShards[i] = new ConcurrentHashMap<>();
            hotAccountExecutors[i] = Executors.newVirtualThreadPerTaskExecutor();
        }

        // Cold account LRU cache
        this.coldAccountCache = Caffeine.newBuilder()
            .maximumSize(MAX_COLD_MANAGERS_IN_MEMORY)
            .expireAfterAccess(Duration.ofMinutes(30))
            .removalListener((UUID key, ColdAccountManager value, RemovalCause cause) -> {
                if (value != null) {
                    value.cleanup();
                    log.debug("Evicted cold account manager for {}", key);
                }
            })
            .build();

        this.coldAccountExecutor = Executors.newVirtualThreadPerTaskExecutor();

        log.info("Initialized SEPARATED Account Manager Registry: {} hot accounts, max {} cold in cache",
            hotAccountIds.size(), MAX_COLD_MANAGERS_IN_MEMORY);
    }

    public HotAccountManager getHotAccountManager(UUID accountId) {
        if (!hotAccountIds.contains(accountId)) {
            throw new IllegalArgumentException("Account " + accountId + " is not configured as hot");
        }

        int shard = getAccountShard(accountId);
        return hotAccountShards[shard].computeIfAbsent(accountId, id ->
            new HighPerformanceHotAccountManager(id, maxBatchSize, snapshotEveryNOperations));
    }

    public ColdAccountManager getColdAccountManager(UUID accountId) {
        if (hotAccountIds.contains(accountId)) {
            throw new IllegalArgumentException("Account " + accountId + " is configured as hot");
        }

        return coldAccountCache.get(accountId, id ->
            new ResourceEfficientColdAccountManager(id, snapshotEveryNOperations, jdbcTemplate));
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

    public Map<String, Object> getStatistics() {
        return Map.of(
            "hotAccountsConfigured", hotAccountIds.size(),
            "hotAccountShards", hotAccountShards.length,
            "coldAccountsCached", coldAccountCache.estimatedSize(),
            "coldAccountCacheHitRate", coldAccountCache.stats().hitRate()
        );
    }

    @PreDestroy
    public void cleanup() {
        for (Map<UUID, HotAccountManager> shard : hotAccountShards) {
            shard.values().forEach(BaseAccountManager::cleanup);
        }
        coldAccountCache.invalidateAll();
    }
}
