package org.seleznyov.iyu.kfin.ledger.application.service.arenaseparated;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

// Hybrid Partitioned Cold Account Manager - Best of both worlds
@Slf4j
public class HybridPartitionedColdAccountManager implements PartitionAccountManager {

    private final int partitionId;
    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final Executor virtualExecutor;
    private final int snapshotEveryNOperations;

    // Active account states - only for recently accessed accounts
    private final ConcurrentHashMap<UUID, AccountState> activeAccountStates = new ConcurrentHashMap<>();

    // Statistics for monitoring - using volatile for approximate statistics (performance optimization)
    private volatile long totalOperations = 0;
    private volatile long cacheHits = 0;
    private volatile long cacheMisses = 0;

    // Minimal per-account state for active accounts only
    private static class AccountState {
        final AtomicInteger operationsSinceSnapshot = new AtomicInteger(0);
        final AtomicLong lastSnapshotOrdinal = new AtomicLong(0);
        volatile long lastAccessTime = System.currentTimeMillis();
        final UUID accountId;

        AccountState(UUID accountId) {
            this.accountId = accountId;
        }

        void touch() {
            lastAccessTime = System.currentTimeMillis();
        }

        boolean isInactive(long cutoffTime) {
            return lastAccessTime < cutoffTime;
        }
    }

    public HybridPartitionedColdAccountManager(int partitionId, int snapshotEveryNOperations,
                                               NamedParameterJdbcTemplate jdbcTemplate) {
        this.partitionId = partitionId;
        this.snapshotEveryNOperations = snapshotEveryNOperations;
        this.jdbcTemplate = jdbcTemplate;
        this.virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();

        log.debug("Created HYBRID partition {} cold account manager (snapshot_every={})",
            partitionId, snapshotEveryNOperations);
    }

    @Override
    public UUID getAccountId() {
        // Partition manager handles multiple accounts, not a single account
        // Return a synthetic UUID that represents this partition
        throw new UnsupportedOperationException(
            "Partition manager handles multiple accounts. Use getPartitionId() instead."
        );
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public CompletableFuture<Boolean> addEntry(EntryRecord entry) {
        totalOperations++; // Simple increment, approximate statistics

        return CompletableFuture.supplyAsync(() -> {
            try {
                UUID accountId = entry.getAccountId();

                // Get or create account state (DB hit only on first access per account)
                AccountState state = getOrCreateAccountState(accountId);

                // All subsequent operations use memory state
                int operations = state.operationsSinceSnapshot.incrementAndGet();
                state.touch();

                // Check if snapshot is needed
                if (operations >= snapshotEveryNOperations) {
                    // Atomic reset - only winner creates snapshot
                    if (state.operationsSinceSnapshot.compareAndSet(operations, 0)) {
                        state.lastSnapshotOrdinal.set(entry.getEntryOrdinal());
                        log.debug("Triggered snapshot for cold account {} in partition {} at ordinal {}",
                            accountId, partitionId, entry.getEntryOrdinal());
                    }
                }

                // Direct database insert with PostgreSQL Advisory Lock
                insertEntryDirectly(entry);

                log.trace("Added entry to cold account {} in partition {}", accountId, partitionId);
                return true;

            } catch (Exception e) {
                log.error("Failed to insert entry for cold account {} in partition {}",
                    entry.getAccountId(), partitionId, e);
                return false;
            }
        }, virtualExecutor);
    }

    private AccountState getOrCreateAccountState(UUID accountId) {
        AccountState existing = activeAccountStates.get(accountId);
        if (existing != null) {
            cacheHits++; // Approximate increment
            return existing;
        }

        // Cache miss - create new state and initialize from DB
        cacheMisses++; // Approximate increment
        return activeAccountStates.computeIfAbsent(accountId, id -> {
            AccountState newState = new AccountState(id);
            initializeAccountStateFromDb(newState);
            return newState;
        });
    }

    private void initializeAccountStateFromDb(AccountState state) {
        try {
            // One-time DB query to get last snapshot info
            String sql = """
                SELECT operations_count, last_entry_ordinal 
                FROM ledger.entries_snapshots 
                WHERE account_id = :accountId 
                ORDER BY snapshot_ordinal DESC 
                LIMIT 1
                """;

            Map<String, Object> params = Map.of("accountId", state.accountId);
            List<Map<String, Object>> results = jdbcTemplate.queryForList(sql, params);

            if (!results.isEmpty()) {
                Map<String, Object> row = results.get(0);
                Integer operationsCount = (Integer) row.get("operations_count");
                Long lastEntryOrdinal = ((Number) row.get("last_entry_ordinal")).longValue();

                // Initialize state from last snapshot
                if (operationsCount != null) {
                    state.operationsSinceSnapshot.set(0); // Reset since we're starting fresh
                }
                if (lastEntryOrdinal != null) {
                    state.lastSnapshotOrdinal.set(lastEntryOrdinal);
                }

                log.debug("Initialized cold account {} state from DB: operations_count={}",
                    state.accountId, operationsCount);
            } else {
                // No snapshot exists - start from zero
                state.operationsSinceSnapshot.set(0);
                state.lastSnapshotOrdinal.set(0);
                log.debug("No snapshot found for cold account {}, starting from zero", state.accountId);
            }

        } catch (Exception e) {
            log.warn("Failed to initialize account state from DB for {}, using defaults",
                state.accountId, e);
            state.operationsSinceSnapshot.set(0);
            state.lastSnapshotOrdinal.set(0);
        }
    }

    private void insertEntryDirectly(EntryRecord entry) {
        String lockSql = "SELECT pg_advisory_lock(:accountHash)";
        String unlockSql = "SELECT pg_advisory_unlock(:accountHash)";
        String insertSql = """
            INSERT INTO ledger.entry_records 
            (id, account_id, transaction_id, entry_type, amount, created_at, 
             operation_date, idempotency_key, currency_code, entry_ordinal)
            VALUES (:id, :accountId, :transactionId, :entryType, :amount, :createdAt, 
                    :operationDate, :idempotencyKey, :currencyCode, :entryOrdinal)
            """;

        long accountHash = entry.getAccountId().hashCode();
        Map<String, Object> lockParams = Map.of("accountHash", accountHash);

        try {
            // Acquire PostgreSQL Advisory Lock
            jdbcTemplate.queryForObject(lockSql, lockParams, Boolean.class);

            // Insert entry
            Map<String, Object> entryParams = Map.of(
                "id", entry.getId(),
                "accountId", entry.getAccountId(),
                "transactionId", entry.getTransactionId(),
                "entryType", entry.getEntryType().name(),
                "amount", entry.getAmount(),
                "createdAt", entry.getCreatedAt(),
                "operationDate", entry.getOperationDate(),
                "idempotencyKey", entry.getIdempotencyKey(),
                "currencyCode", entry.getCurrencyCode(),
                "entryOrdinal", entry.getEntryOrdinal()
            );

            jdbcTemplate.update(insertSql, entryParams);

        } finally {
            // Release lock
            jdbcTemplate.queryForObject(unlockSql, lockParams, Boolean.class);
        }
    }

    @Override
    public long getCurrentBalance(AccountSnapshot lastSnapshot) {
        // This method is called without specific account ID, which doesn't make sense for partition manager
        // Throw exception to indicate incorrect usage
        throw new UnsupportedOperationException(
            "Partition manager requires specific account ID. Use getCurrentBalanceForAccount(UUID accountId) instead."
        );
    }

    @Override
    public long getCurrentBalanceForAccount(UUID accountId) {
        // Option 1: Use cached account state if available (faster)
        AccountState state = activeAccountStates.get(accountId);
        if (state != null) {
            // Could calculate balance from cached entries, but for simplicity use DB
            log.trace("Getting balance for cached account {} in partition {}", accountId, partitionId);
        }

        // Option 2: Always get fresh balance from DB (acceptable for low RPS cold accounts)
        return getCurrentBalanceFromDb(accountId);
    }

    private long getCurrentBalanceFromDb(UUID accountId) {
        String snapshotSql = """
            SELECT balance, last_entry_ordinal 
            FROM ledger.entries_snapshots 
            WHERE account_id = :accountId 
            ORDER BY snapshot_ordinal DESC 
            LIMIT 1
            """;

        Map<String, Object> params = Map.of("accountId", accountId);
        List<Map<String, Object>> snapshots = jdbcTemplate.queryForList(snapshotSql, params);

        long balance = 0L;
        long lastEntryOrdinal = 0L;

        if (!snapshots.isEmpty()) {
            Map<String, Object> snapshot = snapshots.get(0);
            balance = ((Number) snapshot.get("balance")).longValue();
            lastEntryOrdinal = ((Number) snapshot.get("last_entry_ordinal")).longValue();
        }

        // Get entries after snapshot
        String entriesSql = """
            SELECT entry_type, amount 
            FROM ledger.entry_records 
            WHERE account_id = :accountId 
            AND entry_ordinal > :lastEntryOrdinal
            ORDER BY entry_ordinal
            """;

        Map<String, Object> entryParams = Map.of(
            "accountId", accountId,
            "lastEntryOrdinal", lastEntryOrdinal
        );

        List<Map<String, Object>> entries = jdbcTemplate.queryForList(entriesSql, entryParams);

        for (Map<String, Object> entry : entries) {
            String entryType = (String) entry.get("entry_type");
            long amount = ((Number) entry.get("amount")).longValue();

            balance += "DEBIT".equals(entryType) ? -amount : amount;
        }

        return balance;
    }

    @Override
    public boolean shouldCreateSnapshot() {
        // For partition manager, check if ANY account needs snapshot
        boolean hasSnapshotNeeded = activeAccountStates.values().stream()
            .anyMatch(state -> state.operationsSinceSnapshot.get() == 0 && state.lastSnapshotOrdinal.get() > 0);

        if (hasSnapshotNeeded) {
            log.debug("Partition {} has accounts that need snapshots", partitionId);
        }
        return hasSnapshotNeeded;
    }

    @Override
    public long getSnapshotOrdinal() {
        // Return max ordinal from all active accounts that need snapshots
        return activeAccountStates.values().stream()
            .filter(state -> state.operationsSinceSnapshot.get() == 0 && state.lastSnapshotOrdinal.get() > 0)
            .mapToLong(state -> state.lastSnapshotOrdinal.get())
            .max()
            .orElse(0L);
    }

    @Override
    public void onSnapshotCreated() {
        // Reset flags for all accounts that had snapshots created
        List<AccountState> snapshotAccounts = activeAccountStates.values().stream()
            .filter(state -> state.operationsSinceSnapshot.get() == 0 && state.lastSnapshotOrdinal.get() > 0)
            .collect(Collectors.toList());

        snapshotAccounts.forEach(state -> state.lastSnapshotOrdinal.set(0));

        log.debug("Snapshot created for partition {}, reset flags for {} accounts",
            partitionId, snapshotAccounts.size());
    }

    @Override
    public boolean shouldCreateSnapshotForAccount(UUID accountId) {
        AccountState state = activeAccountStates.get(accountId);
        if (state == null) {
            log.trace("No cached state for account {} in partition {}, no snapshot needed", accountId, partitionId);
            return false;
        }

        boolean shouldCreate = state.operationsSinceSnapshot.get() == 0 && state.lastSnapshotOrdinal.get() > 0;
        if (shouldCreate) {
            log.debug("Account {} in partition {} should create snapshot at ordinal {}",
                accountId, partitionId, state.lastSnapshotOrdinal.get());
        }
        return shouldCreate;
    }

    @Override
    public long getSnapshotOrdinalForAccount(UUID accountId) {
        AccountState state = activeAccountStates.get(accountId);
        if (state == null) {
            log.trace("No cached state for account {} in partition {}, snapshot ordinal is 0", accountId, partitionId);
            return 0L;
        }
        return state.lastSnapshotOrdinal.get();
    }

    @Override
    public void onSnapshotCreatedForAccount(UUID accountId) {
        AccountState state = activeAccountStates.get(accountId);
        if (state != null) {
            long previousOrdinal = state.lastSnapshotOrdinal.getAndSet(0);
            log.debug("Snapshot created for account {} in partition {} at ordinal {}",
                accountId, partitionId, previousOrdinal);
        } else {
            log.warn("Attempted to mark snapshot created for account {} in partition {}, but no cached state found",
                accountId, partitionId);
        }
    }

    // Cleanup inactive accounts - called periodically
    @Override
    public int cleanupInactiveAccounts(Duration inactiveThreshold) {
        long cutoffTime = System.currentTimeMillis() - inactiveThreshold.toMillis();

        List<UUID> inactiveAccounts = activeAccountStates.entrySet().stream()
            .filter(entry -> entry.getValue().isInactive(cutoffTime))
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());

        int removedCount = 0;
        for (UUID accountId : inactiveAccounts) {
            AccountState removed = activeAccountStates.remove(accountId);
            if (removed != null) {
                removedCount++;
                log.trace("Evicted inactive cold account {} from partition {}", accountId, partitionId);
            }
        }

        if (removedCount > 0) {
            log.debug("Cleaned up {} inactive accounts from partition {}, remaining: {}",
                removedCount, partitionId, activeAccountStates.size());
        }

        return removedCount;
    }

    // Statistics and monitoring
    @Override
    public Map<String, Object> getPartitionStatistics() {
        double cacheHitRate = 0.0;
        long totalOps = totalOperations; // Volatile read
        if (totalOps > 0) {
            cacheHitRate = cacheHits / (double) totalOps; // Volatile reads
        }

        return Map.of(
            "partitionId", partitionId,
            "activeAccountsCount", activeAccountStates.size(),
            "totalOperations", totalOps,
            "cacheHitRate", cacheHitRate,
            "cacheHits", cacheHits, // Volatile read
            "cacheMisses", cacheMisses, // Volatile read
            "memoryUsageBytes", activeAccountStates.size() * 200L, // Approximate
            "statisticsNote", "approximate values for performance"
        );
    }

    public int getActiveAccountsCount() {
        return activeAccountStates.size();
    }

    @Override
    public void cleanup() {
        activeAccountStates.clear();
        log.debug("Cleanup hybrid partition {} manager (cleared {} active accounts)",
            partitionId, activeAccountStates.size());
    }
}