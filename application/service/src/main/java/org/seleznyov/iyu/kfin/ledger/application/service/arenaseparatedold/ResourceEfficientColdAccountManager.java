package org.seleznyov.iyu.kfin.ledger.application.service.arenaseparatedold;

import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.application.service.arena.AccountSnapshot;
import org.seleznyov.iyu.kfin.ledger.application.service.arenaseparated.ColdAccountManager;
import org.seleznyov.iyu.kfin.ledger.application.service.arenaseparated.EntryRecord;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

// Resource-Efficient Cold Account Implementation
@Slf4j
public class ResourceEfficientColdAccountManager implements ColdAccountManager {

    private final UUID accountId;
    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final Executor virtualExecutor;
    private final int snapshotEveryNOperations;

    // Minimal atomics for snapshot coordination
    private final AtomicInteger operationsSinceSnapshot = new AtomicInteger(0);
    private final AtomicLong lastSnapshotOrdinal = new AtomicLong(0);

    // Memory footprint: ~200 bytes vs 2MB for hot accounts

    public ResourceEfficientColdAccountManager(UUID accountId, int snapshotEveryNOperations,
                                               NamedParameterJdbcTemplate jdbcTemplate) {
        this.accountId = accountId;
        this.snapshotEveryNOperations = snapshotEveryNOperations;
        this.jdbcTemplate = jdbcTemplate;
        this.virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();
        log.debug("Created RESOURCE-EFFICIENT cold account manager for {}", accountId);
    }

    @Override
    public UUID getAccountId() {
        return accountId;
    }

    @Override
    public CompletableFuture<Boolean> addEntry(EntryRecord entry) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Direct database insert with PostgreSQL Advisory Lock
                insertEntryDirectly(entry);

                // Snapshot logic with atomics
                int operations = operationsSinceSnapshot.incrementAndGet();
                if (operations >= snapshotEveryNOperations) {
                    if (operationsSinceSnapshot.compareAndSet(operations, 0)) {
                        lastSnapshotOrdinal.set(entry.getEntryOrdinal());
                    }
                }

                log.trace("Added entry to cold account {} via direct DB insert", accountId);
                return true;

            } catch (Exception e) {
                log.error("Failed to insert entry for cold account {}", accountId, e);
                return false;
            }
        }, virtualExecutor);
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
        // For cold accounts, always get fresh balance from DB (acceptable for low RPS)
        return getCurrentBalanceFromDb();
    }

    private long getCurrentBalanceFromDb() {
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
        return operationsSinceSnapshot.get() == 0 && lastSnapshotOrdinal.get() > 0;
    }

    @Override
    public long getSnapshotOrdinal() {
        return lastSnapshotOrdinal.get();
    }

    @Override
    public void onSnapshotCreated() {
        lastSnapshotOrdinal.set(0);
        log.debug("Snapshot created for cold account {}", accountId);
    }

    @Override
    public void cleanup() {
        log.debug("Cleanup cold account manager for {} (no resources to release)", accountId);
    }
}
