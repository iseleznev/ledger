package org.seleznyov.iyu.kfin.ledger.application.service.arena;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

// Main ledger service with account sharding
@Slf4j
@Service
@RequiredArgsConstructor
public class HighPerformanceLedgerService {

    private static final int SHARD_COUNT = 256; // Power of 2 for fast modulo
    private final ConcurrentHashMap<UUID, AccountBatchManager> hotAccountManagers = new ConcurrentHashMap<>();
    private final Set<UUID> hotAccountIds; // Injected from configuration
    private final LedgerRingBuffer ringBuffer;
    private final SnapshotManager snapshotManager;
    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final Executor virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();

    @Value("${ledger.batch.maxSize:1000}")
    private int maxBatchSize;

    @Value("${ledger.snapshot.everyNOperations:5000}")
    private int snapshotEveryNOperations;

    public CompletableFuture<String> processTransaction(UUID debitAccountId, UUID creditAccountId,
                                                        long amount, String currencyCode,
                                                        UUID transactionId, UUID idempotencyKey) {
        return CompletableFuture.supplyAsync(() -> {

            // Create debit and credit entries
            long baseOrdinal = System.nanoTime(); // Simple ordering for demo
            LocalDate operationDate = LocalDate.now();
            Instant now = Instant.now();

            EntryRecord debitEntry = EntryRecord.builder()
                .id(UUID.randomUUID())
                .accountId(debitAccountId)
                .transactionId(transactionId)
                .entryType(EntryRecord.EntryType.DEBIT)
                .amount(amount)
                .createdAt(now)
                .operationDate(operationDate)
                .idempotencyKey(idempotencyKey)
                .currencyCode(currencyCode)
                .entryOrdinal(baseOrdinal)
                .build();

            EntryRecord creditEntry = EntryRecord.builder()
                .id(UUID.randomUUID())
                .accountId(creditAccountId)
                .transactionId(transactionId)
                .entryType(EntryRecord.EntryType.CREDIT)
                .amount(amount)
                .createdAt(now)
                .operationDate(operationDate)
                .idempotencyKey(idempotencyKey)
                .currencyCode(currencyCode)
                .entryOrdinal(baseOrdinal + 1)
                .build();

            // Process entries based on account type
            if (isHotAccount(debitAccountId)) {
                processHotAccountEntry(debitEntry);
            } else {
                processColdAccountEntry(debitEntry);
            }

            if (isHotAccount(creditAccountId)) {
                processHotAccountEntry(creditEntry);
            } else {
                processColdAccountEntry(creditEntry);
            }

            return "Transaction accepted: " + transactionId;

        }, virtualExecutor);
    }

    private void processHotAccountEntry(EntryRecord entry) {
        AccountBatchManager manager = getOrCreateHotAccountManager(entry.getAccountId());

        CompletableFuture<Boolean> addResult = manager.addEntry(entry);
        addResult.thenAccept(added -> {
            if (added) {
                // Update snapshot
                snapshotManager.updateSnapshot(entry.getAccountId(), entry);

                // Check if batch is ready
                if (manager.isBatchReady()) {
                    BatchData batch = manager.extractBatch();
                    if (batch != null) {
                        boolean published = publishToRingBuffer(batch);
                        if (published) {
                            // Notify manager that batch was queued
                            manager.onBatchQueued(batch.getEntryCount());
                        }
                    }
                }
            } else {
                log.warn("Failed to add entry to batch for hot account {}", entry.getAccountId());
            }
        });
    }

    private void processColdAccountEntry(EntryRecord entry) {
        // Use PostgreSQL Advisory Lock for cold accounts
        virtualExecutor.execute(() -> {
            String lockSql = "SELECT pg_advisory_lock(:accountHash)";
            String unlockSql = "SELECT pg_advisory_unlock(:accountHash)";

            long accountHash = entry.getAccountId().hashCode();
            Map<String, Object> lockParams = Map.of("accountHash", accountHash);

            try {
                // Acquire advisory lock
                jdbcTemplate.queryForObject(lockSql, lockParams, Boolean.class);

                // Insert entry directly to database
                insertSingleEntry(entry);

                // Update snapshot if needed
                snapshotManager.updateSnapshot(entry.getAccountId(), entry);

            } finally {
                // Release advisory lock
                jdbcTemplate.queryForObject(unlockSql, lockParams, Boolean.class);
            }
        });
    }

    private void insertSingleEntry(EntryRecord entry) {
        String sql = """
            INSERT INTO ledger.entry_records 
            (id, account_id, transaction_id, entry_type, amount, created_at, 
             operation_date, idempotency_key, currency_code, entry_ordinal)
            VALUES (:id, :accountId, :transactionId, :entryType, :amount, :createdAt, 
                    :operationDate, :idempotencyKey, :currencyCode, :entryOrdinal)
            """;

        Map<String, Object> params = Map.of(
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

        jdbcTemplate.update(sql, params);
        log.debug("Inserted single entry for cold account {}", entry.getAccountId());
    }

    private AccountBatchManager getOrCreateHotAccountManager(UUID accountId) {
        return hotAccountManagers.computeIfAbsent(accountId, id ->
            new AccountBatchManager(id, maxBatchSize, snapshotEveryNOperations));
    }

    private boolean publishToRingBuffer(BatchData batch) {
        int maxRetries = 10;
        int retries = 0;

        while (retries < maxRetries) {
            boolean published = ringBuffer.tryPublish(batch);
            if (published) {
                return true;
            }

            retries++;
            // Use onSpinWait instead of sleep for virtual threads
            Thread.onSpinWait();
        }

        log.error("Failed to publish batch {} after {} retries", batch.getBatchId(), maxRetries);
        batch.setStatus(BatchData.BatchStatus.FAILED);
        return false;
    }

    private boolean isHotAccount(UUID accountId) {
        return hotAccountIds.contains(accountId);
    }

    // Get current balance - for hot accounts use memory, for cold accounts use DB
    public long getCurrentBalance(UUID accountId) {
        if (isHotAccount(accountId)) {
            // Hot account: get from memory (snapshot + entries in current batch)
            AccountBatchManager manager = hotAccountManagers.get(accountId);
            if (manager != null) {
                AccountSnapshot lastSnapshot = snapshotManager.getLatestSnapshot(accountId);
                return manager.getCurrentBalance(lastSnapshot);
            } else {
                // Hot account but no manager yet - get from DB
                return snapshotManager.getCurrentBalanceFromDb(accountId);
            }
        } else {
            // Cold account: get from database
            return snapshotManager.getCurrentBalanceFromDb(accountId);
        }
    }

    // Account sharding for load distribution
    private int getAccountShard(UUID accountId) {
        return Math.abs(accountId.hashCode()) % SHARD_COUNT;
    }

    @PreDestroy
    public void cleanup() {
        hotAccountManagers.values().forEach(AccountBatchManager::cleanup);
    }
}
