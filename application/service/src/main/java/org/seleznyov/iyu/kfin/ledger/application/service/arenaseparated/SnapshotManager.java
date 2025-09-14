package org.seleznyov.iyu.kfin.ledger.application.service.arenaseparated;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

// Snapshot manager with off-heap storage
@Slf4j
@Component
public class SnapshotManager {

    private final Arena snapshotArena = Arena.ofShared();
    private final ConcurrentHashMap<UUID, MemorySegment> accountSnapshots = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, AtomicLong> accountBalances = new ConcurrentHashMap<>();
    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final Executor virtualExecutor;

    public SnapshotManager(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public CompletableFuture<Void> updateSnapshot(UUID accountId, EntryRecord entry) {
        return CompletableFuture.runAsync(() -> {
            MemorySegment snapshot = accountSnapshots.computeIfAbsent(accountId,
                id -> snapshotArena.allocate(LedgerMemoryLayouts.SNAPSHOT_SIZE));

            AtomicLong balance = accountBalances.computeIfAbsent(accountId,
                id -> new AtomicLong(getCurrentBalanceFromDb(accountId)));

            // Update balance based on entry type
            long newBalance = entry.getEntryType() == EntryRecord.EntryType.DEBIT
                ? balance.addAndGet(-entry.getAmount())
                : balance.addAndGet(entry.getAmount());

            updateSnapshotInMemory(snapshot, accountId, entry, newBalance);

        }, virtualExecutor);
    }

    private void updateSnapshotInMemory(MemorySegment snapshot, UUID accountId,
                                        EntryRecord entry, long balance) {
        writeUuidToMemory(snapshot, 0, UUID.randomUUID()); // snapshot id
        writeUuidToMemory(snapshot, 16, accountId);
        snapshot.set(JAVA_INT, 32, (int) entry.getOperationDate().toEpochDay());
        snapshot.set(JAVA_LONG, 36, balance);
        writeUuidToMemory(snapshot, 44, entry.getId());
        snapshot.set(JAVA_LONG, 60, entry.getEntryOrdinal());
        snapshot.set(JAVA_INT, 68, 0); // operations_count - will be set when persisting
        snapshot.set(JAVA_LONG, 72, System.currentTimeMillis());
        snapshot.set(JAVA_LONG, 80, 0L); // snapshot_ordinal - will be set when persisting
    }

    public CompletableFuture<Void> persistSnapshot(UUID accountId) {
        return CompletableFuture.runAsync(() -> {
            MemorySegment snapshot = accountSnapshots.get(accountId);
            if (snapshot == null) return;

            AccountSnapshot snapshotData = readSnapshotFromMemory(snapshot);
            saveSnapshotToDatabase(snapshotData);

        }, virtualExecutor);
    }

    private AccountSnapshot readSnapshotFromMemory(MemorySegment snapshot) {
        return AccountSnapshot.builder()
            .id(readUuidFromMemory(snapshot, 0))
            .accountId(readUuidFromMemory(snapshot, 16))
            .operationDate(LocalDate.ofEpochDay(snapshot.get(JAVA_INT, 32)))
            .balance(snapshot.get(JAVA_LONG, 36))
            .lastEntryRecordId(readUuidFromMemory(snapshot, 44))
            .lastEntryOrdinal(snapshot.get(JAVA_LONG, 60))
            .operationsCount(snapshot.get(JAVA_INT, 68))
            .createdAt(Instant.ofEpochMilli(snapshot.get(JAVA_LONG, 72)))
            .snapshotOrdinal(snapshot.get(JAVA_LONG, 80))
            .build();
    }

    private UUID readUuidFromMemory(MemorySegment segment, long offset) {
        long mostSig = segment.get(JAVA_LONG, offset);
        long leastSig = segment.get(JAVA_LONG, offset + 8);
        return new UUID(mostSig, leastSig);
    }

    private void saveSnapshotToDatabase(AccountSnapshot snapshot) {
        String sql = """
            INSERT INTO ledger.entries_snapshots 
            (id, account_id, operation_date, balance, last_entry_record_id, 
             last_entry_ordinal, operations_count, created_at)
            VALUES (:id, :accountId, :operationDate, :balance, :lastEntryRecordId, 
                    :lastEntryOrdinal, :operationsCount, :createdAt)
            """;

        Map<String, Object> params = Map.of(
            "id", snapshot.getId(),
            "accountId", snapshot.getAccountId(),
            "operationDate", snapshot.getOperationDate(),
            "balance", snapshot.getBalance(),
            "lastEntryRecordId", snapshot.getLastEntryRecordId(),
            "lastEntryOrdinal", snapshot.getLastEntryOrdinal(),
            "operationsCount", getCurrentOperationsCountFromDb(snapshot.getAccountId()),
            "createdAt", snapshot.getCreatedAt()
        );

        jdbcTemplate.update(sql, params);

        log.debug("Persisted snapshot for account {} with balance {}",
            snapshot.getAccountId(), snapshot.getBalance());
    }

    // Get balance from database: last snapshot + subsequent entries
    public long getCurrentBalanceFromDb(UUID accountId) {
        // Get latest snapshot
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
            AND entry_ordinal >= :lastEntryOrdinal
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

            if ("DEBIT".equals(entryType)) {
                balance -= amount;
            } else {
                balance += amount;
            }
        }

        return balance;
    }

    public AccountSnapshot getLatestSnapshot(UUID accountId) {
        String sql = """
            SELECT id, account_id, operation_date, balance, last_entry_record_id, 
                   last_entry_ordinal, operations_count, snapshot_ordinal, created_at
            FROM ledger.entries_snapshots 
            WHERE account_id = :accountId 
            ORDER BY snapshot_ordinal DESC 
            LIMIT 1
            """;

        Map<String, Object> params = Map.of("accountId", accountId);

        List<Map<String, Object>> results = jdbcTemplate.queryForList(sql, params);

        if (results.isEmpty()) {
            return null;
        }

        Map<String, Object> row = results.get(0);
        return AccountSnapshot.builder()
            .id((UUID) row.get("id"))
            .accountId((UUID) row.get("account_id"))
            .operationDate(((java.sql.Date) row.get("operation_date")).toLocalDate())
            .balance(((Number) row.get("balance")).longValue())
            .lastEntryRecordId((UUID) row.get("last_entry_record_id"))
            .lastEntryOrdinal(((Number) row.get("last_entry_ordinal")).longValue())
            .operationsCount(((Number) row.get("operations_count")).intValue())
            .snapshotOrdinal(((Number) row.get("snapshot_ordinal")).longValue())
            .createdAt(((java.sql.Timestamp) row.get("created_at")).toInstant())
            .build();
    }

    private int getCurrentOperationsCountFromDb(UUID accountId) {
        String sql = """
            SELECT COUNT(*) 
            FROM ledger.entry_records 
            WHERE account_id = :accountId
            """;

        Map<String, Object> params = Map.of("accountId", accountId);
        Integer count = jdbcTemplate.queryForObject(sql, params, Integer.class);
        return count != null ? count : 0;
    }

    private void writeUuidToMemory(MemorySegment segment, long offset, UUID uuid) {
        segment.set(JAVA_LONG, offset, uuid.getMostSignificantBits());
        segment.set(JAVA_LONG, offset + 8, uuid.getLeastSignificantBits());
    }
}
