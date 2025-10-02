package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.domain.model.snapshot.EntriesSnapshot;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.PostgreSqlEntriesSnapshotRingBufferHandler;

import java.util.UUID;

// ===== ACCOUNT SNAPSHOT MANAGER =====

@Slf4j
@Data
@Accessors(fluent = true)
public class SnapshotArenaManager {

    private final UUID accountId;
    private final PostgreSqlEntriesSnapshotRingBufferHandler snapshotBatch;

    // Snapshot triggers configuration
    private final long timeBasedIntervalMs;
    private final int countBasedThreshold;

    // State tracking
    private AccountSnapshot lastSnapshot;
    private long lastSnapshotTime;
    private int entriesSinceLastSnapshot = 0;

    public SnapshotArenaManager(
        UUID accountId,
        PostgreSqlEntriesSnapshotRingBufferHandler ringBufferHandler,
        long timeBasedIntervalMs,
        int countBasedThreshold
    ) {
        this.accountId = accountId;
        this.snapshotBatch = ringBufferHandler;
        this.timeBasedIntervalMs = timeBasedIntervalMs;
        this.countBasedThreshold = countBasedThreshold;
        this.lastSnapshotTime = System.currentTimeMillis();
    }

    /**
     * ✅ Проверяет нужен ли snapshot и создает его если нужно
     */
    public boolean maybeCreateSnapshot(long currentBalance, long currentSequence) {

        long currentTime = System.currentTimeMillis();
        entriesSinceLastSnapshot++;

        SnapshotTrigger trigger = null;

        // Time-based trigger
        if (currentTime - lastSnapshotTime >= timeBasedIntervalMs) {
            trigger = SnapshotTrigger.TIME_BASED;
        }
        // Count-based trigger
        else if (entriesSinceLastSnapshot >= countBasedThreshold) {
            trigger = SnapshotTrigger.COUNT_BASED;
        }

        if (trigger != null) {
            return createSnapshot(currentBalance, currentSequence, trigger,
                currentTime - lastSnapshotTime);
        }

        return false;
    }

    /**
     * ✅ Создает snapshot при прохождении barrier
     */
    public boolean createBarrierSnapshot(long currentBalance, long currentSequence) {
        long currentTime = System.currentTimeMillis();
        return createSnapshot(currentBalance, currentSequence, SnapshotTrigger.BARRIER_BASED,
            currentTime - lastSnapshotTime);
    }

    private boolean createSnapshot(long balance, long sequence, SnapshotTrigger trigger, long duration) {

        AccountSnapshot snapshot = AccountSnapshot.create(
            accountId, balance, entriesSinceLastSnapshot, sequence, trigger, duration);

        boolean added = snapshotBatch.addSnapshot(snapshot);

        if (added) {
            this.lastSnapshot = snapshot;
            this.lastSnapshotTime = snapshot.createdAt().toEpochMilli();
            this.entriesSinceLastSnapshot = 0;

            log.debug("Created {} snapshot for account {}: balance={}, entries={}",
                trigger, accountId, balance, snapshot.entryCount());
        }

        return added;
    }

    public EntriesSnapshot getLastSnapshot() {
        return lastSnapshot;
    }
}
