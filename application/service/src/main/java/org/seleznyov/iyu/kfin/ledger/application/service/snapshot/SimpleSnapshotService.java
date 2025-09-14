package org.seleznyov.iyu.kfin.ledger.application.service.snapshot;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.domain.model.snapshot.EntriesSnapshot;
import org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.api.model.result.EntriesSummaryResult;
import org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.api.repository.AdvisoryLockRepository;
import org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.api.repository.EntriesSnapshotRepository;
import org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.api.repository.EntryRecordRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@Service
@RequiredArgsConstructor
@Slf4j
public class SimpleSnapshotService {

    private final AdvisoryLockRepository advisoryLockRepository;
    private final EntryRecordRepository entryRecordRepository;
    private final EntriesSnapshotRepository snapshotRepository;

    private final AtomicLong snapshotsCreated = new AtomicLong(0);
    private final AtomicLong failedCreations = new AtomicLong(0);
    private final AtomicLong totalCreationTime = new AtomicLong(0);


    @Transactional
    public EntriesSnapshot createSnapshot(UUID accountId, LocalDate operationDate) {
        log.debug("Creating simple snapshot for account: {}, date: {}", accountId, operationDate);

        long startTime = System.currentTimeMillis();

        // 1. Acquire advisory lock to ensure consistency
        boolean lockAcquired = advisoryLockRepository.tryTransactionalLock(accountId);
        if (!lockAcquired) {
            failedCreations.incrementAndGet();
            log.warn("Failed to acquire lock for snapshot creation: account={}", accountId);
            throw new SnapshotCreationException(
                "Account " + accountId + " is currently locked, cannot create snapshot"
            );
        }

        log.debug("Acquired advisory lock for snapshot creation: account={}", accountId);

        try {
            // 2. Check if snapshot already exists for this date
            if (snapshotAlreadyExists(accountId, operationDate)) {
                failedCreations.incrementAndGet();
                log.debug("Snapshot already exists for account {} on date {}", accountId, operationDate);
                throw new SnapshotAlreadyExistsException(
                    "Snapshot already exists for account " + accountId + " on date " + operationDate
                );
            }

            // 3. Get account summary (balance + metadata)
            EntriesSummaryResult summary = entryRecordRepository.entriesSummary(accountId, operationDate);

            // 4. Create snapshot record
            EntriesSnapshot snapshot = new EntriesSnapshot(
                UUID.randomUUID(), // Generate new snapshot ID
                accountId,
                operationDate,
                summary.balance(),
                summary.lastEntryRecordId(),
                summary.lastEntryOrdinal(),
                summary.operationsCount()
            );

            // 5. Persist snapshot
            snapshotRepository.insert(snapshot);

            // 6. Update success metrics
            snapshotsCreated.incrementAndGet();
            totalCreationTime.addAndGet(System.currentTimeMillis() - startTime);

            log.info("Simple snapshot created successfully: account={}, balance={}, entries={}, date={}",
                accountId, summary.balance(), summary.operationsCount(), operationDate);

            return snapshot;

        } catch (SnapshotCreationException | SnapshotAlreadyExistsException e) {
            // Re-throw known exceptions (metrics already updated)
            throw e;

        } catch (Exception e) {
            failedCreations.incrementAndGet();
            log.error("Failed to create simple snapshot: account={}, date={}, error={}",
                accountId, operationDate, e.getMessage(), e);
            throw new SnapshotCreationException(
                "Failed to create snapshot for account " + accountId + ": " + e.getMessage(), e
            );
        }
        // Advisory lock is automatically released when transaction commits/rolls back
    }

    public boolean canCreateSnapshot(UUID accountId) {
        return true; // Simple service can handle any account
    }

    /**
     * Gets snapshot creation statistics for monitoring.
     * Now returns real metrics instead of placeholders.
     */
    public SnapshotStats getSnapshotStats() {
        long created = snapshotsCreated.get();
        long failed = failedCreations.get();
        double avgTime = created > 0 ? (double) totalCreationTime.get() / created : 0.0;

        return new SnapshotStats(
            "SIMPLE",
            created,
            failed,
            avgTime
        );
    }

    private boolean snapshotAlreadyExists(UUID accountId, LocalDate operationDate) {
        return snapshotRepository.findExactSnapshot(accountId, operationDate).isPresent();
    }

    public static class SnapshotCreationException extends RuntimeException {
        public SnapshotCreationException(String message) {
            super(message);
        }

        public SnapshotCreationException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static class SnapshotAlreadyExistsException extends RuntimeException {
        public SnapshotAlreadyExistsException(String message) {
            super(message);
        }
    }

    public record SnapshotStats(
        String strategy,
        long snapshotsCreated,
        long failedCreations,
        double averageCreationTimeMs
    ) {}
}