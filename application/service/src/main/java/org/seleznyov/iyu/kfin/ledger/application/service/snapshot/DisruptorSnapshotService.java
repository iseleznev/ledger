package org.seleznyov.iyu.kfin.ledger.application.service.snapshot;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.domain.model.snapshot.EntriesSnapshot;
import org.seleznyov.iyu.kfin.ledger.infrastructure.disruptor.DisruptorManager;
import org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.api.repository.EntriesSnapshotRepository;
import org.seleznyov.iyu.kfin.ledger.infrastructure.detection.MLEnhancedHotAccountDetector;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Service
@RequiredArgsConstructor
@Slf4j
public class DisruptorSnapshotService {

    private final DisruptorManager disruptorManager;
    private final EntriesSnapshotRepository snapshotRepository;
    private final MLEnhancedHotAccountDetector hotAccountDetector;

    // Timeout for snapshot operations
    private static final long SNAPSHOT_TIMEOUT_MS = 10000; // 10 seconds

    // Performance metrics
    private final AtomicLong snapshotsCreated = new AtomicLong(0);
    private final AtomicLong failedCreations = new AtomicLong(0);
    private final AtomicLong totalCreationTime = new AtomicLong(0);

    @Transactional
    public EntriesSnapshot createSnapshot(UUID accountId, LocalDate operationDate) {
        log.debug("Creating disruptor snapshot for hot account: {}, date: {}", accountId, operationDate);

        long startTime = System.currentTimeMillis();

        try {
            // 1. Check if snapshot already exists
            if (snapshotAlreadyExists(accountId, operationDate)) {
                log.debug("Snapshot already exists for hot account {} on date {}", accountId, operationDate);
                throw new SnapshotAlreadyExistsException(
                    "Snapshot already exists for account " + accountId + " on date " + operationDate
                );
            }

            // 2. Create snapshot through disruptor barrier mechanism
            CompletableFuture<SnapshotResult> snapshotFuture = publishSnapshotBarrier(accountId, operationDate);

            // 3. Wait for snapshot completion with timeout
            SnapshotResult result = snapshotFuture.get(SNAPSHOT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            if (!result.success()) {
                log.warn("Disruptor snapshot failed: account={}, date={}, reason={}",
                    accountId, operationDate, result.errorMessage());
                failedCreations.incrementAndGet();
                throw new SnapshotCreationException(result.errorMessage());
            }

            // 4. Create and persist snapshot record
            EntriesSnapshot snapshot = new EntriesSnapshot(
                UUID.randomUUID(),
                accountId,
                operationDate,
                result.balance(),
                result.lastEntryRecordId(),
                result.lastEntryOrdinal(),
                result.operationsCount()
            );

            snapshotRepository.insert(snapshot);

            // Update metrics
            snapshotsCreated.incrementAndGet();
            totalCreationTime.addAndGet(System.currentTimeMillis() - startTime);

            log.info("Disruptor snapshot created successfully: account={}, balance={}, entries={}, date={}",
                accountId, result.balance(), result.operationsCount(), operationDate);

            return snapshot;

        } catch (java.util.concurrent.TimeoutException e) {
            failedCreations.incrementAndGet();
            log.error("Disruptor snapshot timeout: account={}, date={}", accountId, operationDate);
            throw new SnapshotCreationException("Snapshot creation timeout for account " + accountId);

        } catch (java.util.concurrent.ExecutionException e) {
            failedCreations.incrementAndGet();
            log.error("Disruptor snapshot execution error: account={}, date={}, error={}",
                accountId, operationDate, e.getCause().getMessage());
            throw new SnapshotCreationException("Snapshot creation failed: " + e.getCause().getMessage());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failedCreations.incrementAndGet();
            log.error("Disruptor snapshot interrupted: account={}, date={}", accountId, operationDate);
            throw new SnapshotCreationException("Snapshot creation interrupted");

        } catch (SnapshotCreationException | SnapshotAlreadyExistsException e) {
            throw e; // Re-throw known exceptions

        } catch (Exception e) {
            failedCreations.incrementAndGet();
            log.error("Unexpected error creating disruptor snapshot: account={}, date={}, error={}",
                accountId, operationDate, e.getMessage(), e);
            throw new SnapshotCreationException(
                "Unexpected error creating snapshot for account " + accountId + ": " + e.getMessage(), e
            );
        }
    }

    /**
     * Checks if account can use disruptor snapshot service.
     * Only hot accounts currently managed by disruptor should use this service.
     */
    public boolean canCreateSnapshot(UUID accountId) {
        try {
            // Primary check: Is this account detected as hot by ML system?
            boolean isHotAccount = hotAccountDetector.isHotAccount(accountId);

            if (!isHotAccount) {
                log.debug("Account {} not detected as hot by ML system", accountId);
                return false;
            }

            // Secondary check: Is disruptor manager initialized and running?
            // Since we don't have isAccountManaged() method, we check if disruptor is active
            // and assume hot accounts should use disruptor if it's available
            boolean disruptorAvailable = disruptorManager.isInitialized();

            if (!disruptorAvailable) {
                log.debug("Disruptor not available for hot account {}", accountId);
                return false;
            }

            log.debug("Account {} can use disruptor snapshots: hot={}, disruptorAvailable={}",
                accountId, isHotAccount, disruptorAvailable);

            return true;

        } catch (Exception e) {
            log.warn("Error checking disruptor snapshot capability for account {}: {}",
                accountId, e.getMessage());

            // Fail safe: if we can't determine account status,
            // assume it should use SimpleLedgerService for safety
            return false;
        }
    }

    /**
     * Gets snapshot creation statistics for monitoring.
     */
    public SnapshotStats getSnapshotStats() {
        long created = snapshotsCreated.get();
        long failed = failedCreations.get();
        double avgTime = created > 0 ? (double) totalCreationTime.get() / created : 0.0;

        return new SnapshotStats(
            "DISRUPTOR",
            created,
            failed,
            avgTime
        );
    }

    /**
     * Checks if disruptor is available for processing.
     */
    public boolean isAccountInDisruptor(UUID accountId) {
        try {
            return disruptorManager.isInitialized();
        } catch (Exception e) {
            log.warn("Error checking disruptor status: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Gets detailed account analysis for administrative purposes.
     */
    public AccountSnapshotAnalysis getAccountAnalysis(UUID accountId) {
        try {
            boolean isHot = hotAccountDetector.isHotAccount(accountId);
            boolean inDisruptor = disruptorManager.isInitialized();
            boolean canSnapshot = canCreateSnapshot(accountId);

            var mlAnalysis = hotAccountDetector.getDetailedAnalysis(accountId);

            return new AccountSnapshotAnalysis(
                accountId,
                isHot,
                inDisruptor,
                canSnapshot,
                mlAnalysis.reason(),
                mlAnalysis.burstScore(),
                mlAnalysis.shortTermScore(),
                mlAnalysis.mlScore()
            );

        } catch (Exception e) {
            log.warn("Error analyzing account {}: {}", accountId, e.getMessage());
            return new AccountSnapshotAnalysis(
                accountId, false, false, false,
                "Analysis failed: " + e.getMessage(), 0.0, 0.0, 0.0
            );
        }
    }

    private CompletableFuture<SnapshotResult> publishSnapshotBarrier(UUID accountId, LocalDate operationDate) {
        try {
            // Use actual barrier mechanism from DisruptorManager
            CompletableFuture<DisruptorManager.SnapshotBarrierResult> barrierFuture =
                disruptorManager.publishSnapshotBarrier(accountId, operationDate);

            // Transform barrier result to snapshot result
            return barrierFuture.thenApply(barrierResult -> new SnapshotResult(
                barrierResult.success(),
                barrierResult.balance(),
                barrierResult.lastEntryRecordId(),
                barrierResult.lastEntryOrdinal(),
                barrierResult.operationsCount(),
                barrierResult.errorMessage()
            ));

        } catch (Exception e) {
            log.error("Failed to publish snapshot barrier: account={}, error={}", accountId, e.getMessage());

            CompletableFuture<SnapshotResult> future = new CompletableFuture<>();
            future.complete(new SnapshotResult(
                false, // failure
                0L,
                null,
                0L,
                0,
                "Failed to publish barrier: " + e.getMessage()
            ));

            return future;
        }
    }

    private boolean snapshotAlreadyExists(UUID accountId, LocalDate operationDate) {
        try {
            // Use the repository method that should delegate to client
            return snapshotRepository.findExactSnapshot(accountId, operationDate).isPresent();

        } catch (Exception e) {
            log.warn("Error checking existing snapshot: account={}, date={}, error={}",
                accountId, operationDate, e.getMessage());
            // Fail safe: assume no snapshot exists on error
            return false;
        }
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

    private record SnapshotResult(
        boolean success,
        long balance,
        UUID lastEntryRecordId,
        long lastEntryOrdinal,
        int operationsCount,
        String errorMessage
    ) {}

    public record SnapshotStats(
        String strategy,
        long snapshotsCreated,
        long failedCreations,
        double averageCreationTimeMs
    ) {}

    public record AccountSnapshotAnalysis(
        UUID accountId,
        boolean isHotAccount,
        boolean inDisruptorRing,
        boolean canCreateSnapshot,
        String mlReason,
        double burstScore,
        double shortTermScore,
        double mlScore
    ) {}
}