package org.seleznyov.iyu.kfin.ledger.application.service.snapshot;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.application.service.AdaptiveStrategyService;
import org.seleznyov.iyu.kfin.ledger.domain.model.ProcessingStrategy;
import org.seleznyov.iyu.kfin.ledger.domain.model.snapshot.EntriesSnapshot;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.UUID;

/**
 * Adaptive snapshot service that routes snapshot creation to appropriate strategy.
 * Determines whether to use simple (advisory lock) or disruptor (barrier) approach
 * based on account characteristics and system load.
 * <p>
 * Routing logic:
 * - Cold accounts -> SimpleSnapshotService with advisory locks
 * - Hot accounts -> DisruptorSnapshotService with barrier pattern
 * - Fallback to SimpleSnapshotService if DisruptorSnapshotService unavailable
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class AdaptiveSnapshotService {

    private final SimpleSnapshotService simpleSnapshotService;
    private final DisruptorSnapshotService disruptorSnapshotService;
    private final AdaptiveStrategyService adaptiveStrategyService;

    /**
     * Creates snapshot using adaptive strategy selection.
     *
     * @param accountId     Account to create snapshot for
     * @param operationDate Snapshot date
     * @return Created snapshot
     */
    public EntriesSnapshot createSnapshot(UUID accountId, LocalDate operationDate) {
        log.debug("Creating adaptive snapshot for account: {}, date: {}", accountId, operationDate);

        // Determine strategy for this account
        ProcessingStrategy strategy = adaptiveStrategyService.selectStrategyForBalance(accountId);

        log.debug("Selected {} strategy for snapshot creation: account={}", strategy, accountId);

        try {
            return switch (strategy) {
                case SIMPLE -> {
                    log.debug("Routing to SimpleSnapshotService for cold account snapshot");
                    yield simpleSnapshotService.createSnapshot(accountId, operationDate);
                }
                case DISRUPTOR -> {
                    log.debug("Routing to DisruptorSnapshotService for hot account snapshot");

                    // Check if disruptor service can handle this account
                    if (disruptorSnapshotService.canCreateSnapshot(accountId)) {
                        yield disruptorSnapshotService.createSnapshot(accountId, operationDate);
                    } else {
                        log.warn("DisruptorSnapshotService cannot handle account {}, falling back to SimpleSnapshotService",
                            accountId);
                        yield simpleSnapshotService.createSnapshot(accountId, operationDate);
                    }
                }
            };

        } catch (Exception e) {
            log.error("Adaptive snapshot creation failed: account={}, date={}, strategy={}, error={}",
                accountId, operationDate, strategy, e.getMessage(), e);

            // If primary strategy fails and it was DISRUPTOR, try fallback to SIMPLE
            if (strategy == ProcessingStrategy.DISRUPTOR) {
                log.info("Attempting fallback to SimpleSnapshotService after DISRUPTOR failure: account={}",
                    accountId);

                try {
                    return simpleSnapshotService.createSnapshot(accountId, operationDate);

                } catch (Exception fallbackException) {
                    log.error("Fallback snapshot creation also failed: account={}, date={}, error={}",
                        accountId, operationDate, fallbackException.getMessage());
                    throw new SnapshotCreationException(
                        "Both primary and fallback snapshot creation failed for account " + accountId,
                        fallbackException
                    );
                }
            } else {
                // SIMPLE strategy failed, no fallback available
                throw new SnapshotCreationException(
                    "Snapshot creation failed for account " + accountId + ": " + e.getMessage(), e
                );
            }
        }
    }

    /**
     * Creates snapshots for multiple accounts (batch operation).
     *
     * @param accountIds    List of account IDs
     * @param operationDate Snapshot date
     * @return Batch operation result
     */
    public BatchSnapshotResult createBatchSnapshots(java.util.List<UUID> accountIds, LocalDate operationDate) {
        log.info("Creating batch snapshots for {} accounts, date: {}", accountIds.size(), operationDate);

        int successful = 0;
        int failed = 0;
        int simpleUsed = 0;
        int disruptorUsed = 0;

        for (UUID accountId : accountIds) {
            try {
                ProcessingStrategy strategy = adaptiveStrategyService.selectStrategyForBalance(accountId);

                createSnapshot(accountId, operationDate);

                successful++;
                if (strategy == ProcessingStrategy.SIMPLE) {
                    simpleUsed++;
                } else {
                    disruptorUsed++;
                }

                log.debug("Batch snapshot successful for account {}: strategy={}", accountId, strategy);

            } catch (Exception e) {
                failed++;
                log.warn("Batch snapshot failed for account {}: {}", accountId, e.getMessage());
            }
        }

        log.info("Batch snapshot completed: {} successful ({} simple, {} disruptor), {} failed",
            successful, simpleUsed, disruptorUsed, failed);

        return new BatchSnapshotResult(
            accountIds.size(),
            successful,
            failed,
            simpleUsed,
            disruptorUsed,
            operationDate
        );
    }

    /**
     * Gets combined snapshot statistics from all strategies.
     *
     * @return Combined snapshot statistics
     */
    public CombinedSnapshotStats getSnapshotStats() {
        var simpleStats = simpleSnapshotService.getSnapshotStats();
        var disruptorStats = disruptorSnapshotService.getSnapshotStats();

        return new CombinedSnapshotStats(
            simpleStats,
            disruptorStats,
            simpleStats.snapshotsCreated() + disruptorStats.snapshotsCreated(),
            simpleStats.failedCreations() + disruptorStats.failedCreations()
        );
    }

    /**
     * Forces strategy refresh and creates snapshot (administrative function).
     *
     * @param accountId     Account ID
     * @param operationDate Snapshot date
     * @param forceStrategy Optional strategy to force
     * @return Created snapshot
     */
    public EntriesSnapshot createSnapshotWithStrategy(
        UUID accountId,
        LocalDate operationDate,
        ProcessingStrategy forceStrategy
    ) {
        log.info("Creating snapshot with forced strategy: account={}, date={}, strategy={}",
            accountId, operationDate, forceStrategy);

        return switch (forceStrategy) {
            case SIMPLE -> simpleSnapshotService.createSnapshot(accountId, operationDate);
            case DISRUPTOR -> disruptorSnapshotService.createSnapshot(accountId, operationDate);
        };
    }

    /**
     * Exception thrown when snapshot creation fails.
     */
    public static class SnapshotCreationException extends RuntimeException {

        public SnapshotCreationException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Batch snapshot operation result.
     */
    public record BatchSnapshotResult(
        int totalAccounts,
        int successfulSnapshots,
        int failedSnapshots,
        int simpleStrategyUsed,
        int disruptorStrategyUsed,
        LocalDate operationDate
    ) {

    }

    /**
     * Combined statistics from all snapshot strategies.
     */
    public record CombinedSnapshotStats(
        SimpleSnapshotService.SnapshotStats simpleStats,
        DisruptorSnapshotService.SnapshotStats disruptorStats,
        long totalSnapshotsCreated,
        long totalFailedCreations
    ) {

    }
}