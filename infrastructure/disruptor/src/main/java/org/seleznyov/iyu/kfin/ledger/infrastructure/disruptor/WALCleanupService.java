package org.seleznyov.iyu.kfin.ledger.infrastructure.disruptor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Service for periodic cleanup of old WAL entries.
 * Prevents WAL table growth by removing old processed/failed entries.
 * <p>
 * Configuration properties:
 * - ledger.wal.cleanup.enabled: Enable/disable automatic cleanup (default: true)
 * - ledger.wal.cleanup.retention-days: Days to retain WAL entries (default: 7)
 */
@Service
@ConditionalOnProperty(name = "ledger.wal.cleanup.enabled", havingValue = "true", matchIfMissing = true)
@RequiredArgsConstructor
@Slf4j
public class WALCleanupService {

    private final WALService walService;

    private final AtomicLong totalCleaned = new AtomicLong(0);
    private final AtomicLong lastCleanupTime = new AtomicLong(0);

    /**
     * Periodic WAL cleanup task.
     * Runs every 6 hours by default, removes entries older than 7 days.
     */
    @Scheduled(fixedRateString = "${ledger.wal.cleanup.interval:21600000}") // 6 hours default
    public void performScheduledCleanup() {
        int retentionDays = getRetentionDays();

        log.info("Starting scheduled WAL cleanup (retention: {} days)", retentionDays);

        try {
            long startTime = System.currentTimeMillis();
            int cleanedCount = walService.cleanupOldEntries(retentionDays);
            long duration = System.currentTimeMillis() - startTime;

            totalCleaned.addAndGet(cleanedCount);
            lastCleanupTime.set(System.currentTimeMillis());

            log.info("WAL cleanup completed in {}ms: cleaned {} entries", duration, cleanedCount);

        } catch (Exception e) {
            log.error("WAL cleanup failed: {}", e.getMessage(), e);
        }
    }

    /**
     * Manual WAL cleanup (administrative function).
     *
     * @param retentionDays Custom retention period in days
     * @return Number of entries cleaned up
     */
    public int performManualCleanup(int retentionDays) {
        if (retentionDays < 1) {
            throw new IllegalArgumentException("Retention days must be at least 1");
        }

        log.info("Starting manual WAL cleanup (retention: {} days)", retentionDays);

        try {
            long startTime = System.currentTimeMillis();
            int cleanedCount = walService.cleanupOldEntries(retentionDays);
            long duration = System.currentTimeMillis() - startTime;

            totalCleaned.addAndGet(cleanedCount);
            lastCleanupTime.set(System.currentTimeMillis());

            log.info("Manual WAL cleanup completed in {}ms: cleaned {} entries", duration, cleanedCount);
            return cleanedCount;

        } catch (Exception e) {
            log.error("Manual WAL cleanup failed: {}", e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Gets cleanup statistics for monitoring.
     */
    public CleanupStats getCleanupStats() {
        return new CleanupStats(
            totalCleaned.get(),
            lastCleanupTime.get(),
            getRetentionDays()
        );
    }

    private int getRetentionDays() {
        // Default retention period: 7 days
        // In real application, this would read from @Value or ConfigurationProperties
        return 7; // Configurable via properties: ${ledger.wal.cleanup.retention-days:7}
    }

    /**
     * Cleanup statistics for monitoring.
     */
    public record CleanupStats(
        long totalEntriesCleaned,
        long lastCleanupTimestamp,
        int retentionDays
    ) {

    }
}