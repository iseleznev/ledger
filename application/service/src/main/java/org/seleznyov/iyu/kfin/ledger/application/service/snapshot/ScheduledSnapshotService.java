package org.seleznyov.iyu.kfin.ledger.application.service.snapshot;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.application.config.SnapshotConfiguration;
import org.seleznyov.iyu.kfin.ledger.infrastructure.detection.MLEnhancedHotAccountDetector;
import org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.api.repository.AccountRepository;
import org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.api.repository.EntriesSnapshotRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Scheduled service for automatic daily snapshot creation.
 * Runs periodic tasks to maintain snapshot coverage for performance optimization.
 * <p>
 * Configuration properties:
 * - ledger.snapshots.scheduled.enabled: Enable/disable automatic snapshots (default: true)
 * - ledger.snapshots.scheduled.accounts: List of accounts for daily snapshots
 * - ledger.snapshots.scheduled.cron: Cron expression for scheduling (default: daily at 2 AM)
 */
@Service
@ConditionalOnProperty(name = "ledger.snapshots.scheduled.enabled", havingValue = "true", matchIfMissing = true)
@RequiredArgsConstructor
@Slf4j
public class ScheduledSnapshotService {

    private final AdaptiveSnapshotService adaptiveSnapshotService;
    private final SnapshotConfiguration config;
    private final MLEnhancedHotAccountDetector hotAccountDetector;
    private final EntriesSnapshotRepository snapshotRepository; // Добавить эту зависимость
    private final AccountRepository accountRepository;

    private final AtomicLong totalScheduledSnapshots = new AtomicLong(0);
    private final AtomicLong lastScheduledRun = new AtomicLong(0);

    /**
     * Daily snapshot creation task.
     * Runs every day at 2 AM by default to create snapshots for active accounts.
     */
    @Scheduled(cron = "${ledger.snapshots.scheduled.cron:0 0 2 * * *}")
    public void createDailySnapshots() {
        LocalDate snapshotDate = LocalDate.now().minusDays(1); // Previous day snapshots

        log.info("Starting scheduled daily snapshot creation for date: {}", snapshotDate);

        try {
            long startTime = System.currentTimeMillis();

            // Get list of accounts that need snapshots
            List<UUID> accountsForSnapshots = getAccountsForSnapshots();

            if (accountsForSnapshots.isEmpty()) {
                log.info("No accounts configured for scheduled snapshots");
                return;
            }

            // Create batch snapshots
            AdaptiveSnapshotService.BatchSnapshotResult result = adaptiveSnapshotService.createBatchSnapshots(
                accountsForSnapshots, snapshotDate
            );

            long duration = System.currentTimeMillis() - startTime;

            totalScheduledSnapshots.addAndGet(result.successfulSnapshots());
            lastScheduledRun.set(System.currentTimeMillis());

            log.info("Daily snapshot creation completed in {}ms: {} successful, {} failed, {} accounts",
                duration, result.successfulSnapshots(), result.failedSnapshots(), result.totalAccounts());

            if (result.failedSnapshots() > 0) {
                log.warn("Some scheduled snapshots failed - {} out of {} accounts",
                    result.failedSnapshots(), result.totalAccounts());
            }

        } catch (Exception e) {
            log.error("Scheduled snapshot creation failed: {}", e.getMessage(), e);
        }
    }

    /**
     * Manual snapshot creation for specific date (administrative function).
     *
     * @param snapshotDate Date to create snapshots for
     * @return Batch operation result
     */
    public AdaptiveSnapshotService.BatchSnapshotResult createSnapshotsForDate(LocalDate snapshotDate) {
        log.info("Creating manual snapshots for date: {}", snapshotDate);

        List<UUID> accountsForSnapshots = getAccountsForSnapshots();

        if (accountsForSnapshots.isEmpty()) {
            log.warn("No accounts configured for snapshot creation");
            return new AdaptiveSnapshotService.BatchSnapshotResult(0, 0, 0, 0, 0, snapshotDate);
        }

        try {
            AdaptiveSnapshotService.BatchSnapshotResult result = adaptiveSnapshotService.createBatchSnapshots(
                accountsForSnapshots, snapshotDate
            );

            log.info("Manual snapshot creation completed: {} successful, {} failed",
                result.successfulSnapshots(), result.failedSnapshots());

            return result;

        } catch (Exception e) {
            log.error("Manual snapshot creation failed for date {}: {}", snapshotDate, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Creates snapshots for specific accounts on given date.
     *
     * @param accountIds   List of account IDs
     * @param snapshotDate Date to create snapshots for
     * @return Batch operation result
     */
    public AdaptiveSnapshotService.BatchSnapshotResult createSnapshotsForAccounts(
        List<UUID> accountIds, LocalDate snapshotDate
    ) {
        if (accountIds.isEmpty()) {
            throw new IllegalArgumentException("Account IDs list cannot be empty");
        }

        log.info("Creating snapshots for {} specific accounts on date: {}", accountIds.size(), snapshotDate);

        try {
            AdaptiveSnapshotService.BatchSnapshotResult result = adaptiveSnapshotService.createBatchSnapshots(
                accountIds, snapshotDate
            );

            log.info("Targeted snapshot creation completed: {} successful, {} failed",
                result.successfulSnapshots(), result.failedSnapshots());

            return result;

        } catch (Exception e) {
            log.error("Targeted snapshot creation failed for date {}: {}", snapshotDate, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Gets scheduled snapshot statistics for monitoring.
     */
    public ScheduledSnapshotStats getScheduledStats() {
        return new ScheduledSnapshotStats(
            totalScheduledSnapshots.get(),
            lastScheduledRun.get(),
            getAccountsForSnapshots().size()
        );
    }

    public SnapshotCoverageReport validateSnapshotCoverage(int checkDays) {
        if (checkDays < 1) {
            throw new IllegalArgumentException("checkDays must be at least 1");
        }

        log.info("Validating snapshot coverage for last {} days", checkDays);

        try {
            List<UUID> configuredAccounts = getAccountsForSnapshots();

            if (configuredAccounts.isEmpty()) {
                log.info("No accounts configured for snapshot validation");
                return new SnapshotCoverageReport(0, 0, checkDays, LocalDate.now());
            }

            int totalExpectedSnapshots = 0;
            int missingSnapshots = 0;

            LocalDate endDate = LocalDate.now().minusDays(1); // Yesterday
            LocalDate startDate = endDate.minusDays(checkDays - 1);

            log.debug("Checking snapshot coverage from {} to {} for {} accounts",
                startDate, endDate, configuredAccounts.size());

            for (UUID accountId : configuredAccounts) {
                for (LocalDate date = startDate; !date.isAfter(endDate); date = date.plusDays(1)) {
                    totalExpectedSnapshots++;

                    try {
                        if (snapshotRepository.findExactSnapshot(accountId, date).isEmpty()) {
                            missingSnapshots++;
                            log.debug("Missing snapshot: account={}, date={}", accountId, date);
                        }
                    } catch (Exception e) {
                        log.warn("Error checking snapshot for account {} on date {}: {}",
                            accountId, date, e.getMessage());
                        missingSnapshots++; // Assume missing on error
                    }
                }
            }

            double coveragePercent = totalExpectedSnapshots > 0 ?
                ((double) (totalExpectedSnapshots - missingSnapshots) / totalExpectedSnapshots) * 100.0 : 100.0;

            log.info("Snapshot coverage validation completed: {}/{} snapshots present ({:.1f}% coverage)",
                totalExpectedSnapshots - missingSnapshots, totalExpectedSnapshots, coveragePercent);

            if (missingSnapshots > 0) {
                log.warn("Found {} missing snapshots out of {} expected", missingSnapshots, totalExpectedSnapshots);
            }

            return new SnapshotCoverageReport(
                configuredAccounts.size(),
                missingSnapshots,
                checkDays,
                LocalDate.now()
            );

        } catch (Exception e) {
            log.error("Error validating snapshot coverage: {}", e.getMessage(), e);

            // Return error report
            return new SnapshotCoverageReport(
                0, // unknown accounts
                -1, // error indicator
                checkDays,
                LocalDate.now()
            );
        }
    }

    private List<UUID> getAccountsForSnapshots() {
        try {
            // 1. Приоритет: конфигурация из properties
            List<String> configuredAccountStrings = config.getAccounts();
            if (configuredAccountStrings != null && !configuredAccountStrings.isEmpty()) {

                List<UUID> configuredAccounts = new ArrayList<>();
                for (String accountStr : configuredAccountStrings) {
                    try {
                        configuredAccounts.add(UUID.fromString(accountStr.trim()));
                    } catch (IllegalArgumentException e) {
                        log.warn("Invalid account UUID in configuration: {}", accountStr);
                    }
                }

                if (!configuredAccounts.isEmpty()) {
                    log.debug("Using {} configured accounts for snapshots", configuredAccounts.size());
                    return configuredAccounts;
                }
            }

            // 2. Fallback стратегии
            return switch (config.getFallbackStrategy()) {
                case ACTIVE -> getActiveAccountsFallback();
                case HOT -> getHotAccountsFallback();
                case NONE -> {
                    log.info("No accounts configured for snapshots and fallback strategy is NONE");
                    yield List.of();
                }
            };

        } catch (Exception e) {
            log.error("Error getting accounts for snapshots: {}", e.getMessage(), e);
            return List.of(); // Safe fallback
        }
    }

    private List<UUID> getActiveAccountsFallback() {
        try {
            log.info("Using ACTIVE accounts fallback strategy");

            // Ищем аккаунты с активностью за последние 30 дней
            List<UUID> recentlyActiveAccounts = accountRepository.findAccountsWithRecentActivity(30);

            if (!recentlyActiveAccounts.isEmpty()) {
                log.info("Found {} accounts with recent activity (last 30 days)", recentlyActiveAccounts.size());
                return recentlyActiveAccounts;
            }

            // Fallback к всем активным аккаунтам (когда-либо имевшим транзакции)
            List<UUID> allActiveAccounts = accountRepository.findActiveAccounts();
            log.info("No recent activity found, using {} accounts with any historical activity", allActiveAccounts.size());

            // Ограничиваем количество для разумного размера batch
            int maxAccounts = 100; // Configurable
            if (allActiveAccounts.size() > maxAccounts) {
                log.warn("Too many active accounts ({}), limiting to first {}", allActiveAccounts.size(), maxAccounts);
                return allActiveAccounts.subList(0, maxAccounts);
            }

            return allActiveAccounts;

        } catch (Exception e) {
            log.error("Error getting active accounts fallback: {}", e.getMessage(), e);
            return List.of();
        }
    }

    private List<UUID> getHotAccountsFallback() {
        try {
            log.info("Using HOT accounts fallback strategy");

            // Получаем горячие аккаунты с метриками
            List<MLEnhancedHotAccountDetector.HotAccountInfo> hotAccountsInfo =
                hotAccountDetector.getHotAccountsWithMetrics(0.5); // минимальный уровень активности

            List<UUID> hotAccounts = hotAccountsInfo.stream()
                .map(MLEnhancedHotAccountDetector.HotAccountInfo::accountId)
                .toList();

            if (!hotAccounts.isEmpty()) {
                log.info("Found {} hot accounts for snapshots (activity >= 0.5)", hotAccounts.size());

                // Логируем топ-5 самых горячих для мониторинга
                hotAccountsInfo.stream()
                    .limit(5)
                    .forEach(info -> log.debug("Hot account: {} (score: {:.2f}, reason: {})",
                        info.accountId(), info.activityScore(), info.reason()));

                return hotAccounts;
            }

            // Fallback: просто все detected hot accounts без metrics
            List<UUID> basicHotAccounts = hotAccountDetector.getAllHotAccounts();
            log.info("No hot accounts with metrics >= 0.5, using {} basic hot accounts", basicHotAccounts.size());

            return basicHotAccounts;

        } catch (Exception e) {
            log.error("Error getting hot accounts fallback: {}", e.getMessage(), e);
            return List.of();
        }
    }
    /**
     * Scheduled snapshot statistics for monitoring.
     */
    public record ScheduledSnapshotStats(
        long totalScheduledSnapshots,
        long lastScheduledRunTimestamp,
        int configuredAccounts
    ) {

    }

    /**
     * Snapshot coverage validation report.
     */
    public record SnapshotCoverageReport(
        int totalConfiguredAccounts,
        int missingSnapshots,
        int daysChecked,
        LocalDate reportDate
    ) {
        /**
         * Calculates coverage percentage.
         * @return coverage percentage (0-100), or -1 if error occurred
         */
        public double getCoveragePercentage() {
            if (missingSnapshots == -1) return -1; // Error indicator

            int totalExpected = totalConfiguredAccounts * daysChecked;
            if (totalExpected == 0) return 100.0;

            return ((double) (totalExpected - missingSnapshots) / totalExpected) * 100.0;
        }

        /**
         * Checks if coverage is acceptable (>= 95%).
         */
        public boolean isAcceptableCoverage() {
            double coverage = getCoveragePercentage();
            return coverage >= 95.0 && coverage != -1;
        }
    }}