package org.seleznyov.iyu.kfin.ledger.infrastructure.detection;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Sliding window metrics collector for account activity analysis.
 * Uses multiple time windows with exponential weighted moving averages (EWMA)
 * to track account transaction patterns and detect hot account behavior.
 * <p>
 * Time windows:
 * - 5 seconds: Burst detection
 * - 1 minute: Short-term activity patterns
 * - 5 minutes: Medium-term trends
 * <p>
 * Metrics collected:
 * - Transaction count per window
 * - Average transaction amount
 * - Peak transaction rate
 * - Time since last activity
 */
@Component
@Slf4j
public class SlidingWindowMetrics {

    // Sliding window configurations
    private static final int BURST_WINDOW_SECONDS = 5;
    private static final int SHORT_WINDOW_SECONDS = 60;
    private static final int MEDIUM_WINDOW_SECONDS = 300;

    // EWMA decay factors (higher = more recent data weight)
    private static final double BURST_DECAY_FACTOR = 0.9;
    private static final double SHORT_DECAY_FACTOR = 0.7;
    private static final double MEDIUM_DECAY_FACTOR = 0.5;

    // Memory management
    private static final int MAX_TRACKED_ACCOUNTS = 10000;
    private static final long CLEANUP_INTERVAL_MINUTES = 5;
    private static final long ACCOUNT_TTL_MINUTES = 30;

    // Account metrics storage
    private final ConcurrentHashMap<UUID, AccountMetrics> accountMetrics = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cleanupExecutor = Executors.newSingleThreadScheduledExecutor(
        r -> new Thread(r, "metrics-cleanup")
    );

    @PostConstruct
    public void initialize() {
        log.info("Initializing SlidingWindowMetrics with windows: {}s, {}s, {}s",
            BURST_WINDOW_SECONDS, SHORT_WINDOW_SECONDS, MEDIUM_WINDOW_SECONDS);

        // Start periodic cleanup of stale metrics
        cleanupExecutor.scheduleAtFixedRate(
            this::cleanupStaleMetrics,
            CLEANUP_INTERVAL_MINUTES,
            CLEANUP_INTERVAL_MINUTES,
            TimeUnit.MINUTES
        );

        log.info("SlidingWindowMetrics initialized successfully");
    }

    /**
     * Records transaction activity for account.
     * Updates all sliding window metrics with EWMA calculations.
     *
     * @param accountId Account that performed transaction
     * @param amount    Transaction amount
     * @param isDebit   True if debit operation, false if credit
     */
    public void recordTransaction(UUID accountId, long amount, boolean isDebit) {
        long currentTime = System.currentTimeMillis();

        AccountMetrics metrics = accountMetrics.computeIfAbsent(accountId, id -> new AccountMetrics(currentTime));

        synchronized (metrics) {
            // Update transaction counts
            metrics.burstWindow.increment();
            metrics.shortWindow.increment();
            metrics.mediumWindow.increment();

            // Update amount tracking
            metrics.totalAmount.add(amount);
            metrics.transactionCount.increment();

            // Track debit/credit ratios
            if (isDebit) {
                metrics.debitCount.increment();
            } else {
                metrics.creditCount.increment();
            }

            // Update EWMA rates
            updateEWMAMetrics(metrics, currentTime);

            // Update last activity timestamp
            metrics.lastActivityTime = currentTime;

            log.debug("Transaction recorded: account={}, amount={}, debit={}, burstRate={}",
                accountId, amount, isDebit, metrics.burstRate);
        }
    }

    /**
     * Gets current metrics snapshot for account.
     *
     * @param accountId Account to get metrics for
     * @return Current metrics or null if account not tracked
     */
    public AccountMetricsSnapshot getMetrics(UUID accountId) {
        AccountMetrics metrics = accountMetrics.get(accountId);
        if (metrics == null) {
            return null;
        }

        long currentTime = System.currentTimeMillis();

        synchronized (metrics) {
            // Update EWMA before returning snapshot
            updateEWMAMetrics(metrics, currentTime);

            return new AccountMetricsSnapshot(
                accountId,
                metrics.burstRate,
                metrics.shortRate,
                metrics.mediumRate,
                metrics.transactionCount.sum(),
                metrics.debitCount.sum(),
                metrics.creditCount.sum(),
                metrics.totalAmount.sum(),
                currentTime - metrics.lastActivityTime,
                currentTime
            );
        }
    }

    /**
     * Checks if account shows burst activity pattern.
     *
     * @param accountId Account to check
     * @param threshold Burst threshold (transactions per second)
     * @return True if account is experiencing burst activity
     */
    public boolean isBurstActive(UUID accountId, double threshold) {
        AccountMetricsSnapshot snapshot = getMetrics(accountId);
        return snapshot != null && snapshot.burstRate() > threshold;
    }

    /**
     * Gets accounts currently showing high activity.
     *
     * @param burstThreshold Minimum burst rate
     * @param shortThreshold Minimum short-term rate
     * @return Map of active accounts with their metrics
     */
    public Map<UUID, AccountMetricsSnapshot> getHighActivityAccounts(
        double burstThreshold,
        double shortThreshold
    ) {
        Map<UUID, AccountMetricsSnapshot> activeAccounts = new ConcurrentHashMap<>();

        accountMetrics.forEach((accountId, metrics) -> {
            AccountMetricsSnapshot snapshot = getMetrics(accountId);
            if (snapshot != null &&
                (snapshot.burstRate() > burstThreshold || snapshot.shortRate() > shortThreshold)) {
                activeAccounts.put(accountId, snapshot);
            }
        });

        return activeAccounts;
    }

    /**
     * Gets total number of tracked accounts.
     */
    public int getTrackedAccountsCount() {
        return accountMetrics.size();
    }

    /**
     * Forces cleanup of specific account metrics (administrative function).
     */
    public boolean clearAccountMetrics(UUID accountId) {
        return accountMetrics.remove(accountId) != null;
    }

    private void updateEWMAMetrics(AccountMetrics metrics, long currentTime) {
        long timeDelta = currentTime - metrics.lastUpdateTime;

        if (timeDelta > 0) {
            // Update EWMA rates for each window
            updateEWMARate(metrics.burstWindow, timeDelta, BURST_WINDOW_SECONDS * 1000, BURST_DECAY_FACTOR, metrics);
            updateEWMARate(metrics.shortWindow, timeDelta, SHORT_WINDOW_SECONDS * 1000, SHORT_DECAY_FACTOR, metrics);
            updateEWMARate(metrics.mediumWindow, timeDelta, MEDIUM_WINDOW_SECONDS * 1000, MEDIUM_DECAY_FACTOR, metrics);

            metrics.lastUpdateTime = currentTime;
        }
    }

    private void updateEWMARate(
        WindowMetrics window,
        long timeDelta,
        long windowSizeMs,
        double decayFactor,
        AccountMetrics accountMetrics
    ) {
        // Calculate decay based on time elapsed
        double alpha = 1.0 - Math.exp(-((double) timeDelta) / (windowSizeMs * decayFactor));

        // Current rate in transactions per second
        double currentRate = (double) window.sum() / (timeDelta / 1000.0);

        // Apply EWMA
        if (window == accountMetrics.burstWindow) {
            accountMetrics.burstRate = (1.0 - alpha) * accountMetrics.burstRate + alpha * currentRate;
        } else if (window == accountMetrics.shortWindow) {
            accountMetrics.shortRate = (1.0 - alpha) * accountMetrics.shortRate + alpha * currentRate;
        } else if (window == accountMetrics.mediumWindow) {
            accountMetrics.mediumRate = (1.0 - alpha) * accountMetrics.mediumRate + alpha * currentRate;
        }

        // Reset window counter for next period
        window.reset();
    }

    private void cleanupStaleMetrics() {
        long currentTime = System.currentTimeMillis();
        long ttlMs = ACCOUNT_TTL_MINUTES * 60 * 1000;

        int removedCount = 0;
        var iterator = accountMetrics.entrySet().iterator();

        while (iterator.hasNext()) {
            var entry = iterator.next();
            AccountMetrics metrics = entry.getValue();

            if (currentTime - metrics.lastActivityTime > ttlMs) {
                iterator.remove();
                removedCount++;
            }
        }

        // Also enforce maximum account limit
        if (accountMetrics.size() > MAX_TRACKED_ACCOUNTS) {
            // Remove oldest accounts beyond limit
            accountMetrics.entrySet().stream()
                .sorted((e1, e2) -> Long.compare(e1.getValue().lastActivityTime, e2.getValue().lastActivityTime))
                .limit(accountMetrics.size() - MAX_TRACKED_ACCOUNTS)
                .map(Map.Entry::getKey)
                .forEach(accountMetrics::remove);
        }

        if (removedCount > 0) {
            log.debug("Cleaned up {} stale account metrics, {} accounts remaining",
                removedCount, accountMetrics.size());
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down SlidingWindowMetrics");
        cleanupExecutor.shutdown();

        try {
            if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            cleanupExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        log.info("SlidingWindowMetrics shutdown completed");
    }

    /**
     * Internal account metrics storage.
     */
    private static class AccountMetrics {

        final WindowMetrics burstWindow = new WindowMetrics();
        final WindowMetrics shortWindow = new WindowMetrics();
        final WindowMetrics mediumWindow = new WindowMetrics();

        final LongAdder totalAmount = new LongAdder();
        final LongAdder transactionCount = new LongAdder();
        final LongAdder debitCount = new LongAdder();
        final LongAdder creditCount = new LongAdder();

        volatile double burstRate = 0.0;
        volatile double shortRate = 0.0;
        volatile double mediumRate = 0.0;

        volatile long lastActivityTime;
        volatile long lastUpdateTime;

        AccountMetrics(long creationTime) {
            this.lastActivityTime = creationTime;
            this.lastUpdateTime = creationTime;
        }
    }

    /**
     * Window-specific metrics with atomic counters.
     */
    private static class WindowMetrics {

        private final AtomicLong counter = new AtomicLong(0);

        void increment() {
            counter.incrementAndGet();
        }

        long sum() {
            return counter.get();
        }

        void reset() {
            counter.set(0);
        }
    }

    /**
     * Immutable snapshot of account metrics at point in time.
     */
    public record AccountMetricsSnapshot(
        UUID accountId,
        double burstRate,           // Transactions per second in 5s window
        double shortRate,           // Transactions per second in 1m window
        double mediumRate,          // Transactions per second in 5m window
        long totalTransactions,
        long debitCount,
        long creditCount,
        long totalAmount,
        long timeSinceLastActivity, // Milliseconds since last transaction
        long snapshotTime
    ) {

    }
}