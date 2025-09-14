package org.seleznyov.iyu.kfin.ledger.infrastructure.detection;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * ML-Enhanced hot account detection service using multi-tier analysis.
 * Combines real-time metrics with behavioral pattern recognition and
 * predictive modeling to identify accounts requiring high-performance processing.
 * <p>
 * Detection Tiers:
 * - Tier 1: Burst detection (100ms response) - immediate DISRUPTOR activation
 * - Tier 2: Short-term patterns (30 sec analysis) - sustained high activity
 * - Tier 3: ML prediction (5 min ahead) - proactive strategy switching
 * <p>
 * Features:
 * - Adaptive thresholds based on system load
 * - Temporal behavior modeling (hourly/daily patterns)
 * - False positive minimization through multi-signal correlation
 * - Self-tuning based on historical performance
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class MLEnhancedHotAccountDetector {

    private final SlidingWindowMetrics slidingWindowMetrics;

    // Detection thresholds (adaptive)
    private final AtomicReference<Double> burstThreshold = new AtomicReference<>(10.0);
    private final AtomicReference<Double> shortTermThreshold = new AtomicReference<>(5.0);
    private final AtomicReference<Double> mediumTermThreshold = new AtomicReference<>(2.0);

    // Behavioral pattern storage
    private final ConcurrentHashMap<UUID, AccountBehaviorProfile> behaviorProfiles = new ConcurrentHashMap<>();

    // System load adaptation
    private volatile double systemLoadFactor = 1.0;     // 1.0 = normal, >1.0 = high load
    private final AtomicLong totalDetections = new AtomicLong(0);
    private final AtomicLong falsePositives = new AtomicLong(0);

    @PostConstruct
    public void initialize() {
        log.info("Initializing ML-Enhanced Hot Account Detector");
        log.info("Initial thresholds - Burst: {}, Short: {}, Medium: {}",
            burstThreshold, shortTermThreshold, mediumTermThreshold);
    }


    /**
     * Updates behavior profile asynchronously (called from background thread).
     * This method is called from AsyncMLMetricsCollector, not from hot path.
     */
    public void updateBehaviorProfileAsync(UUID accountId, long amount, boolean isDebit) {
        updateBehaviorProfile(accountId, amount, isDebit);
    }

//    /**
//     * Records transaction for ML analysis and updates behavioral patterns.
//     * Should be called for every transaction processed.
//     *
//     * @param accountId Account performing transaction
//     * @param amount    Transaction amount
//     * @param isDebit   True for debit, false for credit
//     */
//    public void recordTransaction(UUID accountId, long amount, boolean isDebit) {
//        // Update sliding window metrics
//        slidingWindowMetrics.recordTransaction(accountId, amount, isDebit);
//
//        // Update behavioral profile
//        updateBehaviorProfile(accountId, amount, isDebit);
//    }

    /**
     * Determines if account should use DISRUPTOR strategy.
     * Multi-tier analysis with different response times.
     *
     * @param accountId Account to analyze
     * @return true if account requires high-performance processing
     */
    public boolean isHotAccount(UUID accountId) {
        // Tier 1: Immediate burst detection (~1ms response time)
        if (detectBurstActivity(accountId)) {
            log.debug("Hot account detected via burst analysis: {}", accountId);
            totalDetections.incrementAndGet();
            return true;
        }

        // Tier 2: Short-term pattern analysis (~10ms response time)
        if (detectShortTermActivity(accountId)) {
            log.debug("Hot account detected via short-term analysis: {}", accountId);
            totalDetections.incrementAndGet();
            return true;
        }

        // Tier 3: ML-based predictive analysis (~50ms response time)
        if (detectPredictiveActivity(accountId)) {
            log.debug("Hot account detected via predictive analysis: {}", accountId);
            totalDetections.incrementAndGet();
            return true;
        }

        return false;
    }

    /**
     * Gets detailed analysis for account (administrative/debugging).
     *
     * @param accountId Account to analyze
     * @return Detailed detection analysis
     */
    public HotAccountAnalysis getDetailedAnalysis(UUID accountId) {
        var metrics = slidingWindowMetrics.getMetrics(accountId);
        var profile = behaviorProfiles.get(accountId);

        if (metrics == null) {
            return new HotAccountAnalysis(
                accountId, false, "No metrics available",
                0.0, 0.0, 0.0, null, LocalDateTime.now()
            );
        }

        // Calculate detection scores
        double burstScore = calculateBurstScore(metrics);
        double shortScore = calculateShortTermScore(metrics);
        double mlScore = calculateMLScore(accountId, metrics, profile);

        boolean isHot = burstScore > 1.0 || shortScore > 1.0 || mlScore > 1.0;
        String reason = buildDetectionReason(burstScore, shortScore, mlScore);

        return new HotAccountAnalysis(
            accountId, isHot, reason, burstScore, shortScore, mlScore,
            profile, LocalDateTime.now()
        );
    }

    /**
     * Updates system load factor to adapt thresholds.
     * Higher load = lower thresholds (more accounts go to DISRUPTOR).
     *
     * @param currentLoad System load factor (1.0 = normal, 2.0 = high load)
     */
    public void updateSystemLoad(double currentLoad) {
        this.systemLoadFactor = currentLoad;

        // Adaptive threshold adjustment
        double adaptiveBurst = burstThreshold.get() / Math.max(currentLoad, 0.5);
        double adaptiveShort = shortTermThreshold.get() / Math.max(currentLoad, 0.5);

        log.debug("System load updated: {}, adaptive thresholds: burst={}, short={}",
            currentLoad, adaptiveBurst, adaptiveShort);
    }

    /**
     * Reports false positive detection for self-tuning.
     *
     * @param accountId Account that was incorrectly classified as hot
     */
    public void reportFalsePositive(UUID accountId) {
        falsePositives.incrementAndGet();

        // Thread-safe обновление thresholds
        if (falsePositives.get() % 10 == 0) {
            burstThreshold.updateAndGet(current -> current * 1.05);
            shortTermThreshold.updateAndGet(current -> current * 1.05);

            log.info("Adjusted thresholds after {} false positives: burst={}, short={}",
                falsePositives.get(), burstThreshold.get(), shortTermThreshold.get());
        }
    }

    /**
     * Gets detection statistics for monitoring.
     */
    public DetectionStats getDetectionStats() {
        double falsePositiveRate = totalDetections.get() > 0 ?
            (double) falsePositives.get() / totalDetections.get() : 0.0;

        return new DetectionStats(
            totalDetections.get(),
            falsePositives.get(),
            falsePositiveRate,
            burstThreshold.get(),    // Используем get()
            shortTermThreshold.get(), // Используем get()
            systemLoadFactor,
            behaviorProfiles.size()
        );
    }

    /**
     * Gets all currently tracked hot accounts.
     * Based on behavioral profiles and recent activity.
     *
     * @return List of account IDs that are currently considered hot
     */
    public List<UUID> getAllHotAccounts() {
        List<UUID> hotAccounts = new ArrayList<>();

        // Проверяем все аккаунты в behavioral profiles
        for (Map.Entry<UUID, AccountBehaviorProfile> entry : behaviorProfiles.entrySet()) {
            UUID accountId = entry.getKey();
            AccountBehaviorProfile profile = entry.getValue();

            // Проверяем активность за последние N минут
            long timeSinceActivity = System.currentTimeMillis() - profile.lastActivityTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            boolean recentlyActive = timeSinceActivity < 10 * 60 * 1000; // 10 минут

            if (recentlyActive && isHotAccount(accountId)) {
                hotAccounts.add(accountId);
            }
        }

        log.debug("Found {} hot accounts from {} tracked profiles", hotAccounts.size(), behaviorProfiles.size());
        return hotAccounts;
    }

    /**
     * Gets hot accounts with detailed metrics for snapshot scheduling.
     *
     * @param minActivityLevel Minimum activity level threshold
     * @return List of hot accounts with their activity scores
     */
    public List<HotAccountInfo> getHotAccountsWithMetrics(double minActivityLevel) {
        List<HotAccountInfo> hotAccountsInfo = new ArrayList<>();

        for (Map.Entry<UUID, AccountBehaviorProfile> entry : behaviorProfiles.entrySet()) {
            UUID accountId = entry.getKey();

            try {
                var analysis = getDetailedAnalysis(accountId);
                if (analysis.isHot()) {
                    double activityScore = Math.max(analysis.burstScore(),
                        Math.max(analysis.shortTermScore(), analysis.mlScore()));

                    if (activityScore >= minActivityLevel) {
                        hotAccountsInfo.add(new HotAccountInfo(
                            accountId,
                            activityScore,
                            analysis.reason()
                        ));
                    }
                }
            } catch (Exception e) {
                log.warn("Error analyzing account {} for hot account metrics: {}", accountId, e.getMessage());
            }
        }

        // Сортируем по activity score (самые активные первые)
        hotAccountsInfo.sort((a, b) -> Double.compare(b.activityScore(), a.activityScore()));

        log.debug("Found {} hot accounts with activity level >= {}", hotAccountsInfo.size(), minActivityLevel);
        return hotAccountsInfo;
    }

    /**
     * Hot account information with metrics.
     */
    public record HotAccountInfo(
        UUID accountId,
        double activityScore,
        String reason
    ) {}

    private boolean detectBurstActivity(UUID accountId) {
        var metrics = slidingWindowMetrics.getMetrics(accountId);
        if (metrics == null) return false;

        double adaptiveThreshold = burstThreshold.get() / systemLoadFactor;
        return metrics.burstRate() > adaptiveThreshold;
    }

    private boolean detectShortTermActivity(UUID accountId) {
        var metrics = slidingWindowMetrics.getMetrics(accountId);
        if (metrics == null) return false;

        double adaptiveThreshold = shortTermThreshold.get() / systemLoadFactor;

        // Multi-signal analysis
        boolean highShortRate = metrics.shortRate() > adaptiveThreshold;
        boolean sustainedActivity = metrics.timeSinceLastActivity() < 5000; // Active within 5 seconds
        boolean balancedOperations = calculateOperationBalance(metrics) > 0.3; // Mixed debit/credit

        return highShortRate && sustainedActivity && balancedOperations;
    }

    private boolean detectPredictiveActivity(UUID accountId) {
        var metrics = slidingWindowMetrics.getMetrics(accountId);
        var profile = behaviorProfiles.get(accountId);

        if (metrics == null || profile == null) return false;

        // Temporal pattern analysis
        double hourlyPattern = analyzeHourlyPattern(profile);
        double weeklyPattern = analyzeWeeklyPattern(profile);
        double trendPrediction = analyzeTrendPrediction(metrics, profile);

        // Combined ML score
        double mlScore = (hourlyPattern * 0.4 + weeklyPattern * 0.3 + trendPrediction * 0.3);

        return mlScore > 0.7; // 70% confidence threshold
    }

    private void updateBehaviorProfile(UUID accountId, long amount, boolean isDebit) {
        LocalDateTime now = LocalDateTime.now();

        behaviorProfiles.compute(accountId, (id, profile) -> {
            if (profile == null) {
                profile = new AccountBehaviorProfile(id, now);
            }

            // Update hourly patterns
            int hourOfDay = now.getHour();
            profile.hourlyActivity[hourOfDay]++;

            // Update weekly patterns
            int dayOfWeek = now.getDayOfWeek().getValue() - 1; // 0-6
            profile.weeklyActivity[dayOfWeek]++;

            // Update recent activity buffer (circular buffer)
            int bufferIndex = (int) (profile.totalTransactions % profile.recentActivity.length);
            profile.recentActivity[bufferIndex] = new ActivityRecord(now, amount, isDebit);

            profile.totalTransactions++;
            profile.lastActivityTime = now;

            return profile;
        });
    }

    private double calculateBurstScore(SlidingWindowMetrics.AccountMetricsSnapshot metrics) {
        return metrics.burstRate() / (burstThreshold.get() / systemLoadFactor);
    }

    private double calculateShortTermScore(SlidingWindowMetrics.AccountMetricsSnapshot metrics) {
        double baseScore = metrics.shortRate() / (shortTermThreshold.get() / systemLoadFactor);
        double activityBonus = metrics.timeSinceLastActivity() < 10000 ? 1.2 : 1.0;
        double balanceBonus = calculateOperationBalance(metrics) > 0.5 ? 1.1 : 1.0;

        return baseScore * activityBonus * balanceBonus;
    }

    private double calculateMLScore(UUID accountId, SlidingWindowMetrics.AccountMetricsSnapshot metrics,
                                    AccountBehaviorProfile profile) {
        if (profile == null) return 0.0;

        double hourlyScore = analyzeHourlyPattern(profile);
        double weeklyScore = analyzeWeeklyPattern(profile);
        double trendScore = analyzeTrendPrediction(metrics, profile);

        return (hourlyScore + weeklyScore + trendScore) / 3.0;
    }

    private double calculateOperationBalance(SlidingWindowMetrics.AccountMetricsSnapshot metrics) {
        long total = metrics.debitCount() + metrics.creditCount();
        if (total == 0) return 0.0;

        double ratio = Math.min(metrics.debitCount(), metrics.creditCount()) / (double) total;
        return ratio; // 0.5 = perfect balance, 0.0 = all one type
    }

    private double analyzeHourlyPattern(AccountBehaviorProfile profile) {
        int currentHour = LocalDateTime.now().getHour();
        long currentHourActivity = profile.hourlyActivity[currentHour];
        long averageHourlyActivity = Arrays.stream(profile.hourlyActivity).sum() / 24;

        if (averageHourlyActivity == 0) return 0.0;

        return Math.min(currentHourActivity / (double) averageHourlyActivity, 2.0) / 2.0;
    }

    private double analyzeWeeklyPattern(AccountBehaviorProfile profile) {
        int currentDay = LocalDateTime.now().getDayOfWeek().getValue() - 1;
        long currentDayActivity = profile.weeklyActivity[currentDay];
        long averageDailyActivity = Arrays.stream(profile.weeklyActivity).sum() / 7;

        if (averageDailyActivity == 0) return 0.0;

        return Math.min(currentDayActivity / (double) averageDailyActivity, 2.0) / 2.0;
    }

    private double analyzeTrendPrediction(SlidingWindowMetrics.AccountMetricsSnapshot metrics,
                                          AccountBehaviorProfile profile) {
        // Simple trend analysis based on recent vs medium term activity
        double recentTrend = metrics.shortRate() / Math.max(metrics.mediumRate(), 0.1);
        return Math.min(recentTrend, 2.0) / 2.0;
    }

    private String buildDetectionReason(double burstScore, double shortScore, double mlScore) {
        List<String> reasons = new ArrayList<>();

        if (burstScore > 1.0) reasons.add("burst activity (" + String.format("%.2f", burstScore) + ")");
        if (shortScore > 1.0) reasons.add("sustained activity (" + String.format("%.2f", shortScore) + ")");
        if (mlScore > 1.0) reasons.add("ML prediction (" + String.format("%.2f", mlScore) + ")");

        return reasons.isEmpty() ? "no significant activity" : String.join(", ", reasons);
    }

    /**
     * Account behavioral profile for ML analysis.
     */
    private static class AccountBehaviorProfile {

        final UUID accountId;
        final LocalDateTime createdAt;
        final long[] hourlyActivity = new long[24];    // Activity per hour of day
        final long[] weeklyActivity = new long[7];     // Activity per day of week
        final ActivityRecord[] recentActivity = new ActivityRecord[100]; // Circular buffer

        long totalTransactions = 0;
        LocalDateTime lastActivityTime;

        AccountBehaviorProfile(UUID accountId, LocalDateTime createdAt) {
            this.accountId = accountId;
            this.createdAt = createdAt;
            this.lastActivityTime = createdAt;
        }
    }

    /**
     * Individual activity record for pattern analysis.
     */
    private record ActivityRecord(LocalDateTime timestamp, long amount, boolean isDebit) {

    }

    /**
     * Detailed hot account analysis result.
     */
    public record HotAccountAnalysis(
        UUID accountId,
        boolean isHot,
        String reason,
        double burstScore,
        double shortTermScore,
        double mlScore,
        AccountBehaviorProfile profile,
        LocalDateTime analysisTime
    ) {

    }

    /**
     * Detection statistics for monitoring.
     */
    public record DetectionStats(
        long totalDetections,
        long falsePositives,
        double falsePositiveRate,
        double currentBurstThreshold,
        double currentShortThreshold,
        double systemLoadFactor,
        int trackedAccounts
    ) {

    }
}