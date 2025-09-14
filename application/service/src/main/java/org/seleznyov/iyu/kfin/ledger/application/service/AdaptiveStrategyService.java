package org.seleznyov.iyu.kfin.ledger.application.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.domain.model.ProcessingStrategy;
import org.seleznyov.iyu.kfin.ledger.infrastructure.detection.AsyncMLMetricsCollector;
import org.seleznyov.iyu.kfin.ledger.infrastructure.detection.MLEnhancedHotAccountDetector;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service responsible for selecting optimal processing strategy for accounts.
 * Uses ML-enhanced hot account detection with adaptive thresholds and
 * behavioral pattern analysis to determine SIMPLE vs DISRUPTOR routing.
 *
 * Strategy Selection Logic:
 * - SIMPLE: Cold accounts with low/predictable activity
 * - DISRUPTOR: Hot accounts detected via ML analysis (burst, patterns, predictions)
 *
 * Features:
 * - Real-time transaction recording for ML analysis
 * - Strategy caching with TTL to reduce ML overhead
 * - Fallback mechanisms for reliability
 * - Performance monitoring and false positive tracking
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class AdaptiveStrategyService {

    private final MLEnhancedHotAccountDetector hotAccountDetector;
    private final AsyncMLMetricsCollector asyncMetricsCollector;


    // Strategy assignment cache (reduces ML computation overhead)
    private final Map<UUID, StrategyAssignment> strategyCache = new ConcurrentHashMap<>();

    // Strategy assignment TTL (shorter than before due to ML accuracy)
    private static final long STRATEGY_TTL_MINUTES = 2; // 2 minutes for responsive switching

    /**
     * Records transaction for ML analysis asynchronously.
     * Non-blocking call that queues event for background processing.
     */
    public void recordTransaction(UUID accountId, long amount, boolean isDebit) {
        // Асинхронная отправка в очередь - не блокирует hot path
        asyncMetricsCollector.recordTransactionAsync(accountId, amount, isDebit);

        // Убираем синхронный вызов:
        // hotAccountDetector.recordTransaction(accountId, amount, isDebit);

        log.debug("Transaction queued for async ML analysis: account={}, amount={}, debit={}",
            accountId, amount, isDebit);
    }

    /**
     * Selects optimal processing strategy for transfer operation.
     * Uses ML-enhanced detection with caching for performance.
     *
     * @param debitAccountId Source account (primary decision factor)
     * @param creditAccountId Target account (secondary factor)
     * @param amount Transfer amount (influences ML decision)
     * @return Recommended processing strategy
     */
    public ProcessingStrategy selectStrategy(UUID debitAccountId, UUID creditAccountId, long amount) {
        log.debug("Selecting strategy for transfer: {} -> {}, amount: {}",
            debitAccountId, creditAccountId, amount);

        // Record transaction for ML learning (debit account is primary)
        recordTransaction(debitAccountId, amount, true);
        recordTransaction(creditAccountId, amount, false);

        // Check cache first for performance
        StrategyAssignment cachedStrategy = strategyCache.get(debitAccountId);
        if (cachedStrategy != null && !isStale(cachedStrategy)) {
            log.debug("Using cached strategy {} for account: {}",
                cachedStrategy.strategy(), debitAccountId);
            return cachedStrategy.strategy();
        }

        // Determine strategy using ML detection
        ProcessingStrategy selectedStrategy = determineStrategyML(debitAccountId, amount);

        // Cache the assignment
        String reason = buildAssignmentReason(debitAccountId, selectedStrategy);
        strategyCache.put(debitAccountId, new StrategyAssignment(
            selectedStrategy,
            LocalDateTime.now(),
            reason
        ));

        log.info("Selected {} strategy for account: {} (amount: {}, reason: {})",
            selectedStrategy, debitAccountId, amount, reason);

        return selectedStrategy;
    }

    /**
     * Selects strategy for balance queries (lighter weight decision).
     * Uses cached assignments or quick ML check.
     *
     * @param accountId Account to query
     * @return Recommended processing strategy
     */
    public ProcessingStrategy selectStrategyForBalance(UUID accountId) {
        log.debug("Selecting strategy for balance query: {}", accountId);

        // Check cache first
        StrategyAssignment cached = strategyCache.get(accountId);
        if (cached != null && !isStale(cached)) {
            return cached.strategy();
        }

        // Quick ML check for balance queries
        boolean isHot = hotAccountDetector.isHotAccount(accountId);
        ProcessingStrategy strategy = isHot ? ProcessingStrategy.DISRUPTOR : ProcessingStrategy.SIMPLE;

        // Update cache
        strategyCache.put(accountId, new StrategyAssignment(
            strategy,
            LocalDateTime.now(),
            "Balance query ML check: " + (isHot ? "hot detected" : "cold account")
        ));

        return strategy;
    }

    /**
     * Forces strategy refresh for specific account (admin/testing purposes).
     * Clears cache and triggers fresh ML analysis.
     *
     * @param accountId Account to refresh
     */
    public void refreshAccountStrategy(UUID accountId) {
        log.info("Forcing strategy refresh for account: {}", accountId);
        strategyCache.remove(accountId);

        // Trigger fresh evaluation
        selectStrategyForBalance(accountId);
    }

    /**
     * Reports false positive detection for ML model tuning.
     *
     * @param accountId Account incorrectly classified as hot
     * @param actualPerformance Observed performance characteristics
     */
    public void reportFalsePositive(UUID accountId, String actualPerformance) {
        log.warn("False positive reported for account {}: {}", accountId, actualPerformance);

        // Clear cached strategy to force re-evaluation
        strategyCache.remove(accountId);

        // Report to ML detector for threshold adjustment
        hotAccountDetector.reportFalsePositive(accountId);
    }

    /**
     * Updates system load factor for adaptive threshold adjustment.
     *
     * @param systemLoad Current system load (1.0 = normal, >1.0 = high load)
     */
    public void updateSystemLoad(double systemLoad) {
        log.debug("Updating system load factor: {}", systemLoad);
        hotAccountDetector.updateSystemLoad(systemLoad);

        // Clear strategy cache to apply new thresholds
        if (systemLoad > 1.5) { // High load - force re-evaluation
            int cacheSize = strategyCache.size();
            strategyCache.clear();
            log.info("Cleared strategy cache ({} entries) due to high system load: {}",
                cacheSize, systemLoad);
        }
    }

    /**
     * Gets strategy selection statistics for monitoring.
     *
     * @return Current strategy statistics with ML detector data
     */
    public StrategyStats getStrategyStats() {
        long simpleCount = strategyCache.values().stream()
            .mapToLong(assignment -> assignment.strategy() == ProcessingStrategy.SIMPLE ? 1 : 0)
            .sum();

        long disruptorCount = strategyCache.values().stream()
            .mapToLong(assignment -> assignment.strategy() == ProcessingStrategy.DISRUPTOR ? 1 : 0)
            .sum();

        // Get ML detector statistics
        var detectionStats = hotAccountDetector.getDetectionStats();

        return new StrategyStats(
            simpleCount,
            disruptorCount,
            strategyCache.size(),
            LocalDateTime.now(),
            detectionStats.totalDetections(),
            detectionStats.falsePositives(),
            detectionStats.falsePositiveRate()
        );
    }

    /**
     * Gets detailed analysis for account (administrative/debugging).
     *
     * @param accountId Account to analyze
     * @return Detailed ML analysis and strategy reasoning
     */
    public DetailedStrategyAnalysis getDetailedAnalysis(UUID accountId) {
        StrategyAssignment cached = strategyCache.get(accountId);
        var mlAnalysis = hotAccountDetector.getDetailedAnalysis(accountId);

        ProcessingStrategy recommendedStrategy = mlAnalysis.isHot() ?
            ProcessingStrategy.DISRUPTOR : ProcessingStrategy.SIMPLE;

        return new DetailedStrategyAnalysis(
            accountId,
            recommendedStrategy,
            cached != null ? cached.strategy() : null,
            mlAnalysis,
            cached != null ? cached.reason() : "No cached strategy",
            LocalDateTime.now()
        );
    }

    /**
     * Clears stale strategy assignments from cache.
     * Should be called periodically by scheduled task.
     */
    public void cleanupStaleAssignments() {
        int removedCount = 0;
        var iterator = strategyCache.entrySet().iterator();

        while (iterator.hasNext()) {
            var entry = iterator.next();
            if (isStale(entry.getValue())) {
                iterator.remove();
                removedCount++;
            }
        }

        if (removedCount > 0) {
            log.debug("Cleaned up {} stale strategy assignments", removedCount);
        }
    }

    private ProcessingStrategy determineStrategyML(UUID accountId, long amount) {
        // Use ML enhanced detection
        boolean isHot = hotAccountDetector.isHotAccount(accountId);

        if (isHot) {
            log.debug("ML detector classified account {} as HOT", accountId);
            return ProcessingStrategy.DISRUPTOR;
        }

        // Additional business rule: very large amounts prefer SIMPLE for additional safety
        if (amount > 10_000_000_00L) { // > 10M in cents
            log.debug("Large amount detected ({}), preferring SIMPLE strategy for safety", amount);
            return ProcessingStrategy.SIMPLE;
        }

        log.debug("ML detector classified account {} as COLD", accountId);
        return ProcessingStrategy.SIMPLE;
    }

    private String buildAssignmentReason(UUID accountId, ProcessingStrategy strategy) {
        var mlAnalysis = hotAccountDetector.getDetailedAnalysis(accountId);

        if (strategy == ProcessingStrategy.DISRUPTOR) {
            return "ML detected hot: " + mlAnalysis.reason();
        } else {
            return "ML classified cold: " + mlAnalysis.reason();
        }
    }

    private boolean isStale(StrategyAssignment assignment) {
        return assignment.assignedAt().isBefore(LocalDateTime.now().minusMinutes(STRATEGY_TTL_MINUTES));
    }

    /**
     * Gets current processing strategy for account.
     * Used for administrative monitoring and debugging.
     *
     * @param accountId Account to check
     * @return Current strategy assignment with metadata
     */
    public StrategyAssignment getCurrentStrategy(UUID accountId) {
        log.debug("Getting current strategy for account: {}", accountId);

        try {
            // Check cache first
            StrategyAssignment cached = strategyCache.get(accountId);
            if (cached != null && !isStale(cached)) {
                log.debug("Returning cached strategy for account {}: {}", accountId, cached.strategy());
                return cached;
            }

            // No valid cache - determine fresh strategy
            ProcessingStrategy strategy = selectStrategyForBalance(accountId);

            // This will update cache as side effect
            StrategyAssignment newAssignment = strategyCache.get(accountId);
            if (newAssignment != null) {
                log.debug("Returning fresh strategy for account {}: {}", accountId, newAssignment.strategy());
                return newAssignment;
            }

            // Fallback: create temporary assignment without caching
            String reason = buildAssignmentReason(accountId, strategy);
            StrategyAssignment tempAssignment = new StrategyAssignment(
                strategy,
                LocalDateTime.now(),
                reason
            );

            log.debug("Returning temporary strategy for account {}: {}", accountId, strategy);
            return tempAssignment;

        } catch (Exception e) {
            log.error("Error getting current strategy for account {}: {}", accountId, e.getMessage(), e);

            // Error fallback
            return new StrategyAssignment(
                ProcessingStrategy.SIMPLE, // Safe fallback
                LocalDateTime.now(),
                "Error determining strategy: " + e.getMessage()
            );
        }
    }

    /**
     * Gets current strategy as simple enum (for backward compatibility).
     *
     * @param accountId Account to check
     * @return Processing strategy enum
     */
    public ProcessingStrategy getCurrentStrategySimple(UUID accountId) {
        return getCurrentStrategy(accountId).strategy();
    }

    // Сделать StrategyAssignment публичным для использования в AdminController
    public record StrategyAssignment(
        ProcessingStrategy strategy,
        LocalDateTime assignedAt,
        String reason
    ) {
        public boolean isStale(long ttlMinutes) {
            return assignedAt.isBefore(LocalDateTime.now().minusMinutes(ttlMinutes));
        }
    }
    /**
     * Enhanced strategy selection statistics including ML data.
     */
    public record StrategyStats(
        long simpleAssignments,
        long disruptorAssignments,
        long totalCachedAssignments,
        LocalDateTime lastUpdated,
        long totalMLDetections,
        long falsePositives,
        double falsePositiveRate
    ) {}

    /**
     * Detailed strategy analysis for administrative purposes.
     */
    public record DetailedStrategyAnalysis(
        UUID accountId,
        ProcessingStrategy recommendedStrategy,
        ProcessingStrategy cachedStrategy,
        MLEnhancedHotAccountDetector.HotAccountAnalysis mlAnalysis,
        String strategyReason,
        LocalDateTime analysisTime
    ) {}
}