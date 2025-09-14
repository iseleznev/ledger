package org.seleznyov.iyu.kfin.ledger.presentation.rest.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.application.service.snapshot.AdaptiveSnapshotService;
import org.seleznyov.iyu.kfin.ledger.application.service.AdaptiveStrategyService;
import org.seleznyov.iyu.kfin.ledger.application.service.LedgerService;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

/**
 * Administrative REST controller for system monitoring and management.
 * Provides endpoints for system statistics, configuration, and diagnostic operations.
 */
@RestController
@RequestMapping("/api/v1/admin")
@RequiredArgsConstructor
@Validated
@Slf4j
public class AdminController {

    private final LedgerService ledgerService;
    private final AdaptiveStrategyService adaptiveStrategyService;
    private final AdaptiveSnapshotService adaptiveSnapshotService;
    private final CreateSnapshotUseCase createSnapshotUseCase;
    private final MLEnhancedHotAccountDetector hotAccountDetector;
    private final SlidingWindowMetrics slidingWindowMetrics;
    private final AsyncMLMetricsCollector metricsCollector;

    /**
     * Gets overall system statistics and health status.
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getSystemStats() {
        log.debug("Getting system statistics");

        try {
            var ledgerStats = ledgerService.getProcessingStats();
            var strategyStats = adaptiveStrategyService.getStrategyStats();
            var snapshotStats = adaptiveSnapshotService.getSnapshotStats();
            var detectionStats = hotAccountDetector.getDetectionStats();

            return ResponseEntity.ok(Map.of(
                "ledgerStats", ledgerStats,
                "strategyStats", strategyStats,
                "snapshotStats", snapshotStats,
                "detectionStats", detectionStats,
                "trackedAccounts", slidingWindowMetrics.getTrackedAccountsCount(),
                "timestamp", java.time.LocalDateTime.now()
            ));

        } catch (Exception e) {
            log.error("Error getting system stats: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                .body(Map.of("error", "Failed to retrieve system statistics"));
        }
    }

    /**
     * Gets detailed analysis for specific account.
     */
    @GetMapping("/accounts/{accountId}/analysis")
    public ResponseEntity<Map<String, Object>> getAccountAnalysis(@PathVariable UUID accountId) {
        log.debug("Getting detailed analysis for account: {}", accountId);

        try {
            var strategyAnalysis = adaptiveStrategyService.getDetailedAnalysis(accountId);
            var hotAccountAnalysis = hotAccountDetector.getDetailedAnalysis(accountId);
            var metricsSnapshot = slidingWindowMetrics.getMetrics(accountId);

            return ResponseEntity.ok(Map.of(
                "accountId", accountId,
                "strategyAnalysis", strategyAnalysis,
                "hotAccountAnalysis", hotAccountAnalysis,
                "metricsSnapshot", metricsSnapshot,
                "timestamp", java.time.LocalDateTime.now()
            ));

        } catch (Exception e) {
            log.error("Error getting account analysis for {}: {}", accountId, e.getMessage(), e);
            return ResponseEntity.internalServerError()
                .body(Map.of("error", "Failed to retrieve account analysis"));
        }
    }

    @PostMapping("/accounts/{accountId}/refresh-strategy")
    public ResponseEntity<Map<String, Object>> refreshAccountStrategy(@PathVariable UUID accountId) {
        log.info("Forcing strategy refresh for account: {}", accountId);

        try {
            // Refresh strategy (clears cache)
            adaptiveStrategyService.refreshAccountStrategy(accountId);

            // Get new strategy assignment with metadata
            AdaptiveStrategyService.StrategyAssignment newStrategyAssignment =
                adaptiveStrategyService.getCurrentStrategy(accountId);

            return ResponseEntity.ok(Map.of(
                "accountId", accountId,
                "message", "Strategy refreshed successfully",
                "strategy", newStrategyAssignment.strategy().name(),
                "assignedAt", newStrategyAssignment.assignedAt(),
                "reason", newStrategyAssignment.reason(),
                "timestamp", LocalDateTime.now()
            ));

        } catch (Exception e) {
            log.error("Error refreshing strategy for account {}: {}", accountId, e.getMessage(), e);
            return ResponseEntity.internalServerError()
                .body(Map.of(
                    "accountId", accountId,
                    "error", "Failed to refresh account strategy: " + e.getMessage(),
                    "timestamp", LocalDateTime.now()
                ));
        }
    }

    // Можно добавить дополнительный endpoint для получения strategy без refresh
    @GetMapping("/accounts/{accountId}/strategy")
    public ResponseEntity<Map<String, Object>> getAccountStrategy(@PathVariable UUID accountId) {
        log.debug("Getting current strategy for account: {}", accountId);

        try {
            AdaptiveStrategyService.StrategyAssignment strategyAssignment =
                adaptiveStrategyService.getCurrentStrategy(accountId);

            return ResponseEntity.ok(Map.of(
                "accountId", accountId,
                "strategy", strategyAssignment.strategy().name(),
                "assignedAt", strategyAssignment.assignedAt(),
                "reason", strategyAssignment.reason(),
                "isStale", strategyAssignment.isStale(2), // 2 minutes TTL
                "timestamp", LocalDateTime.now()
            ));

        } catch (Exception e) {
            log.error("Error getting strategy for account {}: {}", accountId, e.getMessage(), e);
            return ResponseEntity.internalServerError()
                .body(Map.of(
                    "accountId", accountId,
                    "error", "Failed to get account strategy: " + e.getMessage(),
                    "timestamp", LocalDateTime.now()
                ));
        }
    }    /**
     * Creates snapshot for specific account and date.
     */
    @PostMapping("/accounts/{accountId}/snapshots")
    public ResponseEntity<Map<String, Object>> createSnapshot(
        @PathVariable UUID accountId,
        @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate operationDate
    ) {
        LocalDate snapshotDate = operationDate != null ? operationDate : LocalDate.now();
        log.info("Creating snapshot for account: {}, date: {}", accountId, snapshotDate);

        try {
            var request = new CreateSnapshotUseCase.SnapshotRequest(accountId, snapshotDate);
            var result = createSnapshotUseCase.createSnapshot(request);

            if (result.success()) {
                return ResponseEntity.ok(Map.of(
                    "accountId", accountId,
                    "operationDate", snapshotDate,
                    "balance", result.balance(),
                    "operationsCount", result.operationsCount(),
                    "message", result.message(),
                    "timestamp", java.time.LocalDateTime.now()
                ));
            } else {
                return ResponseEntity.badRequest().body(Map.of(
                    "accountId", accountId,
                    "operationDate", snapshotDate,
                    "success", false,
                    "failureReason", result.failureReason(),
                    "message", result.message(),
                    "timestamp", java.time.LocalDateTime.now()
                ));
            }

        } catch (Exception e) {
            log.error("Error creating snapshot for account {} on {}: {}", accountId, snapshotDate, e.getMessage(), e);
            return ResponseEntity.internalServerError()
                .body(Map.of("error", "Failed to create snapshot"));
        }
    }

    /**
     * Updates system load factor for adaptive thresholds.
     */
    @PostMapping("/system-load")
    public ResponseEntity<Map<String, Object>> updateSystemLoad(
        @RequestParam double loadFactor
    ) {
        log.info("Updating system load factor: {}", loadFactor);

        try {
            if (loadFactor < 0.1 || loadFactor > 10.0) {
                return ResponseEntity.badRequest().body(Map.of(
                    "error", "Load factor must be between 0.1 and 10.0"
                ));
            }

            adaptiveStrategyService.updateSystemLoad(loadFactor);

            return ResponseEntity.ok(Map.of(
                "message", "System load factor updated successfully",
                "loadFactor", loadFactor,
                "timestamp", java.time.LocalDateTime.now()
            ));

        } catch (Exception e) {
            log.error("Error updating system load factor: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                .body(Map.of("error", "Failed to update system load factor"));
        }
    }

    /**
     * Reports false positive for ML tuning.
     */
    @PostMapping("/accounts/{accountId}/false-positive")
    public ResponseEntity<Map<String, Object>> reportFalsePositive(
        @PathVariable UUID accountId,
        @RequestParam String performance
    ) {
        log.info("Reporting false positive for account: {}, performance: {}", accountId, performance);

        try {
            adaptiveStrategyService.reportFalsePositive(accountId, performance);

            return ResponseEntity.ok(Map.of(
                "accountId", accountId,
                "message", "False positive reported successfully",
                "performance", performance,
                "timestamp", java.time.LocalDateTime.now()
            ));

        } catch (Exception e) {
            log.error("Error reporting false positive for account {}: {}", accountId, e.getMessage(), e);
            return ResponseEntity.internalServerError()
                .body(Map.of("error", "Failed to report false positive"));
        }
    }

    /**
     * Gets high activity accounts from metrics.
     */
    @GetMapping("/accounts/high-activity")
    public ResponseEntity<Map<String, Object>> getHighActivityAccounts(
        @RequestParam(defaultValue = "10.0") double burstThreshold,
        @RequestParam(defaultValue = "5.0") double shortThreshold
    ) {
        log.debug("Getting high activity accounts with thresholds: burst={}, short={}",
            burstThreshold, shortThreshold);

        try {
            var highActivityAccounts = slidingWindowMetrics.getHighActivityAccounts(
                burstThreshold, shortThreshold
            );

            return ResponseEntity.ok(Map.of(
                "highActivityAccounts", highActivityAccounts,
                "totalAccounts", highActivityAccounts.size(),
                "burstThreshold", burstThreshold,
                "shortThreshold", shortThreshold,
                "timestamp", java.time.LocalDateTime.now()
            ));

        } catch (Exception e) {
            log.error("Error getting high activity accounts: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                .body(Map.of("error", "Failed to retrieve high activity accounts"));
        }
    }

    /**
     * Clears metrics for specific account.
     */
    @DeleteMapping("/accounts/{accountId}/metrics")
    public ResponseEntity<Map<String, Object>> clearAccountMetrics(@PathVariable UUID accountId) {
        log.info("Clearing metrics for account: {}", accountId);

        try {
            boolean cleared = slidingWindowMetrics.clearAccountMetrics(accountId);
            adaptiveStrategyService.refreshAccountStrategy(accountId);

            return ResponseEntity.ok(Map.of(
                "accountId", accountId,
                "cleared", cleared,
                "message", cleared ? "Account metrics cleared successfully" : "No metrics found for account",
                "timestamp", java.time.LocalDateTime.now()
            ));

        } catch (Exception e) {
            log.error("Error clearing metrics for account {}: {}", accountId, e.getMessage(), e);
            return ResponseEntity.internalServerError()
                .body(Map.of("error", "Failed to clear account metrics"));
        }
    }

    /**
     * Gets system health check.
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        try {
            return ResponseEntity.ok(Map.of(
                "status", "healthy",
                "service", "ledger-admin",
                "timestamp", java.time.LocalDateTime.now(),
                "version", "1.0.0"
            ));

        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                "status", "unhealthy",
                "error", e.getMessage(),
                "timestamp", java.time.LocalDateTime.now()
            ));
        }
    }

    @GetMapping("/ml-metrics/stats")
    public ResponseEntity<AsyncMLMetricsCollector.AsyncMetricsStats> getMLMetricsStats() {
        return ResponseEntity.ok(metricsCollector.getStats());
    }

    @GetMapping("/ml-metrics/health")
    public ResponseEntity<String> checkMLMetricsHealth() {
        var stats = metricsCollector.getStats();

        // Проверяем что очередь не переполнена
        double utilizationPercent = (double) stats.currentQueueSize() /
            (stats.currentQueueSize() + stats.remainingCapacity()) * 100;

        if (utilizationPercent > 90) {
            return ResponseEntity.status(503).body("ML metrics queue overloaded: " + utilizationPercent + "%");
        }

        if (stats.totalEventsDropped() > 0) {
            return ResponseEntity.status(200).body("ML metrics healthy but " + stats.totalEventsDropped() + " events dropped");
        }

        return ResponseEntity.ok("ML metrics healthy");
    }
}