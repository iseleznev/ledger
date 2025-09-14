package org.seleznyov.iyu.kfin.ledger.presentation.grpc.mapper;

import org.seleznyov.iyu.kfin.ledger.application.service.snapshot.AdaptiveSnapshotService;
import org.seleznyov.iyu.kfin.ledger.application.service.AdaptiveStrategyService;
import org.seleznyov.iyu.kfin.ledger.application.service.LedgerService;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Mapper for converting administrative data to gRPC messages.
 */
@Component
public class GrpcAdminMapper {

    /**
     * Builds system statistics response.
     */
    public SystemStatsResponse buildSystemStatsResponse(
        LedgerService.CombinedProcessingStats ledgerStats,
        AdaptiveStrategyService.StrategyStats strategyStats,
        AdaptiveSnapshotService.CombinedSnapshotStats snapshotStats,
        MLEnhancedHotAccountDetector.DetectionStats detectionStats,
        int trackedAccounts
    ) {
        return SystemStatsResponse.newBuilder()
            .setLedgerStats(buildLedgerStats(ledgerStats))
            .setStrategyStats(buildStrategyStats(strategyStats))
            .setSnapshotStats(buildSnapshotStats(snapshotStats))
            .setDetectionStats(buildDetectionStats(detectionStats))
            .setTrackedAccounts(trackedAccounts)
            .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(System.currentTimeMillis() / 1000)
                .setNanos((int) ((System.currentTimeMillis() % 1000) * 1_000_000))
                .build())
            .build();
    }

    /**
     * Builds account analysis response.
     */
    public AccountAnalysisResponse buildAccountAnalysisResponse(
        UUID accountId,
        AdaptiveStrategyService.DetailedStrategyAnalysis strategyAnalysis,
        MLEnhancedHotAccountDetector.HotAccountAnalysis hotAccountAnalysis,
        SlidingWindowMetrics.AccountMetricsSnapshot metricsSnapshot
    ) {
        AccountAnalysisResponse.Builder responseBuilder = AccountAnalysisResponse.newBuilder()
            .setAccountId(accountId.toString())
            .setStrategyAnalysis(buildStrategyAnalysis(strategyAnalysis))
            .setHotAccountAnalysis(buildHotAccountAnalysis(hotAccountAnalysis))
            .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(System.currentTimeMillis() / 1000)
                .setNanos((int) ((System.currentTimeMillis() % 1000) * 1_000_000))
                .build());

        if (metricsSnapshot != null) {
            responseBuilder.setMetricsSnapshot(buildMetricsSnapshot(metricsSnapshot));
        }

        return responseBuilder.build();
    }

    private LedgerStats buildLedgerStats(LedgerService.CombinedProcessingStats stats) {
        return LedgerStats.newBuilder()
            .setSimpleStats(ProcessingStats.newBuilder()
                .setStrategy(stats.simpleStats().strategy())
                .setProcessedCount(stats.simpleStats().processedCount())
                .setFailedCount(stats.simpleStats().failedCount())
                .setAverageProcessingTimeMs(stats.simpleStats().averageProcessingTimeMs())
                .build())
            .setDisruptorStats(ProcessingStats.newBuilder()
                .setStrategy(stats.disruptorStats().strategy())
                .setProcessedCount(stats.disruptorStats().processedCount())
                .setFailedCount(stats.disruptorStats().failedCount())
                .setAverageProcessingTimeMs(stats.disruptorStats().averageProcessingTimeMs())
                .build())
            .build();
    }

    private StrategyStats buildStrategyStats(AdaptiveStrategyService.StrategyStats stats) {
        return StrategyStats.newBuilder()
            .setSimpleAssignments(stats.simpleAssignments())
            .setDisruptorAssignments(stats.disruptorAssignments())
            .setTotalCachedAssignments(stats.totalCachedAssignments())
            .setTotalMlDetections(stats.totalMLDetections())
            .setFalsePositives(stats.falsePositives())
            .setFalsePositiveRate(stats.falsePositiveRate())
            .build();
    }

    private SnapshotStats buildSnapshotStats(AdaptiveSnapshotService.CombinedSnapshotStats stats) {
        return SnapshotStats.newBuilder()
            .setSimpleStats(ProcessingStats.newBuilder()
                .setStrategy(stats.simpleStats().strategy())
                .setProcessedCount(stats.simpleStats().snapshotsCreated())
                .setFailedCount(stats.simpleStats().failedCreations())
                .setAverageProcessingTimeMs(stats.simpleStats().averageCreationTimeMs())
                .build())
            .setDisruptorStats(ProcessingStats.newBuilder()
                .setStrategy(stats.disruptorStats().strategy())
                .setProcessedCount(stats.disruptorStats().snapshotsCreated())
                .setFailedCount(stats.disruptorStats().failedCreations())
                .setAverageProcessingTimeMs(stats.disruptorStats().averageCreationTimeMs())
                .build())
            .setTotalSnapshotsCreated(stats.totalSnapshotsCreated())
            .setTotalFailedCreations(stats.totalFailedCreations())
            .build();
    }

    private DetectionStats buildDetectionStats(MLEnhancedHotAccountDetector.DetectionStats stats) {
        return DetectionStats.newBuilder()
            .setTotalDetections(stats.totalDetections())
            .setFalsePositives(stats.falsePositives())
            .setFalsePositiveRate(stats.falsePositiveRate())
            .setCurrentBurstThreshold(stats.currentBurstThreshold())
            .setCurrentShortThreshold(stats.currentShortThreshold())
            .setSystemLoadFactor(stats.systemLoadFactor())
            .setTrackedAccounts(stats.trackedAccounts())
            .build();
    }

    private StrategyAnalysis buildStrategyAnalysis(AdaptiveStrategyService.DetailedStrategyAnalysis analysis) {
        StrategyAnalysis.Builder builder = StrategyAnalysis.newBuilder()
            .setRecommendedStrategy(analysis.recommendedStrategy().name())
            .setStrategyReason(analysis.strategyReason());

        if (analysis.cachedStrategy() != null) {
            builder.setCachedStrategy(analysis.cachedStrategy().name());
        }

        return builder.build();
    }

    private HotAccountAnalysis buildHotAccountAnalysis(MLEnhancedHotAccountDetector.HotAccountAnalysis analysis) {
        return HotAccountAnalysis.newBuilder()
            .setIsHot(analysis.isHot())
            .setReason(analysis.reason())
            .setBurstScore(analysis.burstScore())
            .setShortTermScore(analysis.shortTermScore())
            .setMlScore(analysis.mlScore())
            .build();
    }

    private MetricsSnapshot buildMetricsSnapshot(SlidingWindowMetrics.AccountMetricsSnapshot snapshot) {
        return MetricsSnapshot.newBuilder()
            .setBurstRate(snapshot.burstRate())
            .setShortRate(snapshot.shortRate())
            .setMediumRate(snapshot.mediumRate())
            .setTotalTransactions(snapshot.totalTransactions())
            .setDebitCount(snapshot.debitCount())
            .setCreditCount(snapshot.creditCount())
            .setTotalAmount(snapshot.totalAmount())
            .setTimeSinceLastActivity(snapshot.timeSinceLastActivity())
            .build();
    }
}