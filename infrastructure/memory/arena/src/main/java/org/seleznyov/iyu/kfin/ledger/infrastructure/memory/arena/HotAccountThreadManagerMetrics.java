package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

@Data
@Builder
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class HotAccountThreadManagerMetrics {
    private final long totalBatchesProcessed;
    private final long totalEntriesProcessed;
    private final long totalErrors;
    private final double avgBatchProcessingTimeMs;
    private final double avgEntryProcessingTimeMicros;
    private final double ringBufferUtilization;
    private final long ringBufferSize;
    private final boolean ringBufferHealthy;
}