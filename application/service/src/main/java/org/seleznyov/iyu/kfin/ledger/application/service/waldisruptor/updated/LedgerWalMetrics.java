package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor.updated;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Production-ready metrics with proper Spring integration
 */
@Component
@Slf4j
public class LedgerWalMetrics {

    private final MeterRegistry meterRegistry;
    private final Counter walWriteCounter;
    private final Counter walFlushCounter;
    private final Counter walErrorCounter;
    private final Timer walWriteTimer;
    private final Timer walFlushTimer;
    private final DistributionSummary batchSizeDistribution;

    public LedgerWalMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;

        this.walWriteCounter = Counter.builder("ledger.wal.writes")
            .description("Total number of WAL writes")
            .register(meterRegistry);

        this.walFlushCounter = Counter.builder("ledger.wal.flushes")
            .description("Total number of WAL batch flushes")
            .register(meterRegistry);

        this.walErrorCounter = Counter.builder("ledger.wal.errors")
            .description("Total number of WAL errors")
            .tag("type", "unknown")
            .register(meterRegistry);

        this.walWriteTimer = Timer.builder("ledger.wal.write.duration")
            .description("Time taken to write to WAL")
            .register(meterRegistry);

        this.walFlushTimer = Timer.builder("ledger.wal.flush.duration")
            .description("Time taken to flush WAL batch")
            .register(meterRegistry);

        this.batchSizeDistribution = DistributionSummary.builder("ledger.wal.batch.size")
            .description("Distribution of WAL batch sizes")
            .register(meterRegistry);
    }

    public void recordWrite() {
        walWriteCounter.increment();
    }

    public void recordFlush(int batchSize, Duration duration) {
        walFlushCounter.increment();
        walFlushTimer.record(duration);
        batchSizeDistribution.record(batchSize);
    }

    public void recordError(String errorType) {
        Counter.builder("ledger.wal.errors")
            .tag("type", errorType)
            .register(meterRegistry)
            .increment();
    }

    public Timer.Sample startWriteTimer() {
        return Timer.start(meterRegistry);
    }

    public void registerRingBufferUtilizationGauge(VirtualThreadLedgerWalService walService) {
        Gauge.builder("ledger.wal.ringbuffer.utilization")
            .description("Ring buffer utilization percentage")
            .register(meterRegistry, walService, service -> {
                long total = service.getRingBufferSize();
                long remaining = service.getRingBufferRemaining();
                return total > 0 ? ((double) (total - remaining) / total) * 100.0 : 0.0;
            });
    }
}
