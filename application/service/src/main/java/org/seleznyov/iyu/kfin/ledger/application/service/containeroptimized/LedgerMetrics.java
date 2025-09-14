package org.seleznyov.iyu.kfin.ledger.application.service.containeroptimized;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Metrics collection for ledger operations
 * Thread-safe metrics collector optimized for high-throughput environments
 */
@Component
@Slf4j
public class LedgerMetrics {

    // Operation counters
    private final LongAdder totalOperations = new LongAdder();
    private final LongAdder successfulOperations = new LongAdder();
    private final LongAdder failedOperations = new LongAdder();

    // Latency tracking (using histogram buckets)
    private final LongAdder[] latencyBuckets = new LongAdder[10]; // 0-1ms, 1-5ms, 5-10ms, etc.
    private final AtomicLong maxLatencyNanos = new AtomicLong(0);
    private final AtomicLong minLatencyNanos = new AtomicLong(Long.MAX_VALUE);

    // Throughput tracking
    private volatile long lastThroughputCalculation = System.currentTimeMillis();
    private final AtomicLong operationsAtLastCalculation = new AtomicLong(0);
    private volatile double currentThroughput = 0.0;

    // Error tracking
    private final Map<String, LongAdder> errorsByType = new ConcurrentHashMap<>();

    // Balance operations
    private final LongAdder balanceReads = new LongAdder();
    private final LongAdder balanceWrites = new LongAdder();

    // WAL operations
    private final LongAdder walWrites = new LongAdder();
    private final LongAdder walFlushes = new LongAdder();

    // Snapshot operations
    private final LongAdder snapshotCreations = new LongAdder();
    private final LongAdder snapshotReads = new LongAdder();

    public LedgerMetrics() {
        for (int i = 0; i < latencyBuckets.length; i++) {
            latencyBuckets[i] = new LongAdder();
        }
    }

    /**
     * Record a successful operation
     */
    public void recordSuccessfulOperation(long latencyNanos) {
        totalOperations.increment();
        successfulOperations.increment();
        recordLatency(latencyNanos);
    }

    /**
     * Record a failed operation
     */
    public void recordFailedOperation(String errorType, long latencyNanos) {
        totalOperations.increment();
        failedOperations.increment();
        recordLatency(latencyNanos);

        errorsByType.computeIfAbsent(errorType, k -> new LongAdder()).increment();
    }

    /**
     * Record operation latency
     */
    private void recordLatency(long latencyNanos) {
        // Update min/max
        updateMinLatency(latencyNanos);
        updateMaxLatency(latencyNanos);

        // Record in appropriate bucket
        double latencyMs = latencyNanos / 1_000_000.0;
        int bucketIndex = getLatencyBucketIndex(latencyMs);
        latencyBuckets[bucketIndex].increment();
    }

    private void updateMinLatency(long latencyNanos) {
        long currentMin = minLatencyNanos.get();
        while (latencyNanos < currentMin && !minLatencyNanos.compareAndSet(currentMin, latencyNanos)) {
            currentMin = minLatencyNanos.get();
        }
    }

    private void updateMaxLatency(long latencyNanos) {
        long currentMax = maxLatencyNanos.get();
        while (latencyNanos > currentMax && !maxLatencyNanos.compareAndSet(currentMax, latencyNanos)) {
            currentMax = maxLatencyNanos.get();
        }
    }

    private int getLatencyBucketIndex(double latencyMs) {
        if (latencyMs < 1) return 0;          // <1ms
        if (latencyMs < 5) return 1;          // 1-5ms
        if (latencyMs < 10) return 2;         // 5-10ms
        if (latencyMs < 25) return 3;         // 10-25ms
        if (latencyMs < 50) return 4;         // 25-50ms
        if (latencyMs < 100) return 5;        // 50-100ms
        if (latencyMs < 250) return 6;        // 100-250ms
        if (latencyMs < 500) return 7;        // 250-500ms
        if (latencyMs < 1000) return 8;       // 500ms-1s
        return 9;                             // >1s
    }

    /**
     * Record balance operation
     */
    public void recordBalanceRead() {
        balanceReads.increment();
    }

    public void recordBalanceWrite() {
        balanceWrites.increment();
    }

    /**
     * Record WAL operation
     */
    public void recordWalWrite() {
        walWrites.increment();
    }

    public void recordWalFlush() {
        walFlushes.increment();
    }

    /**
     * Record snapshot operation
     */
    public void recordSnapshotCreation() {
        snapshotCreations.increment();
    }

    public void recordSnapshotRead() {
        snapshotReads.increment();
    }

    /**
     * Calculate current throughput
     */
    public double calculateCurrentThroughput() {
        long now = System.currentTimeMillis();
        long currentOps = totalOperations.sum();

        long timeDelta = now - lastThroughputCalculation;
        long opsDelta = currentOps - operationsAtLastCalculation.get();

        if (timeDelta > 5000) { // Update every 5 seconds
            if (timeDelta > 0) {
                currentThroughput = (opsDelta * 1000.0) / timeDelta;
            }

            lastThroughputCalculation = now;
            operationsAtLastCalculation.set(currentOps);
        }

        return currentThroughput;
    }

    /**
     * Get comprehensive metrics
     */
    public Map<String, Object> getMetrics() {
        long total = totalOperations.sum();
        long successful = successfulOperations.sum();
        long failed = failedOperations.sum();

        Map<String, Object> metrics = new ConcurrentHashMap<>();

        // Basic counters
        metrics.put("total_operations", total);
        metrics.put("successful_operations", successful);
        metrics.put("failed_operations", failed);
        metrics.put("success_rate_percent", total > 0 ? (successful * 100.0) / total : 100.0);

        // Throughput
        metrics.put("current_throughput_ops_per_sec", calculateCurrentThroughput());

        // Latency
        Map<String, Object> latency = new ConcurrentHashMap<>();
        latency.put("min_ms", minLatencyNanos.get() == Long.MAX_VALUE ? 0 : minLatencyNanos.get() / 1_000_000.0);
        latency.put("max_ms", maxLatencyNanos.get() / 1_000_000.0);

        // Latency distribution
        Map<String, Long> distribution = new ConcurrentHashMap<>();
        String[] bucketNames = {"<1ms", "1-5ms", "5-10ms", "10-25ms", "25-50ms",
            "50-100ms", "100-250ms", "250-500ms", "500ms-1s", ">1s"};
        for (int i = 0; i < latencyBuckets.length; i++) {
            distribution.put(bucketNames[i], latencyBuckets[i].sum());
        }
        latency.put("distribution", distribution);
        metrics.put("latency", latency);

        // Errors
        Map<String, Long> errors = new ConcurrentHashMap<>();
        errorsByType.forEach((type, count) -> errors.put(type, count.sum()));
        metrics.put("errors_by_type", errors);

        // Component metrics
        Map<String, Long> components = new ConcurrentHashMap<>();
        components.put("balance_reads", balanceReads.sum());
        components.put("balance_writes", balanceWrites.sum());
        components.put("wal_writes", walWrites.sum());
        components.put("wal_flushes", walFlushes.sum());
        components.put("snapshot_creations", snapshotCreations.sum());
        components.put("snapshot_reads", snapshotReads.sum());
        metrics.put("components", components);

        return metrics;
    }

    /**
     * Reset all metrics (for testing)
     */
    public void reset() {
        totalOperations.reset();
        successfulOperations.reset();
        failedOperations.reset();

        for (LongAdder bucket : latencyBuckets) {
            bucket.reset();
        }

        maxLatencyNanos.set(0);
        minLatencyNanos.set(Long.MAX_VALUE);

        errorsByType.clear();

        balanceReads.reset();
        balanceWrites.reset();
        walWrites.reset();
        walFlushes.reset();
        snapshotCreations.reset();
        snapshotReads.reset();

        lastThroughputCalculation = System.currentTimeMillis();
        operationsAtLastCalculation.set(0);
        currentThroughput = 0.0;

        log.info("Metrics reset completed");
    }

    /**
     * Get health status based on metrics
     */
    public boolean isHealthy() {
        long total = totalOperations.sum();
        if (total == 0) return true;

        long successful = successfulOperations.sum();
        double successRate = (successful * 100.0) / total;

        // Health criteria
        boolean successRateGood = successRate > 95.0;
        boolean latencyGood = maxLatencyNanos.get() < 1_000_000_000; // < 1 second
        boolean throughputReasonable = calculateCurrentThroughput() >= 0; // Just ensure it's calculated

        return successRateGood && latencyGood && throughputReasonable;
    }
}