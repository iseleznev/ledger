package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.processor.batchprocessor;

import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.PostgreSqlEntryRecordBatchRingBufferHandler;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.PostgresBinaryBatchLayout;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ✅ Metrics BatchProcessor - собирает детальную статистику
 */
@Slf4j
public class MetricsBatchProcessor implements BatchProcessor {

    private final PostgresBinaryBatchLayout.BatchProcessor delegate;

    // Метрики
    private final AtomicLong totalBatches = new AtomicLong(0);
    private final AtomicLong totalEntries = new AtomicLong(0);
    private final AtomicLong totalProcessingTime = new AtomicLong(0);
    private final AtomicLong successfulBatches = new AtomicLong(0);
    private final AtomicLong failedBatches = new AtomicLong(0);

    public MetricsBatchProcessor(PostgresBinaryBatchLayout.BatchProcessor delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean processBatch(PostgreSqlEntryRecordBatchRingBufferHandler ringBufferLayout, long batchSlotOffset, long batchRawSize) {
        final MemorySegment ringBufferSegment = ringBufferLayout.ringBufferSegment();
        long startTime = System.nanoTime();
        int entryCount = PostgresBinaryBatchLayout.readEntryCount(ringBufferSegment, batchSlotOffset);

        totalBatches.incrementAndGet();
        totalEntries.addAndGet(entryCount);

        boolean success = delegate.processBatch(ringBufferSegment, batchSlotOffset);

        long processingTime = System.nanoTime() - startTime;
        totalProcessingTime.addAndGet(processingTime);

        if (success) {
            successfulBatches.incrementAndGet();
        } else {
            failedBatches.incrementAndGet();
        }

        // Периодический вывод метрик
        if (totalBatches.get() % 1000 == 0) {
            logMetrics();
        }

        return success;
    }

    private void logMetrics() {
        long batches = totalBatches.get();
        long entries = totalEntries.get();
        long processingTime = totalProcessingTime.get();
        long successful = successfulBatches.get();
        long failed = failedBatches.get();

        double avgBatchTime = batches > 0 ? (double) processingTime / batches / 1_000_000 : 0; // ms
        double avgEntryTime = entries > 0 ? (double) processingTime / entries / 1_000 : 0; // μs
        double successRate = batches > 0 ? (double) successful / batches * 100 : 0; // %

        log.info("BatchProcessor metrics: batches={}, entries={}, success_rate={:.1f}%, " +
                "avg_batch_time={:.2f}ms, avg_entry_time={:.2f}μs",
            batches, entries, successRate, avgBatchTime, avgEntryTime);
    }

    // Getters для метрик
    public long getTotalBatches() {
        return totalBatches.get();
    }

    public long getTotalEntries() {
        return totalEntries.get();
    }

    public long getSuccessfulBatches() {
        return successfulBatches.get();
    }

    public long getFailedBatches() {
        return failedBatches.get();
    }

    public double getSuccessRate() {
        long total = totalBatches.get();
        return total > 0 ? (double) successfulBatches.get() / total * 100 : 0;
    }
}
