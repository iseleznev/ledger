package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.processor.batchprocessor;

import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.BatchRingBufferHandler;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.PostgresBinaryBatchLayout;

import java.lang.foreign.MemorySegment;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ✅ Logging BatchProcessor - для debug и мониторинга
 */
@Slf4j
public class LoggingBatchProcessor implements BatchProcessor {

    private final BatchProcessor delegate;
    private final AtomicLong processedBatches = new AtomicLong(0);
    private final AtomicLong processedEntries = new AtomicLong(0);

    public LoggingBatchProcessor(BatchProcessor delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean processBatch(BatchRingBufferHandler ringBufferHandler, long batchSlotOffset, long batchRawSize) {
        final MemorySegment ringBufferSegment = ringBufferHandler.ringBufferSegment();
        long startTime = System.nanoTime();

        // Читаем metadata для логирования
        UUID accountId = PostgresBinaryBatchLayout.readAccountId(ringBufferSegment, batchSlotOffset);
        int entryCount = PostgresBinaryBatchLayout.readEntryCount(ringBufferSegment, batchSlotOffset);
        long totalDelta = PostgresBinaryBatchLayout.readTotalDelta(ringBufferSegment, batchSlotOffset);

        log.info("Processing batch: account={}, entries={}, delta={}",
            accountId, entryCount, totalDelta);

        // Делегируем обработку
        boolean success = delegate.processBatch(ringBufferSegment, batchSlotOffset);

        long processingTime = System.nanoTime() - startTime;

        if (success) {
            processedBatches.incrementAndGet();
            processedEntries.addAndGet(entryCount);

            log.info("Successfully processed batch: account={}, {} entries in {}μs",
                accountId, entryCount, processingTime / 1_000);
        } else {
            log.error("Failed to process batch: account={}, {} entries after {}μs",
                accountId, entryCount, processingTime / 1_000);
        }

        return success;
    }

    public long getProcessedBatches() {
        return processedBatches.get();
    }

    public long getProcessedEntries() {
        return processedEntries.get();
    }
}
