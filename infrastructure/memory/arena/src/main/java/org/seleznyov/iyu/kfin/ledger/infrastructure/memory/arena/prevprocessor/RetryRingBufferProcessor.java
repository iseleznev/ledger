package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.prevprocessor;

import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.PostgresBinaryBatchLayout;

import java.lang.foreign.MemorySegment;
import java.util.UUID;

/**
 * ✅ Retry BatchProcessor - с автоматическими retry при ошибках
 */
@Slf4j
public class RetryRingBufferProcessor implements PostgresBinaryBatchLayout.BatchProcessor {

    private final PostgresBinaryBatchLayout.BatchProcessor delegate;
    private final int maxRetries;
    private final long retryDelayMs;

    public RetryRingBufferProcessor(PostgresBinaryBatchLayout.BatchProcessor delegate,
                                    int maxRetries, long retryDelayMs) {
        this.delegate = delegate;
        this.maxRetries = maxRetries;
        this.retryDelayMs = retryDelayMs;
    }

    @Override
    public boolean processBatch(MemorySegment ringBufferSegment, long batchSlotOffset) {

        UUID accountId = PostgresBinaryBatchLayout.readAccountId(ringBufferSegment, batchSlotOffset);
        int entryCount = PostgresBinaryBatchLayout.readEntryCount(ringBufferSegment, batchSlotOffset);

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                boolean success = delegate.processBatch(ringBufferSegment, batchSlotOffset);

                if (success) {
                    if (attempt > 1) {
                        log.info("Batch processing succeeded on attempt {}: account={}, {} entries",
                            attempt, accountId, entryCount);
                    }
                    return true;
                } else {
                    log.warn("Batch processing failed on attempt {}: account={}, {} entries",
                        attempt, accountId, entryCount);
                }

            } catch (Exception e) {
                log.warn("Exception on attempt {} processing batch: account={}, {} entries",
                    attempt, accountId, entryCount, e);
            }

            // Пауза перед retry (кроме последней попытки)
            if (attempt < maxRetries) {
                try {
                    Thread.sleep(retryDelayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("Interrupted during retry delay");
                    break;
                }
            }
        }

        log.error("Failed to process batch after {} attempts: account={}, {} entries",
            maxRetries, accountId, entryCount);
        return false;
    }
}
