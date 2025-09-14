package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.processor.batchprocessor;

import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.PostgresBinaryBatchLayout;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.PostgresBinaryEntryRecordLayout;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.UUID;

/**
 * ✅ Validation BatchProcessor - проверяет данные перед обработкой
 */
@Slf4j
public class ValidationBatchProcessor implements PostgresBinaryBatchLayout.BatchProcessor {

    private final PostgresBinaryBatchLayout.BatchProcessor delegate;

    public ValidationBatchProcessor(PostgresBinaryBatchLayout.BatchProcessor delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean processBatch(MemorySegment ringBufferSegment, long batchSlotOffset) {

        // ✅ Валидация batch metadata
        if (!validateBatchMetadata(ringBufferSegment, batchSlotOffset)) {
            return false;
        }

        // ✅ Валидация PostgreSQL binary data
        if (!validatePostgresBinaryData(ringBufferSegment, batchSlotOffset)) {
            return false;
        }

        // Делегируем обработку
        return delegate.processBatch(ringBufferSegment, batchSlotOffset);
    }

    private boolean validateBatchMetadata(MemorySegment ringSegment, long batchOffset) {
        try {
            UUID accountId = PostgresBinaryBatchLayout.readAccountId(ringSegment, batchOffset);
            int entryCount = PostgresBinaryBatchLayout.readEntryCount(ringSegment, batchOffset);
            long timestamp = PostgresBinaryBatchLayout.readTimestamp(ringSegment, batchOffset);

            // Проверки
            if (accountId == null) {
                log.error("Invalid batch: null accountId");
                return false;
            }

            if (entryCount <= 0 || entryCount > 10000) {
                log.error("Invalid batch: invalid entryCount={}", entryCount);
                return false;
            }

            long currentTime = System.currentTimeMillis();
            if (timestamp > currentTime || timestamp < currentTime - 24 * 60 * 60 * 1000) {
                log.warn("Suspicious batch timestamp: {} (current: {})", timestamp, currentTime);
            }

            return true;

        } catch (Exception e) {
            log.error("Error validating batch metadata", e);
            return false;
        }
    }

    private boolean validatePostgresBinaryData(MemorySegment ringSegment, long batchOffset) {
        try {
            int entryCount = PostgresBinaryBatchLayout.readEntryCount(ringSegment, batchOffset);

            // Проверяем PostgreSQL header signature
            long dataOffset = batchOffset + PostgresBinaryBatchLayout.BATCH_DATA_OFFSET;

            byte[] expectedSignature = "PGCOPY\n\377\r\n\0".getBytes();
            for (int i = 0; i < expectedSignature.length; i++) {
                byte actual = ringSegment.get(ValueLayout.JAVA_BYTE, dataOffset + i);
                if (actual != expectedSignature[i]) {
                    log.error("Invalid PostgreSQL signature at position {}: expected={}, actual={}",
                        i, expectedSignature[i], actual);
                    return false;
                }
            }

            // Проверяем что данные не выходят за границы slot'а
            long expectedDataSize = 19 + // PostgreSQL header
                entryCount * PostgresBinaryEntryRecordLayout.POSTGRES_RECORD_SIZE +
                2; // terminator

            long slotSize = PostgresBinaryBatchLayout.this.batchSlotSize;
            if (expectedDataSize > slotSize) {
                log.error("Data size {} exceeds slot size {}", expectedDataSize, slotSize);
                return false;
            }

            return true;

        } catch (Exception e) {
            log.error("Error validating PostgreSQL binary data", e);
            return false;
        }
    }
}
