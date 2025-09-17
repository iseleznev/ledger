package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.processor.batchprocessor;

import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.DirectPostgresBatchSender;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.BatchRingBufferHandler;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * ✅ Конкретная реализация BatchProcessor для PostgreSQL
 */

@Slf4j
public class PostgresBatchProcessor implements BatchProcessor {

    private final DirectPostgresBatchSender directSender;
    private final int workerId;

    public PostgresBatchProcessor(DirectPostgresBatchSender directSender, int workerId) {
        this.directSender = directSender;
        this.workerId = workerId;
    }

    @Override
    public long processBatch(BatchRingBufferHandler ringBufferHandler, long batchSlotOffset, long batchRawSize) {

        long startTime = System.nanoTime();

        try {
            // ✅ Читаем batch metadata
//            UUID accountId = batchLayout.checksum(ringBufferSegment, batchSlotOffset);
//            long totalDelta = batchLayout.tot(ringBufferSegment, batchSlotOffset);
            long entriesCount = ringBufferHandler.batchElementsCount(batchSlotOffset);
            long checksum = ringBufferHandler.batchChecksum(batchSlotOffset);
//            long timestamp = batchLayout(ringBufferSegment, batchSlotOffset);

//            log.debug("Worker {} processing batch: account={}, entries={}, delta={}",
//                workerId, accountId, entryCount, totalDelta);

            // ✅ Получаем готовый PostgreSQL binary segment
//            MemorySegment postgresBinarySegment = extractPostgresBinaryData(
//                ringBufferSegment, batchSlotOffset, entryCount);

            // ✅ Валидируем checksum
            if (!validateBatchChecksum(ringBufferHandler, batchSlotOffset, checksum)) {
                log.error("Worker {} batch checksum validation failed, found checksum {}",
                    workerId,
                    checksum
                );
                return 0;
            }

            // ✅ Отправляем в PostgreSQL
            final long copiedSize = directSender.sendDirectly(batchSlotOffset, batchRawSize);

            if (copiedSize == batchRawSize) {
                long processingTime = System.nanoTime() - startTime;
                log.debug("Worker {} successfully processed batch: {} bytes in {}μs",
                    workerId,
                    copiedSize,
                    processingTime / 1_000
                );
            } else {
                log.error("Worker {} insert mismatch: expected={}, actual={}",
                    workerId,
                    batchRawSize,
                    copiedSize
                );
            }
            return copiedSize;
        } catch (Exception e) {
            long processingTime = System.nanoTime() - startTime;
            log.error("Worker {} error processing batch after {}μs",
                workerId, processingTime / 1_000, e);
            return 0;
        }
    }

    /**
     * ✅ Извлекает PostgreSQL binary data из batch slot
     */
//    private MemorySegment extractPostgresBinaryData(MemorySegment ringSegment,
//                                                    long batchOffset,
//                                                    int entryCount) {
//
//        // Структура batch slot:
//        // [PostgreSQL_Header][Our_Metadata][PostgreSQL_Binary_Records][Terminator]
//
//        long postgresHeaderSize = POSTGRES_HEADER_SIZE;
//        long postgresDataSize = entryCount * PostgresBinaryEntryRecordLayout.POSTGRES_RECORD_SIZE;
//        long terminatorSize = 2;
//        long totalPostgresSize = postgresHeaderSize + postgresDataSize + terminatorSize;
//
//        // Возвращаем slice который содержит только PostgreSQL данные
//        return ringSegment.asSlice(batchOffset, totalPostgresSize);
//    }

    /**
     * ✅ Валидация checksum для безопасности
     */
    private boolean validateBatchChecksum(BatchRingBufferHandler ringBufferLayout, long batchOffset, long expectedChecksum) {
        try {
            // Пропускаем PostgreSQL header и вычисляем checksum от data
//            long dataOffset = POSTGRES_HEADER_SIZE;
//            long dataSize = ringBufferSegment.byteSize() - POSTGRES_HEADER_SIZE - 2; // minus terminator
            final MemorySegment ringBufferSegment = ringBufferLayout.ringBufferSegment();

            long actualChecksum = 0;
            long dataSize = ringBufferLayout.batchSize(batchOffset);

            // XOR checksum по 8-байтовым блокам
            for (long i = 0; i <= dataSize - 8; i += 8) {
                long value = ringBufferSegment.get(ValueLayout.JAVA_LONG, batchOffset + i);
                actualChecksum ^= value;
            }

            // Обрабатываем остаток
            for (long i = dataSize - (dataSize % 8); i < dataSize; i++) {
                actualChecksum ^= ringBufferSegment.get(ValueLayout.JAVA_BYTE, batchOffset + i);
            }

            boolean valid = actualChecksum == expectedChecksum;
            if (!valid) {
                log.warn("Checksum mismatch: expected={}, actual={}", expectedChecksum, actualChecksum);
            }

            return valid;

        } catch (Exception e) {
            log.error("Error validating checksum", e);
            return false;
        }
    }
}
