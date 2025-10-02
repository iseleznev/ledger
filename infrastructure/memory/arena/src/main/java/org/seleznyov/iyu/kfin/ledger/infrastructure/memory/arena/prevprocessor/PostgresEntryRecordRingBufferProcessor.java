package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.prevprocessor;

import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.domain.model.entryrecord.EntryRecord;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.EntryRecordDirectPostgresBatchSender;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.RingBufferHandler;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * ✅ Конкретная реализация BatchProcessor для PostgreSQL
 */

@Slf4j
public class PostgresEntryRecordRingBufferProcessor implements RingBufferProcessor<EntryRecord> {

    static final String STAGE_TABLE_NAME_PREFIX = "stage_entry_records";

    private static final int POSTGRES_SIGNATURE_OFFSET = 0;             // 11 bytes "PGCOPY\n\377\r\n\0"
    private static final int POSTGRES_FLAGS_OFFSET = 11;                // 4 bytes
    private static final int POSTGRES_EXTENSION_OFFSET = 15;            // 4 bytes
    private static final int POSTGRES_HEADER_SIZE = 19;

    private final EntryRecordDirectPostgresBatchSender directSender;
    private final int workerId;
    private final byte[] postgresSignature = {
        'P', 'G', 'C', 'O', 'P', 'Y', '\n',
        (byte) 0xFF, '\r', '\n', 0
    };
    private final byte[] reservedPrefix = new  byte[POSTGRES_HEADER_SIZE];
    private final byte[] reusableChunk;

    public PostgresEntryRecordRingBufferProcessor(EntryRecordDirectPostgresBatchSender directSender, int maxBatchSize, int workerId) {
        this.directSender = directSender;
        this.workerId = workerId;
        this.reusableChunk = new byte[maxBatchSize];
    }

    @Override
    public long process(RingBufferHandler<EntryRecord> ringBufferHandler, long batchSlotOffset, long batchSize) {

        long startTime = System.nanoTime();

        try {
            // ✅ Читаем batch metadata
//            UUID accountId = batchLayout.checksum(ringBufferSegment, batchSlotOffset);
//            long totalDelta = batchLayout.tot(ringBufferSegment, batchSlotOffset);
//            long entriesCount = ringBufferHandler.batchElementsCount(batchSlotOffset);
            long checksum = ringBufferHandler.batchChecksum(batchSlotOffset, batchSize);
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
            MemorySegment.copy(
                ringBufferHandler.memorySegment(),
                batchSlotOffset - POSTGRES_HEADER_SIZE,
                MemorySegment.ofArray(reservedPrefix),
                0,
                postgresSignature.length
            );

            MemorySegment.copy(
                MemorySegment.ofArray(postgresSignature),
                0,
                ringBufferHandler.memorySegment(),
                batchSlotOffset - POSTGRES_HEADER_SIZE,
                postgresSignature.length
            );

            ringBufferHandler.memorySegment().set(ValueLayout.JAVA_INT, batchSlotOffset + POSTGRES_FLAGS_OFFSET, 0);
            ringBufferHandler.memorySegment().set(ValueLayout.JAVA_INT, batchSlotOffset + POSTGRES_EXTENSION_OFFSET, 0);

            long terminatorOffset = batchSlotOffset + batchSize;

            // PostgreSQL binary format terminator
            final short reservedValue = ringBufferHandler.memorySegment().get(ValueLayout.JAVA_SHORT, terminatorOffset);
            ringBufferHandler.memorySegment().set(ValueLayout.JAVA_SHORT, terminatorOffset, (short) -1);

            final long copiedSize = directSender.sendDirectly(ringBufferHandler, batchSlotOffset - POSTGRES_HEADER_SIZE, this.reusableChunk, batchSize + POSTGRES_HEADER_SIZE);

            ringBufferHandler.memorySegment().set(ValueLayout.JAVA_SHORT, terminatorOffset, reservedValue);

            MemorySegment.copy(
                MemorySegment.ofArray(reservedPrefix),
                0,
                ringBufferHandler.memorySegment(),
                batchSlotOffset - POSTGRES_HEADER_SIZE,
                postgresSignature.length
            );
//            VarHandle.releaseFence();

            if (copiedSize == batchSize) {
                long processingTime = System.nanoTime() - startTime;
                log.debug("Worker {} successfully processed batch: {} bytes in {}μs",
                    workerId,
                    copiedSize,
                    processingTime / 1_000
                );
            } else {
                log.error("Worker {} insert mismatch: expected={}, actual={}",
                    workerId,
                    batchSize,
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
    private boolean validateBatchChecksum(RingBufferHandler<EntryRecord> ringBufferLayout, long batchOffset, long expectedChecksum) {
        try {
            // Пропускаем PostgreSQL header и вычисляем checksum от data
//            long dataOffset = POSTGRES_HEADER_SIZE;
//            long dataSize = ringBufferSegment.byteSize() - POSTGRES_HEADER_SIZE - 2; // minus terminator
            final MemorySegment ringBufferSegment = ringBufferLayout.memorySegment();

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
