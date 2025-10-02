package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.ringbuffer;

import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration.PostgreSQLConfiguration;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration.WalConfiguration;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.stamper.RingBufferStamper;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.prevprocessor.RingBufferProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.VarHandle;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

public class SWMRRingBufferHandler {

    static final Logger log = LoggerFactory.getLogger(SWMRRingBufferHandler.class);

//    private static final VarHandle STAMP_STATUS_MEMORY_BARRIER;
//    private static final VarHandle STAMP_OFFSET_VAR_HANDLE;

//    static {
//        try {
//            MethodHandles.Lookup lookup = MethodHandles.lookup();
//            // Для статических полей
////            STAMP_STATUS_MEMORY_BARRIER = lookup.findVarHandle(
////                SWMRRingBufferHandler.class, "stampStatus", int.class);
//            STAMP_OFFSET_VAR_HANDLE = lookup.findVarHandle(
//                SWMRRingBufferHandler.class, "stampOffset", long.class);
//        } catch (Exception e) {
//            throw new ExceptionInInitializerError(e);
//        }
//    }

    private final static int CPU_CACHE_LINE_SIZE = 64;

    // ===== RING BUFFER METADATA =====

    private static final int WRITE_INDEX_OFFSET = 0;                    // 8 bytes
    private static final int READ_INDEX_OFFSET = 8;                     // 8 bytes
    private static final int TOTAL_BATCHES_OFFSET = 16;                 // 8 bytes
    private static final int METADATA_SIZE = 64;                        // Cache-aligned

    // ===== PostgreSQL batch header =====

    private static final int POSTGRES_SIGNATURE_OFFSET = 0;             // 11 bytes "PGCOPY\n\377\r\n\0"
    private static final int POSTGRES_FLAGS_OFFSET = 11;                // 4 bytes
    private static final int POSTGRES_EXTENSION_OFFSET = 15;            // 4 bytes
    private static final int POSTGRES_HEADER_SIZE = 19;

    // ===== Batch metadata (наши поля) =====

//    private static final int BATCH_CHECKSUM_OFFSET = POSTGRES_HEADER_SIZE;
//    private static final ValueLayout BATCH_CHECKSUM_TYPE = ValueLayout.JAVA_LONG;
//    private static final int BATCH_CHECKSUM_SIZE = (int) BATCH_CHECKSUM_TYPE.byteSize();
//
//    private static final int BATCH_ACCOUNT_ID_MOST_SIGNIFICANT_OFFSET = BATCH_CHECKSUM_OFFSET + BATCH_CHECKSUM_SIZE;
//    private static final ValueLayout BATCH_ACCOUNT_ID_TYPE = ValueLayout.JAVA_LONG;
//    private static final int BATCH_ACCOUNT_ID_LEAST_SIGNIFICANT_OFFSET = (int) (BATCH_ACCOUNT_ID_MOST_SIGNIFICANT_OFFSET + BATCH_ACCOUNT_ID_TYPE.byteSize());
//
//    private static final int BATCH_ENTRIES_COUNT_OFFSET = (int) (BATCH_ACCOUNT_ID_LEAST_SIGNIFICANT_OFFSET + BATCH_ACCOUNT_ID_TYPE.byteSize());
//    private static final ValueLayout BATCH_ENTRIES_COUNT_TYPE = ValueLayout.JAVA_INT;
//
//    private static final int BATCH_TOTAL_AMOUNT_OFFSET = (int) (BATCH_ENTRIES_COUNT_OFFSET + BATCH_ENTRIES_COUNT_TYPE.byteSize());
//    private static final ValueLayout BATCH_TOTAL_AMOUNT_TYPE = ValueLayout.JAVA_LONG;
//
//    private static final int BATCH_TIMESTAMP_OFFSET = (int) (BATCH_TOTAL_AMOUNT_OFFSET + BATCH_TOTAL_AMOUNT_TYPE.byteSize());
//    private static final ValueLayout BATCH_TIMESTAMP_TYPE = ValueLayout.JAVA_LONG;
//
//    private static final int BATCH_HEADER_SIZE_RAW = (int) (BATCH_TIMESTAMP_OFFSET + BATCH_TIMESTAMP_TYPE.byteSize());
//    private static final int BATCH_HEADER_SIZE = (BATCH_HEADER_SIZE_RAW + CPU_CACHE_LINE_SIZE - 1) & -CPU_CACHE_LINE_SIZE;
//
//    private static final int BATCH_BODY_OFFSET = BATCH_HEADER_SIZE;
//
//
//    private static final int FIELD_COUNT_OFFSET = 0;
//    private static final ValueLayout FIELD_COUNT_TYPE = ValueLayout.JAVA_SHORT;
//
//    private static final ValueLayout LENGTH_TYPE = ValueLayout.JAVA_SHORT;
//
//    private static final int AMOUNT_LENGTH_OFFSET = (int) (FIELD_COUNT_OFFSET + FIELD_COUNT_TYPE.byteSize());
//
//    private static final int AMOUNT_OFFSET = (int) (AMOUNT_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
//    private static final ValueLayout AMOUNT_TYPE = ValueLayout.JAVA_LONG;
//
//    private static final int ENTRY_TYPE_LENGTH_OFFSET = (int) (AMOUNT_OFFSET + AMOUNT_TYPE.byteSize());
//
//    private static final int ENTRY_TYPE_OFFSET = (int) (ENTRY_TYPE_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
//    private static final ValueLayout ENTRY_TYPE_TYPE = ValueLayout.JAVA_SHORT;
//
//    private static final int ENTRY_RECORD_ID_LENGTH_OFFSET = (int) (ENTRY_TYPE_OFFSET + ENTRY_TYPE_TYPE.byteSize());
//
//    private static final int ENTRY_RECORD_ID_MSB_OFFSET = (int) (ENTRY_RECORD_ID_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
//    private static final ValueLayout ENTRY_RECORD_ID_SB_TYPE = ValueLayout.JAVA_LONG;
//    private static final int ENTRY_RECORD_ID_LSB_OFFSET = (int) (ENTRY_RECORD_ID_MSB_OFFSET + ENTRY_RECORD_ID_SB_TYPE.byteSize());
//
//    private static final int CREATED_AT_LENGTH_OFFSET = (int) (ENTRY_RECORD_ID_LSB_OFFSET + ENTRY_RECORD_ID_SB_TYPE.byteSize());
//    private static final int CREATED_AT_OFFSET = (int) (CREATED_AT_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
//    private static final ValueLayout CREATED_AT_TYPE = ValueLayout.JAVA_LONG;
//
//    private static final int OPERATION_DAY_LENGTH_OFFSET = (int) (CREATED_AT_OFFSET + CREATED_AT_TYPE.byteSize());
//    private static final int OPERATION_DAY_OFFSET = (int) (CREATED_AT_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
//    private static final ValueLayout OPERATION_DAY_TYPE = ValueLayout.JAVA_LONG;
//
//    private static final int ACCOUNT_ID_LENGTH_OFFSET = (int) (OPERATION_DAY_OFFSET + OPERATION_DAY_TYPE.byteSize());
//    private static final int ACCOUNT_ID_MOST_SIGNIFICANT_OFFSET = (int) (ACCOUNT_ID_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
//    private static final ValueLayout ACCOUNT_ID_TYPE = ValueLayout.JAVA_LONG;
//    private static final int ACCOUNT_ID_LEAST_SIGNIFICANT_OFFSET = (int) (ACCOUNT_ID_MOST_SIGNIFICANT_OFFSET + ACCOUNT_ID_TYPE.byteSize());
//
//    private static final int TRANSACTION_ID_LENGTH_OFFSET = (int) (ACCOUNT_ID_LEAST_SIGNIFICANT_OFFSET + ACCOUNT_ID_TYPE.byteSize());
//    private static final int TRANSACTION_ID_MOST_SIGNIFICANT_OFFSET = (int) (TRANSACTION_ID_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
//    private static final ValueLayout TRANSACTION_ID_TYPE = ValueLayout.JAVA_LONG;
//    private static final int TRANSACTION_ID_LEAST_SIGNIFICANT_OFFSET = (int) (TRANSACTION_ID_MOST_SIGNIFICANT_OFFSET + TRANSACTION_ID_TYPE.byteSize());
//
//    private static final int IDEMPOTENCY_KEY_LENGTH_OFFSET = (int) (TRANSACTION_ID_LEAST_SIGNIFICANT_OFFSET + TRANSACTION_ID_TYPE.byteSize());
//    private static final int IDEMPOTENCY_KEY_MOST_SIGNIFICANT_OFFSET = (int) (IDEMPOTENCY_KEY_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
//    private static final ValueLayout IDEMPOTENCY_KEY_TYPE = ValueLayout.JAVA_LONG;
//    private static final int IDEMPOTENCY_KEY_LEAST_SIGNIFICANT_OFFSET = (int) (IDEMPOTENCY_KEY_MOST_SIGNIFICANT_OFFSET + IDEMPOTENCY_KEY_TYPE.byteSize());
//
//    private static final int CURRENCY_CODE_LENGTH_OFFSET = (int) (IDEMPOTENCY_KEY_LEAST_SIGNIFICANT_OFFSET + IDEMPOTENCY_KEY_TYPE.byteSize());
//    private static final int CURRENCY_CODE_OFFSET = (int) (IDEMPOTENCY_KEY_LEAST_SIGNIFICANT_OFFSET + LENGTH_TYPE.byteSize());
//    private static final ValueLayout CURRENCY_CODE_TYPE = ValueLayout.JAVA_LONG;
//
//    private static final int ORDINAL_LENGTH_OFFSET = (int) (CURRENCY_CODE_OFFSET + CURRENCY_CODE_TYPE.byteSize());
//    private static final int ORDINAL_OFFSET = (int) (ORDINAL_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
//    private static final ValueLayout ORDINAL_TYPE = ValueLayout.JAVA_LONG;
//
//    private static final int POSTGRES_ENTRY_RECORD_RAW_SIZE = (int) (ORDINAL_OFFSET + ORDINAL_TYPE.byteSize());
//
//    public static final int POSTGRES_ENTRY_RECORD_SIZE = (POSTGRES_ENTRY_RECORD_RAW_SIZE + CPU_CACHE_LINE_SIZE - 1) & -CPU_CACHE_LINE_SIZE;


    private final MemorySegment memorySegment;
    private final long arenaHalfSize;

    private long stampOffset;
    private final RingBufferProcessor[] processors;

    //    private long offset;
    private long arenaSize;

    public SWMRRingBufferHandler(
        List<RingBufferProcessor> processors,
        WalConfiguration walConfiguration,
        PostgreSQLConfiguration postgreSqlConfiguration,
        MemorySegment memorySegment
    ) {
        this.processors = new RingBufferProcessor[processors.size()];
        for (int i = 0; i < processors.size(); i++) {
            final RingBufferProcessor reader = processors.get(i);
            this.processors[i] = reader;
        }
        this.memorySegment = memorySegment;
        this.arenaSize = memorySegment.byteSize();
        this.arenaHalfSize = arenaSize >> 1;

        this.stampOffset = 0;

//        final int maxSlotsCount = (int) (((long) postgreSqlConfiguration.ringBufferCopyBatchesCountCapacity()
//            * postgreSqlConfiguration.directCopyBatchRecordsCount())
//            / walConfiguration.batchEntriesCount());

//        walSequenceIdsMaxCount = (int) (((long) postgreSqlConfiguration.ringBufferCopyBatchesCountCapacity()
//            * postgreSqlConfiguration.directCopyBatchRecordsCount())
//            / BATCH_HEADER_SIZE + POSTGRES_ENTRY_RECORD_SIZE);
    }

    public MemorySegment memorySegment() {
        return memorySegment;
    }

    public long readyToProcess(long currentProcessOffset, long expectedProcessSize) {
//        final int status = (int) STAMP_STATUS_MEMORY_BARRIER.getAcquire(this);
//        if (status != MemoryBarrierOperationStatus.COMPLETED.ordinal()) {
//            throw new IllegalStateException("Batch is in progress now and do not ready: " + MemoryBarrierOperationStatus.fromMemoryOrdinal(status).name());
//        }

        VarHandle.acquireFence();
        long currentStampOffset = stampOffset;

        final long batchSize = Math.min(
            Math.min(
                expectedProcessSize,
                currentProcessOffset >= this.arenaHalfSize
                    ? this.arenaSize - currentProcessOffset
                    : this.arenaHalfSize - currentProcessOffset
            ),
            currentStampOffset >= currentProcessOffset
                ? currentStampOffset - currentProcessOffset
                : expectedProcessSize
        );
        // Проверяем есть ли данные
        if (currentProcessOffset >= currentStampOffset && !(currentProcessOffset >= this.arenaHalfSize && currentStampOffset < this.arenaHalfSize)) {
            return 0;
//            throw new IllegalStateException("No data to process");
        }

        return batchSize;

//        try {
//            // ✅ Обрабатываем batch
//            final long processedSize = ringBufferProcessor.process(this, currentReadOffset, batchSize);
//
//            if (processedSize == batchSize) {
//                log.debug("Successfully processed and cleared batch at slot {}", currentReadOffset);
//                return currentReadOffset + processedSize;
//            }
//            log.warn("Failed to process batch at slot {}, rolling back", currentReadOffset);
//            throw new IllegalStateException("Failed to process batch");
//        } catch (Exception exception) {
//            log.error("Error consuming batch at slot {}", currentReadOffset, exception);
//            throw new RuntimeException("Failed to consume batch", exception);
//        }
    }

    public boolean tryStampForward(RingBufferStamper stamper, int maxAttempts) {
//        int maxAttempts = 100;
        int spinAttempts = 20;
        int yieldAttempts = 50;
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            VarHandle.acquireFence();
            long currentStampOffset = stampOffset;
            long nextStampOffset = (currentStampOffset + stamper.stampRecordSize()) % this.arenaSize;

            long minimalProcessOffset = this.arenaSize;
            for (int i = 0; i < processors.length; i++) {
                final long processOffset = processors[i].processOffset() - processors[i].beforeBatchOperationGap();
                if (minimalProcessOffset > processOffset) {
                    minimalProcessOffset = processOffset;
                }
            }

            if (currentStampOffset > minimalProcessOffset - stamper.stampRecordSize()) {
                if (attempt < spinAttempts) {
                    Thread.onSpinWait();
                } else if (attempt < spinAttempts + yieldAttempts) {
                    Thread.yield();
                } else {
                    final int parkAttempt = attempt - spinAttempts - yieldAttempts;
                    final long parkNanos = 1_000L << Math.min(parkAttempt, 16);
                    LockSupport.parkNanos(parkNanos);
                }
                continue;
            }

            try {
                stamper.stamp(memorySegment, stampOffset);

                stampOffset = nextStampOffset;

                VarHandle.releaseFence();

                if (currentStampOffset < this.arenaHalfSize && nextStampOffset >= this.arenaHalfSize) {
                    return true;
                }
                if (currentStampOffset > this.arenaHalfSize && nextStampOffset < this.arenaHalfSize) {
                    return true;
                }
                return false;
            } catch (Exception exception) {
                VarHandle.releaseFence();
                throw new RuntimeException("Failed to publish batch", exception);
            }
        }
        throw new IllegalStateException("Failed to publish batch after " + maxAttempts + " attempts");
    }

//    public long passedBarrierOffset() {
//        final long arenaHalfSize = arenaSize >> 1;
//        final long offset = (long) STAMP_OFFSET_VAR_HANDLE.getAcquire(this);
//        if (offset >= arenaHalfSize) {
//            return arenaHalfSize;
//        }
//        return 0;
//    }

//    public int barrierPassedEntriesCount(long dataSize) {
//        final long arenaHalfSize = arenaSize >> 1;
//        if (stampOffset >= arenaHalfSize) {
//            return (int) (dataSize - arenaHalfSize) / POSTGRES_ENTRY_RECORD_SIZE;
//        }
//        return (int) dataSize / POSTGRES_ENTRY_RECORD_SIZE;
//    }

//    public long batchChecksum(long batchOffset, long batchSize) {
//        long checksum = 0;
//        long dataOffset = batchOffset + BATCH_BODY_OFFSET;
//
//        // XOR checksum по 8-байтовым блокам
//        for (long i = 0; i <= batchSize - 8; i += 8) {
//            long value = memorySegment.get(ValueLayout.JAVA_LONG, dataOffset + i);
//            checksum ^= value;
//        }
//
//        // Обрабатываем остаток
//        for (long i = batchSize - (batchSize % 8); i < batchSize; i++) {
//            checksum ^= memorySegment.get(ValueLayout.JAVA_BYTE, dataOffset + i);
//        }
//
//        return checksum;
//    }

//    private long entriesCountBatchSize(int entriesCount) {
//        return BATCH_HEADER_SIZE
//            + ((long) entriesCount * POSTGRES_ENTRY_RECORD_SIZE)
//            + 2;
//    }

    public void offsetForward(long batchRawSize) {
        if (stampOffset + batchRawSize >= arenaSize) {
            stampOffset = 0;
        } else {
            stampOffset = stampOffset + batchRawSize;
        }
    }

    public boolean offsetBarrierPassed(long batchRawSize) {
        return stampOffset == 0 || (stampOffset + batchRawSize) == (arenaSize >> 1);
    }

    public void resetStampOffset() {
        stampOffset = 0;
    }
}
