package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler;

import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.AccountsPartitionHashTable;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration.PostgreSQLConfiguration;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration.WalConfiguration;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.prevprocessor.RingBufferProcessor;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.stamper.RingBufferStamper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

public class PrevSWMRRingBufferHandler<T> {

    static final Logger log = LoggerFactory.getLogger(PrevSWMRRingBufferHandler.class);

    public static final boolean BARRIER_PASSED = true;

    private static final VarHandle STAMP_STATUS_MEMORY_BARRIER;
    private static final VarHandle WAL_SEQUENCE_ID_MEMORY_BARRIER;
    private static final VarHandle READ_OFFSET_MEMORY_BARRIER;
    private static final VarHandle STAMP_OFFSET_MEMORY_BARRIER;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            // Для статических полей
            STAMP_STATUS_MEMORY_BARRIER = lookup.findVarHandle(
                PrevSWMRRingBufferHandler.class, "stampStatus", int.class);
            WAL_SEQUENCE_ID_MEMORY_BARRIER = lookup.findVarHandle(
                PrevSWMRRingBufferHandler.class, "walSequenceId", long.class);
            READ_OFFSET_MEMORY_BARRIER = lookup.findVarHandle(
                PrevSWMRRingBufferHandler.class, "readOffset", long.class);
            STAMP_OFFSET_MEMORY_BARRIER = lookup.findVarHandle(
                PrevSWMRRingBufferHandler.class, "stampOffset", long.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

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

    private static final int BATCH_CHECKSUM_OFFSET = POSTGRES_HEADER_SIZE;
    private static final ValueLayout BATCH_CHECKSUM_TYPE = ValueLayout.JAVA_LONG;
    private static final int BATCH_CHECKSUM_SIZE = (int) BATCH_CHECKSUM_TYPE.byteSize();

    private static final int BATCH_ACCOUNT_ID_MOST_SIGNIFICANT_OFFSET = BATCH_CHECKSUM_OFFSET + BATCH_CHECKSUM_SIZE;
    private static final ValueLayout BATCH_ACCOUNT_ID_TYPE = ValueLayout.JAVA_LONG;
    private static final int BATCH_ACCOUNT_ID_LEAST_SIGNIFICANT_OFFSET = (int) (BATCH_ACCOUNT_ID_MOST_SIGNIFICANT_OFFSET + BATCH_ACCOUNT_ID_TYPE.byteSize());

    private static final int BATCH_ENTRIES_COUNT_OFFSET = (int) (BATCH_ACCOUNT_ID_LEAST_SIGNIFICANT_OFFSET + BATCH_ACCOUNT_ID_TYPE.byteSize());
    private static final ValueLayout BATCH_ENTRIES_COUNT_TYPE = ValueLayout.JAVA_INT;

    private static final int BATCH_TOTAL_AMOUNT_OFFSET = (int) (BATCH_ENTRIES_COUNT_OFFSET + BATCH_ENTRIES_COUNT_TYPE.byteSize());
    private static final ValueLayout BATCH_TOTAL_AMOUNT_TYPE = ValueLayout.JAVA_LONG;

    private static final int BATCH_TIMESTAMP_OFFSET = (int) (BATCH_TOTAL_AMOUNT_OFFSET + BATCH_TOTAL_AMOUNT_TYPE.byteSize());
    private static final ValueLayout BATCH_TIMESTAMP_TYPE = ValueLayout.JAVA_LONG;

    private static final int BATCH_HEADER_SIZE_RAW = (int) (BATCH_TIMESTAMP_OFFSET + BATCH_TIMESTAMP_TYPE.byteSize());
    private static final int BATCH_HEADER_SIZE = (BATCH_HEADER_SIZE_RAW + CPU_CACHE_LINE_SIZE - 1) & -CPU_CACHE_LINE_SIZE;

    private static final int BATCH_BODY_OFFSET = BATCH_HEADER_SIZE;


    private static final int FIELD_COUNT_OFFSET = 0;
    private static final ValueLayout FIELD_COUNT_TYPE = ValueLayout.JAVA_SHORT;

    private static final ValueLayout LENGTH_TYPE = ValueLayout.JAVA_SHORT;

    private static final int AMOUNT_LENGTH_OFFSET = (int) (FIELD_COUNT_OFFSET + FIELD_COUNT_TYPE.byteSize());

    private static final int AMOUNT_OFFSET = (int) (AMOUNT_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
    private static final ValueLayout AMOUNT_TYPE = ValueLayout.JAVA_LONG;

    private static final int ENTRY_TYPE_LENGTH_OFFSET = (int) (AMOUNT_OFFSET + AMOUNT_TYPE.byteSize());

    private static final int ENTRY_TYPE_OFFSET = (int) (ENTRY_TYPE_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
    private static final ValueLayout ENTRY_TYPE_TYPE = ValueLayout.JAVA_SHORT;

    private static final int ENTRY_RECORD_ID_LENGTH_OFFSET = (int) (ENTRY_TYPE_OFFSET + ENTRY_TYPE_TYPE.byteSize());

    private static final int ENTRY_RECORD_ID_MSB_OFFSET = (int) (ENTRY_RECORD_ID_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
    private static final ValueLayout ENTRY_RECORD_ID_SB_TYPE = ValueLayout.JAVA_LONG;
    private static final int ENTRY_RECORD_ID_LSB_OFFSET = (int) (ENTRY_RECORD_ID_MSB_OFFSET + ENTRY_RECORD_ID_SB_TYPE.byteSize());

    private static final int CREATED_AT_LENGTH_OFFSET = (int) (ENTRY_RECORD_ID_LSB_OFFSET + ENTRY_RECORD_ID_SB_TYPE.byteSize());
    private static final int CREATED_AT_OFFSET = (int) (CREATED_AT_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
    private static final ValueLayout CREATED_AT_TYPE = ValueLayout.JAVA_LONG;

    private static final int OPERATION_DAY_LENGTH_OFFSET = (int) (CREATED_AT_OFFSET + CREATED_AT_TYPE.byteSize());
    private static final int OPERATION_DAY_OFFSET = (int) (CREATED_AT_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
    private static final ValueLayout OPERATION_DAY_TYPE = ValueLayout.JAVA_LONG;

    private static final int ACCOUNT_ID_LENGTH_OFFSET = (int) (OPERATION_DAY_OFFSET + OPERATION_DAY_TYPE.byteSize());
    private static final int ACCOUNT_ID_MOST_SIGNIFICANT_OFFSET = (int) (ACCOUNT_ID_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
    private static final ValueLayout ACCOUNT_ID_TYPE = ValueLayout.JAVA_LONG;
    private static final int ACCOUNT_ID_LEAST_SIGNIFICANT_OFFSET = (int) (ACCOUNT_ID_MOST_SIGNIFICANT_OFFSET + ACCOUNT_ID_TYPE.byteSize());

    private static final int TRANSACTION_ID_LENGTH_OFFSET = (int) (ACCOUNT_ID_LEAST_SIGNIFICANT_OFFSET + ACCOUNT_ID_TYPE.byteSize());
    private static final int TRANSACTION_ID_MOST_SIGNIFICANT_OFFSET = (int) (TRANSACTION_ID_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
    private static final ValueLayout TRANSACTION_ID_TYPE = ValueLayout.JAVA_LONG;
    private static final int TRANSACTION_ID_LEAST_SIGNIFICANT_OFFSET = (int) (TRANSACTION_ID_MOST_SIGNIFICANT_OFFSET + TRANSACTION_ID_TYPE.byteSize());

    private static final int IDEMPOTENCY_KEY_LENGTH_OFFSET = (int) (TRANSACTION_ID_LEAST_SIGNIFICANT_OFFSET + TRANSACTION_ID_TYPE.byteSize());
    private static final int IDEMPOTENCY_KEY_MOST_SIGNIFICANT_OFFSET = (int) (IDEMPOTENCY_KEY_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
    private static final ValueLayout IDEMPOTENCY_KEY_TYPE = ValueLayout.JAVA_LONG;
    private static final int IDEMPOTENCY_KEY_LEAST_SIGNIFICANT_OFFSET = (int) (IDEMPOTENCY_KEY_MOST_SIGNIFICANT_OFFSET + IDEMPOTENCY_KEY_TYPE.byteSize());

    private static final int CURRENCY_CODE_LENGTH_OFFSET = (int) (IDEMPOTENCY_KEY_LEAST_SIGNIFICANT_OFFSET + IDEMPOTENCY_KEY_TYPE.byteSize());
    private static final int CURRENCY_CODE_OFFSET = (int) (IDEMPOTENCY_KEY_LEAST_SIGNIFICANT_OFFSET + LENGTH_TYPE.byteSize());
    private static final ValueLayout CURRENCY_CODE_TYPE = ValueLayout.JAVA_LONG;

    private static final int ORDINAL_LENGTH_OFFSET = (int) (CURRENCY_CODE_OFFSET + CURRENCY_CODE_TYPE.byteSize());
    private static final int ORDINAL_OFFSET = (int) (ORDINAL_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
    private static final ValueLayout ORDINAL_TYPE = ValueLayout.JAVA_LONG;

//    private static final int WAL_SEQUENCE_ID_LENGTH_OFFSET = (int) (ORDINAL_OFFSET + ORDINAL_TYPE.byteSize());
//    public static final int WAL_SEQUENCE_ID_OFFSET = (int) (WAL_SEQUENCE_ID_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
//    private static final ValueLayout WAL_SEQUENCE_ID_TYPE = ValueLayout.JAVA_LONG;

    private static final int POSTGRES_ENTRY_RECORD_RAW_SIZE = (int) (ORDINAL_OFFSET + ORDINAL_TYPE.byteSize());

    public static final int POSTGRES_ENTRY_RECORD_SIZE = (POSTGRES_ENTRY_RECORD_RAW_SIZE + CPU_CACHE_LINE_SIZE - 1) & -CPU_CACHE_LINE_SIZE;


    private final MemorySegment memorySegment;
    //    private final long maxBatches;
//    private final long maxBatchSlotSize;
//    private final long maxEntriesPerBatch;
//    private final long maxBatches;
    private final long arenaHalfSize;
//    private final byte[] postgresSignature = "PGCOPY\n\377\r\n\0".getBytes();

//    private static final VarHandle STAMP_STATUS_MEMORY_BARRIER = ValueLayout.JAVA_INT.varHandle();
//    private static final VarHandle WAL_SEQUENCE_ID_MEMORY_BARRIER = ValueLayout.JAVA_LONG.varHandle();
//    private static final VarHandle READ_OFFSET_MEMORY_BARRIER = ValueLayout.JAVA_LONG.varHandle();
//    private static final VarHandle STAMP_OFFSET_MEMORY_BARRIER = ValueLayout.JAVA_LONG.varHandle();

    private final long[] walSequenceIds;
    private final long[] copySizes;
    //    private final long[] copyOffsets;
    private final int walSequenceIdsMaxCount;
    //    private final long[] batchesSizes;
//    private final long[] batchesOffsets;
//    private int batchIndex = 0;
//    private long batchSizeAccumulated = 0;
    private final AccountsPartitionHashTable accountsPartitionHashTable;
    private int stampStatus;
    private long walSequenceIdsCount;
    private long walSequenceIdsReadyToReadCount;
    private long walSequenceIdsSize;
    private long walSequenceIdsReadyToReadSize;
    private long walSequenceId;
    private long readOffset;
    private long stampOffset;
    private final long walBatchEntriesCount;
    private boolean syncMemoryBarrier;
    private int stampWalSequenceIdIndex;
    private int readWalSequenceIdIndex;
    private long accumulatedToCopySize;
    private long stampBarrierOffset;
    private final RingBufferProcessor<T>[] readers;

    //    private long offset;
    private long arenaSize;
    private final RingBufferStamper<T> stamper;

//    private final AtomicLong writeIndex = new AtomicLong(0);
//    private final AtomicLong readIndex = new AtomicLong(0);

//    private final AtomicLong stampOffset = new AtomicLong(0);
//    private final AtomicLong readOffset = new AtomicLong(0);
//    private final long stampOffset;
//    private final long readOffset;

    public PrevSWMRRingBufferHandler(
        List<RingBufferProcessor<T>> readers,
        WalConfiguration walConfiguration,
        PostgreSQLConfiguration postgreSqlConfiguration,
        MemorySegment memorySegment,
        AccountsPartitionHashTable accountsPartitionHashTable,
        RingBufferStamper<T> stamper
//        long maxEntriesPerBatch
    ) {
        this.stamper = stamper;
        this.readers = new RingBufferProcessor[readers.size()];
        for (int i = 0; i < readers.size(); i++) {
            final RingBufferProcessor<T> reader = readers.get(i);
            this.readers[i] = reader;
        }
        this.memorySegment = memorySegment;
//        this.maxBatches = maxBatches;
        this.arenaSize = memorySegment.byteSize();
        this.arenaHalfSize = arenaSize >> 1;
//        final long maxEntriesPerBatch = postgreSqlConfiguration.ringBufferCopyBatchesCountCapacity();
//        this.maxEntriesPerBatch = maxEntriesPerBatch;//(this.arenaSize - POSTGRES_HEADER_SIZE) / batchElementSize;
//        this.maxBatchSlotSize = BATCH_HEADER_SIZE
//            + (maxEntriesPerBatch * POSTGRES_ENTRY_RECORD_SIZE)
//            + 2;
//        this.offset = 0;
//        this.maxBatches = this.arenaSize / this.maxBatchSlotSize;

        this.stampOffset = 0;
        this.readOffset = 0;

        this.accountsPartitionHashTable = accountsPartitionHashTable;

        this.walBatchEntriesCount = walConfiguration.batchEntriesCount();

        final int maxSlotsCount = (int) (((long) postgreSqlConfiguration.ringBufferCopyBatchesCountCapacity()
            * postgreSqlConfiguration.directCopyBatchRecordsCount())
            / walConfiguration.batchEntriesCount());

        walSequenceIdsMaxCount = (int) (((long) postgreSqlConfiguration.ringBufferCopyBatchesCountCapacity()
            * postgreSqlConfiguration.directCopyBatchRecordsCount())
            / BATCH_HEADER_SIZE + POSTGRES_ENTRY_RECORD_SIZE);

        walSequenceIds = new long[walSequenceIdsMaxCount];
        copySizes = new long[walSequenceIdsMaxCount];
//        copyOffsets = new long[walSequenceIdsMaxCount];
//        batchesSizes = new long[maxSlotsCount];
//        batchesOffsets = new long[maxSlotsCount];

//        this.memorySegment.asSlice(0, METADATA_SIZE).fill((byte) 0);
//        this.memorySegment.set(ValueLayout.JAVA_LONG, WRITE_INDEX_OFFSET, 0L);
//        this.memorySegment.set(ValueLayout.JAVA_LONG, READ_INDEX_OFFSET, 0L);
//        this.memorySegment.set(ValueLayout.JAVA_LONG, TOTAL_BATCHES_OFFSET, 0L);
    }

    public MemorySegment memorySegment() {
        return memorySegment;
    }

    public long tryProcessBatch(RingBufferProcessor<T> ringBufferProcessor, long currentReadOffset, long maxBatchSlotSize) {
        final int status = (int) STAMP_STATUS_MEMORY_BARRIER.getAcquire(this);
        if (status != MemoryBarrierOperationStatus.COMPLETED.ordinal()) {
            throw new IllegalStateException("Batch is in progress now and do not ready: " + MemoryBarrierOperationStatus.fromMemoryOrdinal(status).name());
        }

        VarHandle.acquireFence();
        long currentStampOffset = stampOffset;// (long) STAMP_OFFSET_MEMORY_BARRIER.get(this);

        final long batchSize = Math.min(
            Math.min(
                maxBatchSlotSize,
                currentReadOffset >= this.arenaHalfSize
                    ? this.arenaSize - currentReadOffset
                    : this.arenaHalfSize - currentReadOffset
            ),
            currentStampOffset >= currentReadOffset
                ? currentStampOffset - currentReadOffset
                : maxBatchSlotSize
        );
        // Проверяем есть ли данные
        if (currentReadOffset >= currentStampOffset && !(currentReadOffset >= this.arenaHalfSize && currentStampOffset < this.arenaHalfSize)) {
            throw new IllegalStateException("No data to process");
        }

        try {
            // ✅ Обрабатываем batch
            final long processedSize = ringBufferProcessor.process(this, currentReadOffset, batchSize);

            if (processedSize == batchSize) {
                // ✅ Очищаем slot для переиспользования
                // clearBatchSlot(batchSlotOffset);
                log.debug("Successfully processed and cleared batch at slot {}", currentReadOffset);
//                stampBarrierOffset += processedSize;
//                VarHandle.releaseFence();
                return currentReadOffset + processedSize;
//                READ_OFFSET_MEMORY_BARRIER.setRelease(this, nextReadOffset);
            }
            // Откатываем read index если обработка не удалась
//                READ_OFFSET_MEMORY_BARRIER.setRelease(this, currentReadOffset);
//                readOffset.(currentReadOffset + 1, currentReadOffset);
//                VarHandle.releaseFence();
            log.warn("Failed to process batch at slot {}, rolling back", currentReadOffset);
            throw new IllegalStateException("Failed to process batch");

//            return lastSlotSequenceId;

        } catch (Exception exception) {
            // Откатываем read index при ошибке
//            readOffset.compareAndSet(currentReadOffset + 1, currentReadOffset);
//            READ_OFFSET_MEMORY_BARRIER.setRelease(this, currentReadOffset);
//            VarHandle.releaseFence();
            log.error("Error consuming batch at slot {}", currentReadOffset, exception);
            throw new RuntimeException("Failed to consume batch", exception);
        }
    }

    public boolean stampForward(T stampRecord) {
        int maxAttempts = 100;
        int spinAttempts = 20;
        int yieldAttempts = 50;
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            VarHandle.acquireFence();
            long minimalReadOffset = this.arenaSize;
            for (int i = 0; i < readers.length; i++) {
                final long anotherReadOffset = readers[i].processOffset() - readers[i].beforeBatchOperationGap();
                if (minimalReadOffset > anotherReadOffset) {
                    minimalReadOffset = anotherReadOffset;
                }
            }

            long currentStampOffset = stampOffset;
            long nextStampOffset = (currentStampOffset + stamper.stampRecordSize()) % this.arenaSize;

            if (currentStampOffset > minimalReadOffset - stamper.stampRecordSize()) {
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
                stamper.stamp(memorySegment, stampOffset, stampRecord);

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

    public long passedBarrierOffset() {
        final long arenaHalfSize = arenaSize >> 1;
        final long offset = (long) STAMP_OFFSET_MEMORY_BARRIER.getAcquire(this);
        if (offset >= arenaHalfSize) {
            return arenaHalfSize;
        }
        return 0;
    }

//    public int barrierPassedEntriesCount(long dataSize) {
//        final long arenaHalfSize = arenaSize >> 1;
//        if (stampOffset >= arenaHalfSize) {
//            return (int) (dataSize - arenaHalfSize) / POSTGRES_ENTRY_RECORD_SIZE;
//        }
//        return (int) dataSize / POSTGRES_ENTRY_RECORD_SIZE;
//    }

    public long batchChecksum(long batchOffset, long batchSize) {
        long checksum = 0;
        long dataOffset = batchOffset + BATCH_BODY_OFFSET;

        // XOR checksum по 8-байтовым блокам
        for (long i = 0; i <= batchSize - 8; i += 8) {
            long value = memorySegment.get(ValueLayout.JAVA_LONG, dataOffset + i);
            checksum ^= value;
        }

        // Обрабатываем остаток
        for (long i = batchSize - (batchSize % 8); i < batchSize; i++) {
            checksum ^= memorySegment.get(ValueLayout.JAVA_BYTE, dataOffset + i);
        }

        return checksum;
    }

    private long entriesCountBatchSize(int entriesCount) {
        return BATCH_HEADER_SIZE
            + ((long) entriesCount * POSTGRES_ENTRY_RECORD_SIZE)
            + 2;
    }

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
