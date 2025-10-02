package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler;

import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration.PostgreSQLConfiguration;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration.WalConfiguration;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.prevprocessor.RingBufferProcessor;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

@Slf4j
@Data
@Accessors(fluent = true)
public class PostgreSqlEntryRecordRingBufferHandler implements RingBufferHandler {

    private static final VarHandle STAMP_STATUS_MEMORY_BARRIER;
    private static final VarHandle WAL_SEQUENCE_ID_MEMORY_BARRIER;
    private static final VarHandle READ_OFFSET_MEMORY_BARRIER;
    private static final VarHandle STAMP_OFFSET_MEMORY_BARRIER;

    private static final VarHandle WAL_SEQUENCE_IDS_COUNT_MEMORY_BARRIER;
    private static final VarHandle WAL_SEQUENCE_IDS_SIZE_MEMORY_BARRIER;
    private static final VarHandle WAL_SEQUENCE_IDS_READY_TO_READ_COUNT_MEMORY_BARRIER;
    private static final VarHandle WAL_SEQUENCE_IDS_READY_TO_READ_SIZE_MEMORY_BARRIER;
    private static final VarHandle SYNC_MEMORY_BARRIER;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            // Для статических полей
            SYNC_MEMORY_BARRIER = lookup.findVarHandle(
                PostgreSqlEntryRecordRingBufferHandler.class, "syncMemoryBarrier", boolean.class);
            STAMP_STATUS_MEMORY_BARRIER = lookup.findVarHandle(
                PostgreSqlEntryRecordRingBufferHandler.class, "stampStatus", int.class);
            WAL_SEQUENCE_ID_MEMORY_BARRIER = lookup.findVarHandle(
                PostgreSqlEntryRecordRingBufferHandler.class, "walSequenceId", long.class);
            READ_OFFSET_MEMORY_BARRIER = lookup.findVarHandle(
                PostgreSqlEntryRecordRingBufferHandler.class, "readOffset", long.class);
            STAMP_OFFSET_MEMORY_BARRIER = lookup.findVarHandle(
                PostgreSqlEntryRecordRingBufferHandler.class, "stampOffset", long.class);
            WAL_SEQUENCE_IDS_COUNT_MEMORY_BARRIER = lookup.findVarHandle(
                PostgreSqlEntryRecordRingBufferHandler.class, "walSequenceIdsCount", long.class);
            WAL_SEQUENCE_IDS_SIZE_MEMORY_BARRIER = lookup.findVarHandle(
                PostgreSqlEntryRecordRingBufferHandler.class, "walSequenceIdsSize", long.class);
            WAL_SEQUENCE_IDS_READY_TO_READ_COUNT_MEMORY_BARRIER = lookup.findVarHandle(
                PostgreSqlEntryRecordRingBufferHandler.class, "walSequenceIdsReadyToReadCount", long.class);
            WAL_SEQUENCE_IDS_READY_TO_READ_SIZE_MEMORY_BARRIER = lookup.findVarHandle(
                PostgreSqlEntryRecordRingBufferHandler.class, "walSequenceIdsReadyToReadSize", long.class);
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

    private final MemorySegment ringBufferSegment;
    //    private final long maxBatches;
    private final long maxBatchSlotSize;
    private final long maxEntriesPerBatch;
    private final long maxBatches;
    private final long arenaHalfSize;
    private final byte[] postgresSignature = "PGCOPY\n\377\r\n\0".getBytes();

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

    //    private long offset;
    private long arenaSize;

//    private final AtomicLong writeIndex = new AtomicLong(0);
//    private final AtomicLong readIndex = new AtomicLong(0);

//    private final AtomicLong stampOffset = new AtomicLong(0);
//    private final AtomicLong readOffset = new AtomicLong(0);
//    private final long stampOffset;
//    private final long readOffset;

    public PostgreSqlEntryRecordRingBufferHandler(
        WalConfiguration walConfiguration,
        PostgreSQLConfiguration postgreSqlConfiguration,
        MemorySegment memorySegment
//        long maxEntriesPerBatch
    ) {
        this.ringBufferSegment = memorySegment;
//        this.maxBatches = maxBatches;
        this.arenaSize = memorySegment.byteSize();
        this.arenaHalfSize = arenaSize >> 1;
        final long maxEntriesPerBatch = postgreSqlConfiguration.ringBufferCopyBatchesCountCapacity();
        this.maxEntriesPerBatch = maxEntriesPerBatch;//(this.arenaSize - POSTGRES_HEADER_SIZE) / batchElementSize;
        this.maxBatchSlotSize = BATCH_HEADER_SIZE
            + (maxEntriesPerBatch * EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE)
            + 2;
//        this.offset = 0;
        this.maxBatches = this.arenaSize / this.maxBatchSlotSize;

        this.stampOffset = 0;
        this.readOffset = 0;

        this.walBatchEntriesCount = walConfiguration.batchEntriesCount();

        final int maxSlotsCount = (int) (((long) postgreSqlConfiguration.ringBufferCopyBatchesCountCapacity()
                * postgreSqlConfiguration.directCopyBatchRecordsCount())
                / walConfiguration.batchEntriesCount());

        walSequenceIdsMaxCount = (int) (((long) postgreSqlConfiguration.ringBufferCopyBatchesCountCapacity()
            * postgreSqlConfiguration.directCopyBatchRecordsCount())
            / BATCH_HEADER_SIZE + EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE);

        walSequenceIds = new long[walSequenceIdsMaxCount];
        copySizes = new long[walSequenceIdsMaxCount];
//        copyOffsets = new long[walSequenceIdsMaxCount];
//        batchesSizes = new long[maxSlotsCount];
//        batchesOffsets = new long[maxSlotsCount];

        ringBufferSegment.asSlice(0, METADATA_SIZE).fill((byte) 0);
        ringBufferSegment.set(ValueLayout.JAVA_LONG, WRITE_INDEX_OFFSET, 0L);
        ringBufferSegment.set(ValueLayout.JAVA_LONG, READ_INDEX_OFFSET, 0L);
        ringBufferSegment.set(ValueLayout.JAVA_LONG, TOTAL_BATCHES_OFFSET, 0L);
    }

    @Override
    public MemorySegment memorySegment() {
        return ringBufferSegment;
    }

    @Override
    public long batchChecksum(long batchOffset) {
        return ringBufferSegment.get(ValueLayout.JAVA_LONG, batchOffset + BATCH_CHECKSUM_OFFSET);
    }

    public long walSequenceId(long batchOffset) {
        final int status = (int) STAMP_STATUS_MEMORY_BARRIER.getAcquire(ringBufferSegment, batchOffset);
        if (status != MemoryBarrierOperationStatus.COMPLETED.ordinal()) {
            throw new IllegalStateException("Batch not ready: " + status);
        }
        return ringBufferSegment.get(ValueLayout.JAVA_LONG, batchOffset + BATCH_BODY_OFFSET + WalEntryRecordRingBufferHandler.BATCH_WAL_SEQUENCE_ID_OFFSET);
//        return (long) STAMP_MEMORY_BARRIER_WAL_SEQUENCE_ID.getAcquire(
//            ringBufferSegment,
//            batchOffset + BATCH_BODY_OFFSET + EntryRecordBatchHandler.WAL_SEQUENCE_ID_OFFSET
//        );
    }

    public long batchChecksum() {
        return ringBufferSegment.get(ValueLayout.JAVA_LONG, stampOffset + BATCH_CHECKSUM_OFFSET);
    }

    public void setBatchChecksum(long batchOffset, long checkSum) {
        ringBufferSegment.set(ValueLayout.JAVA_LONG, batchOffset + BATCH_CHECKSUM_OFFSET, checkSum);
    }

    public void setBatchChecksum(long checkSum) {
        ringBufferSegment.set(ValueLayout.JAVA_LONG, stampOffset + BATCH_CHECKSUM_OFFSET, checkSum);
    }

    @Override
    public long batchSize(long batchOffset) {
        return entriesCountBatchSize(
            ringBufferSegment.get(ValueLayout.JAVA_INT, batchOffset + BATCH_ENTRIES_COUNT_OFFSET)
        );
    }

    public long batchSize() {
        return entriesCountBatchSize(
            ringBufferSegment.get(ValueLayout.JAVA_INT, stampOffset + BATCH_ENTRIES_COUNT_OFFSET)
        );
    }

    @Override
    public long batchElementsCount(long batchOffset) {
        return ringBufferSegment.get(ValueLayout.JAVA_INT, batchOffset + BATCH_ENTRIES_COUNT_OFFSET);
    }

    public long batchElementsCount() {
        return ringBufferSegment.get(ValueLayout.JAVA_INT, stampOffset + BATCH_ENTRIES_COUNT_OFFSET);
    }

    public long batchTotalAmount(long batchOffset) {
        return ringBufferSegment.get(ValueLayout.JAVA_INT, batchOffset + BATCH_TOTAL_AMOUNT_OFFSET);
    }

    public long batchTotalAmount() {
        return ringBufferSegment.get(ValueLayout.JAVA_INT, stampOffset + BATCH_TOTAL_AMOUNT_OFFSET);
    }

//    private long slotOffset(long batchIndex) {

    /// /        return METADATA_SIZE + ((batchIndex % maxBatches) * batchSlotSize);
//        final long slotOffset = batchIndex * batchSlotSize;
//        return METADATA_SIZE + (batchIndex % maxBatches) * batchSlotSize);
//    }

    /**
     * This method is called by the consumer thread.
     *
     */
    public long tryProcessBatch(RingBufferProcessor ringBufferProcessor) {
        final int status = (int) STAMP_STATUS_MEMORY_BARRIER.getAcquire(this);
        if (status != MemoryBarrierOperationStatus.COMPLETED.ordinal()) {
            throw new IllegalStateException("Batch is in progress now and do not ready: " + MemoryBarrierOperationStatus.fromMemoryOrdinal(status).name());
        }

        long currentStampOffset = (long) STAMP_OFFSET_MEMORY_BARRIER.get(this);
        long currentReadOffset = (long) READ_OFFSET_MEMORY_BARRIER.get(this);

        VarHandle.acquireFence();

        // Проверяем есть ли данные
        if (currentReadOffset >= currentStampOffset) {
            throw new IllegalStateException("No data to process");
        }

        // Пытаемся занять слот для чтения
        final long slotCount = ringBufferSegment.get(ValueLayout.JAVA_INT, currentReadOffset + BATCH_ENTRIES_COUNT_OFFSET);
        final long slotSize = BATCH_HEADER_SIZE
            + (slotCount * EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE)
            + 2;

        final long batchSize = Math.min(
            this.maxBatchSlotSize,
            currentReadOffset > this.arenaHalfSize
                ? this.arenaSize - this.arenaHalfSize
                : this.arenaHalfSize
        );
        final long slotOffsetBarrier = currentReadOffset > this.arenaHalfSize
            ? this.arenaSize
            : this.arenaHalfSize;
//        currentReadOffset = (long) READ_OFFSET_MEMORY_BARRIER.getAndAdd(this, batchSize);

//        long lastSlotSequenceId = ringBufferSegment.get(ValueLayout.JAVA_LONG, currentReadOffset + WalEntryRecordBatchRingBufferHandler.BATCH_WAL_SEQUENCE_ID_OFFSET);
        long nextOffset = currentReadOffset +  this.copySizes[this.readWalSequenceIdIndex]; //currentReadOffset + slotSize;
        final long alignedSize = slotSize % this.walBatchEntriesCount != 0
            ? ((nextOffset / this.walBatchEntriesCount) + 1) * this.walBatchEntriesCount
            : slotSize;
        final long nextReadOffset = currentReadOffset + alignedSize;
        final int nextReadIndex = (int) (nextReadOffset / this.walBatchEntriesCount);
        final long lastSlotSequenceId = this.walSequenceIds[this.readWalSequenceIdIndex];
//        lastSlotSequenceId = this.walSequenceIds[nextReadIndex];
//        while (slotOffset < batchSize + currentReadOffset && slotOffset < slotOffsetBarrier) {
//            final long nextSlotCount = ringBufferSegment.get(ValueLayout.JAVA_INT, slotOffset + BATCH_ENTRIES_COUNT_OFFSET);
//            final long nextSlotSize = BATCH_HEADER_SIZE
//                + (nextSlotCount * EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE)
//                + 2;
//            lastSlotSequenceId = ringBufferSegment.get(ValueLayout.JAVA_LONG, currentReadOffset + WalEntryRecordBatchRingBufferHandler.BATCH_WAL_SEQUENCE_ID_OFFSET);
//            slotOffset += nextSlotSize;
//        }
//        long nextReadOffset = slotOffset;

        try {
            // ✅ Обрабатываем batch
            final long processed = ringBufferProcessor.process(this, currentReadOffset, batchSize);

            if (processed == batchSize) {
                // ✅ Очищаем slot для переиспользования
                // clearBatchSlot(batchSlotOffset);
                log.debug("Successfully processed and cleared batch at slot {}", currentReadOffset);
                READ_OFFSET_MEMORY_BARRIER.setRelease(this, nextReadOffset);
            } else {
                // Откатываем read index если обработка не удалась
//                READ_OFFSET_MEMORY_BARRIER.setRelease(this, currentReadOffset);
//                readOffset.(currentReadOffset + 1, currentReadOffset);
                VarHandle.releaseFence();
                log.warn("Failed to process batch at slot {}, rolling back", currentReadOffset);
                throw new IllegalStateException("Failed to process batch");
            }

            return lastSlotSequenceId;

        } catch (Exception e) {
            // Откатываем read index при ошибке
//            readOffset.compareAndSet(currentReadOffset + 1, currentReadOffset);
//            READ_OFFSET_MEMORY_BARRIER.setRelease(this, currentReadOffset);
            VarHandle.releaseFence();
            log.error("Error consuming batch at slot {}", currentReadOffset, e);
            throw new RuntimeException("Failed to consume batch", e);
        }
    }

    private long tryStampBatch(
        EntryRecordBatchHandler entryRecordBatchHandler,
        long entriesOffset,
        int entriesCount,
        long totalAmount,
        long walSequenceId
    ) {
        //final int walSequenceIdIndex = (int) (entriesOffset / this.walBatchEntriesCount);
        long copySize = (long) entriesCount * EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE;
        SYNC_MEMORY_BARRIER.setRelease(this, false);
        if (this.accumulatedToCopySize + copySize >= this.maxBatchSlotSize) {
            stampWalSequenceIdIndex++;
            this.accumulatedToCopySize = 0;
        } else {
            this.accumulatedToCopySize += copySize;
        }
        if (stampWalSequenceIdIndex >= this.walSequenceIds.length) {
            stampWalSequenceIdIndex = 0;
        }
        this.walSequenceIds[stampWalSequenceIdIndex] = walSequenceId;
        this.copySizes[stampWalSequenceIdIndex] = this.copySizes[stampWalSequenceIdIndex] + copySize;
//        this.copyOffsets[stampWalSequenceIdIndex] = copyOffsets;
        SYNC_MEMORY_BARRIER.setRelease(this, true);
        long batchSize = entriesCountBatchSize(entriesCount);
//        WAL_SEQUENCE_IDS_COUNT_MEMORY_BARRIER.getAndAdd();
//        if (walSequenceIdsReadyToReadSize <= this.maxBatchSlotSize() - batchSize) {
//            walSequenceIdsReadyToReadSize += batchSize;
//        }
//        if (walSequenceIdsReadyToReadCount <= this.maxBatchSlotSize() - batchSize) {
//            walSequenceIdsReadyToReadCount += batchSize;
//        }
//        if (batchSizeAccumulated <= maxBatchSlotSize - batchSize) {
//            this.walSequenceId = walSequenceId;
//            batchSizeAccumulated += batchSize;
//        }
        int maxAttempts = 100;
        int spinAttempts = 20;
        int yieldAttempts = 50;
        for (int attempt = 0; attempt < maxAttempts; attempt++ ) {
            long currentReadOffset = (long) READ_OFFSET_MEMORY_BARRIER.getAcquire(this) % this.arenaSize;
            long currentStampOffset = (long) STAMP_OFFSET_MEMORY_BARRIER.getAndAdd(this, copySize) % this.arenaSize;

//        long pendingBatches = currentReadOffset - currentStampOffset;// - currentReadOffset;

            if (currentStampOffset > currentReadOffset - copySize) {
                if (attempt < spinAttempts) {
                    Thread.onSpinWait();
                } else if (attempt < spinAttempts + yieldAttempts) {
                    Thread.yield();
                } else {
                    long parkNanos = 1_000L << Math.min(attempt - spinAttempts - yieldAttempts - yieldAttempts, 16);
                    LockSupport.parkNanos(parkNanos);
                }
                continue;
            }

            try {
                STAMP_STATUS_MEMORY_BARRIER.set(this, MemoryBarrierOperationStatus.HANDLING.memoryOrdinal());

                VarHandle.storeStoreFence();

                writePostgresBatchHeader(currentStampOffset, entriesCount, totalAmount);
                copyPostgresBinaryRecords(currentStampOffset, entriesOffset, entryRecordBatchHandler, entriesCount);
                writePostgresTerminator(currentStampOffset, entriesCount);

                final long checksum = calculateBatchChecksum(currentStampOffset, entriesCount);

                ringBufferSegment.set(
                    ValueLayout.JAVA_LONG,
                    currentStampOffset + BATCH_CHECKSUM_OFFSET,
                    checksum
                );

                // ✅ Обновляем глобальный счетчик
                long totalBatches = ringBufferSegment.get(ValueLayout.JAVA_LONG, TOTAL_BATCHES_OFFSET);
                ringBufferSegment.set(ValueLayout.JAVA_LONG, TOTAL_BATCHES_OFFSET, totalBatches + 1);

                VarHandle.releaseFence();

                STAMP_STATUS_MEMORY_BARRIER.setRelease(this, MemoryBarrierOperationStatus.COMPLETED.memoryOrdinal());
                if (batchSize % walBatchEntriesCount != 0) {
                    return ((batchSize / walBatchEntriesCount) + 1) * walBatchEntriesCount;
                } else return batchSize;
            } catch (Exception exception) {
                VarHandle.releaseFence();
                STAMP_STATUS_MEMORY_BARRIER.setRelease(this, MemoryBarrierOperationStatus.ERROR.memoryOrdinal());
                throw new RuntimeException("Failed to publish batch", exception);
            }
        }
        throw new IllegalStateException("Failed to publish batch after " + maxAttempts + " attempts");
    }

    private void writePostgresBatchHeader(
        long batchSlotOffset,
//        UUID accountId,
        int entryCount,
        long totalAmount
    ) {
        MemorySegment.copy(
            MemorySegment.ofArray(postgresSignature), 0,
            ringBufferSegment,
            batchSlotOffset + POSTGRES_SIGNATURE_OFFSET,
            postgresSignature.length
        );

        ringBufferSegment.set(ValueLayout.JAVA_INT, batchSlotOffset + POSTGRES_FLAGS_OFFSET, 0);

        ringBufferSegment.set(ValueLayout.JAVA_INT, batchSlotOffset + POSTGRES_EXTENSION_OFFSET, 0);

        ringBufferSegment.set(ValueLayout.JAVA_INT, batchSlotOffset + BATCH_ENTRIES_COUNT_OFFSET, entryCount);
        ringBufferSegment.set(ValueLayout.JAVA_LONG, batchSlotOffset + BATCH_TOTAL_AMOUNT_OFFSET, totalAmount);
        ringBufferSegment.set(
            ValueLayout.JAVA_LONG,
            batchSlotOffset + BATCH_TIMESTAMP_OFFSET,
            System.currentTimeMillis()
        );
    }

    private void copyPostgresBinaryRecords(
        long batchSlotOffset,
        long entriesOffset,
        EntryRecordBatchHandler sourceLayout,
        int entriesCount
    ) {
        long copySize = (long) entriesCount * EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE;

        MemorySegment.copy(
            sourceLayout.memorySegment(),
            entriesOffset,
            ringBufferSegment,
            batchSlotOffset + BATCH_BODY_OFFSET,
            copySize
        );
    }

    private void writePostgresTerminator(long batchSlotOffset, int entriesCount) {
        long terminatorOffset = batchSlotOffset + BATCH_BODY_OFFSET
            + ((long) entriesCount * EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE);

        // PostgreSQL binary format terminator
        ringBufferSegment.set(ValueLayout.JAVA_SHORT, terminatorOffset, (short) -1);
    }

    public long passedBarrierOffset() {
        final long arenaHalfSize = arenaSize >> 1;
        final long offset = (long) STAMP_OFFSET_MEMORY_BARRIER.getAcquire(this);
        if (offset >= arenaHalfSize) {
            return arenaHalfSize;
        }
        return 0;
//        final long arenaHalfSize = arenaSize >> 1;
//        if (stampOffset == arenaHalfSize) {
//            return 0;
//        }
//        return arenaHalfSize;
    }

    public int barrierPassedEntriesCount(long dataSize) {
        final long arenaHalfSize = arenaSize >> 1;
        if (stampOffset >= arenaHalfSize) {
            return (int) (dataSize - arenaHalfSize) / EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE;
        }
        return (int) dataSize / EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE;
    }

    private long calculateBatchChecksum(long batchSlotOffset, int entriesCount) {
        long checksum = 0;
        long dataOffset = batchSlotOffset + BATCH_BODY_OFFSET;
        long dataSize = (long) entriesCount * EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE;

        // XOR checksum по 8-байтовым блокам
        for (long i = 0; i <= dataSize - 8; i += 8) {
            long value = ringBufferSegment.get(ValueLayout.JAVA_LONG, dataOffset + i);
            checksum ^= value;
        }

        // Обрабатываем остаток
        for (long i = dataSize - (dataSize % 8); i < dataSize; i++) {
            checksum ^= ringBufferSegment.get(ValueLayout.JAVA_BYTE, dataOffset + i);
        }

        return checksum;
    }

    private long entriesCountBatchSize(int entriesCount) {
        return BATCH_HEADER_SIZE
            + ((long) entriesCount * EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE)
            + 2;
    }

    private void clearBatchSlot(long batchSlotOffset) {
        // Очищаем весь slot для переиспользования
        ringBufferSegment.asSlice(batchSlotOffset, batchSize(batchSlotOffset)).fill((byte) 0);
    }

    public boolean stampBatchForwardPassBarrier(
//        UUID accountId,
        long walSequenceId,
        EntryRecordBatchHandler entryRecordBatchHandler,
        long entriesOffset,
        int entryRecordCount,
        long totalAmount
    ) {
        final long entriesBatchSize = tryStampBatch(entryRecordBatchHandler, entriesOffset, entryRecordCount, totalAmount, walSequenceId);
        offsetForward(entriesBatchSize);
        return offsetBarrierPassed(entriesBatchSize);
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

//    public String getDiagnostics() {
//        return String.format(
//            "PostgresBinaryBatchLayout[capacity=%d, size=%d, utilization=%.1f%%, " +
//                "write_index=%d, read_index=%d, total_batches=%d]",
//            maxBatches, size(), getUtilization(),
//            (long) STAMP_OFFSET_MEMORY_BARRIER.getAcquire(this), (long) READ_OFFSET_MEMORY_BARRIER.get(this),
//            ringBufferSegment.get(ValueLayout.JAVA_LONG, TOTAL_BATCHES_OFFSET)
//        );
//    }
//
//    public long size() {
//        return Math.max(0, stampOffset.get() - readOffset.get());
//    }
//
//    public boolean isEmpty() {
//        return readOffset.get() >= stampOffset.get();
//    }
//
//    public boolean isFull() {
//        return stampOffset.get() - readOffset.get() >= maxBatches;
//    }
//
//    public double getUtilization() {
//        return (double) size() / maxBatches * 100.0;
//    }
//
//    public boolean isHealthy() {
//        long write = stampOffset.get();
//        long read = readOffset.get();
//
//        if (read > write) {
//            log.error("Ring buffer inconsistency: read_index={} > write_index={}", read, write);
//            return false;
//        }
//
//        if (write - read > maxBatches) {
//            log.error("Ring buffer overflow: write_index={}, read_index={}, capacity={}",
//                write, read, maxBatches);
//            return false;
//        }
//
//        return true;
//    }

//    private long calculateChecksum(EntryRecordBatchHandler entryRecordBatchHandler, long entryRecordCount) {
//        long checksum = 0;
//        long length = entryRecordCount * this.maxBatchSlotSize;
//
//        // ✅ XOR checksum по 8-байтовым блокам
//        for (int i = 0; i <= length - BATCH_CHECKSUM_SIZE; i += 8) {
//            long value = entryRecordBatchHandler.memorySegment().get(ValueLayout.JAVA_LONG, offset + i);
//            checksum ^= value;
//        }
//
//        // ✅ Обрабатываем остаток
//        for (long i = length - (length % BATCH_CHECKSUM_SIZE); i < length; i++) {
//            checksum ^= entryRecordBatchHandler.memorySegment().get(ValueLayout.JAVA_LONG, offset + i);
//        }
//
//        return checksum;
//    }
}
