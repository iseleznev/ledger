package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler;

import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.prevprocessor.RingBufferProcessor;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Data
@Accessors(fluent = true)
public class OldSharedPostgreSqlEntryRecordRingBufferHandler implements RingBufferHandler {

    private static final VarHandle STAMP_MEMORY_BARRIER_STATUS = ValueLayout.JAVA_INT.varHandle();

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
    private final byte[] postgresSignature = "PGCOPY\n\377\r\n\0".getBytes();

    private long offset;
    private long arenaSize;

//    private final AtomicLong writeIndex = new AtomicLong(0);
//    private final AtomicLong readIndex = new AtomicLong(0);

    private final AtomicLong stampOffset = new AtomicLong(0);
    private final AtomicLong readOffset = new AtomicLong(0);

    public OldSharedPostgreSqlEntryRecordRingBufferHandler(MemorySegment memorySegment, long maxEntriesPerBatch) {
        this.ringBufferSegment = memorySegment;
//        this.maxBatches = maxBatches;
        this.arenaSize = memorySegment.byteSize();
        this.maxEntriesPerBatch = maxEntriesPerBatch;//(this.arenaSize - POSTGRES_HEADER_SIZE) / batchElementSize;
        this.maxBatchSlotSize = BATCH_HEADER_SIZE
            + (maxEntriesPerBatch + EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE)
            + 2;
        this.offset = 0;
        this.maxBatches = this.arenaSize / this.maxBatchSlotSize;


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

    public long batchChecksum() {
        return ringBufferSegment.get(ValueLayout.JAVA_LONG, offset + BATCH_CHECKSUM_OFFSET);
    }

    public void setBatchChecksum(long batchOffset, long checkSum) {
        ringBufferSegment.set(ValueLayout.JAVA_LONG, batchOffset + BATCH_CHECKSUM_OFFSET, checkSum);
    }

    public void setBatchChecksum(long checkSum) {
        ringBufferSegment.set(ValueLayout.JAVA_LONG, offset + BATCH_CHECKSUM_OFFSET, checkSum);
    }

    @Override
    public long batchSize(long batchOffset) {
        return entriesCountBatchSize(
            ringBufferSegment.get(ValueLayout.JAVA_INT, batchOffset + BATCH_ENTRIES_COUNT_OFFSET)
        );
    }

    public long batchSize() {
        return entriesCountBatchSize(
            ringBufferSegment.get(ValueLayout.JAVA_INT, offset + BATCH_ENTRIES_COUNT_OFFSET)
        );
    }

    @Override
    public long batchElementsCount(long batchOffset) {
        return ringBufferSegment.get(ValueLayout.JAVA_INT, batchOffset + BATCH_ENTRIES_COUNT_OFFSET);
    }

    public long batchElementsCount() {
        return ringBufferSegment.get(ValueLayout.JAVA_INT, offset + BATCH_ENTRIES_COUNT_OFFSET);
    }

    public long batchTotalAmount(long batchOffset) {
        return ringBufferSegment.get(ValueLayout.JAVA_INT, batchOffset + BATCH_TOTAL_AMOUNT_OFFSET);
    }

    public long batchTotalAmount() {
        return ringBufferSegment.get(ValueLayout.JAVA_INT, offset + BATCH_TOTAL_AMOUNT_OFFSET);
    }

//    private long slotOffset(long batchIndex) {

    /// /        return METADATA_SIZE + ((batchIndex % maxBatches) * batchSlotSize);
//        final long slotOffset = batchIndex * batchSlotSize;
//        return METADATA_SIZE + (batchIndex % maxBatches) * batchSlotSize);
//    }
    public boolean tryProcessBatch(
        RingBufferProcessor ringBufferProcessor
//        long currentBatchOffset
    ) {
        long currentReadOffset = readOffset.get();
        long currentStampOffset = stampOffset.get();

        // Проверяем есть ли данные
        if (currentReadOffset >= currentStampOffset) {
            return false;
        }

        // Пытаемся занять слот для чтения
        currentReadOffset = readOffset.incrementAndGet();
//        if (!readIndex.compareAndSet(currentReadOffset, currentReadOffset + 1)) {
//            return false;
//        }

        try {
            long batchSlotOffset = currentReadOffset;//currentBatchOffset;
            long batchSize = batchSize(batchSlotOffset);
            // ✅ Обрабатываем batch
            final long processed = ringBufferProcessor.process(this, batchSlotOffset, batchSize);

            if (processed == batchSize) {
                // ✅ Очищаем slot для переиспользования
                // clearBatchSlot(batchSlotOffset);
                log.debug("Successfully processed and cleared batch at slot {}", currentReadOffset);
            } else {
                // Откатываем read index если обработка не удалась
                readOffset.compareAndSet(currentReadOffset + 1, currentReadOffset);
                log.warn("Failed to process batch at slot {}, rolling back", currentReadOffset);
            }

            return processed == batchSize;

        } catch (Exception e) {
            // Откатываем read index при ошибке
            readOffset.compareAndSet(currentReadOffset + 1, currentReadOffset);
            log.error("Error consuming batch at slot {}", currentReadOffset, e);
            throw new RuntimeException("Failed to consume batch", e);
        }
    }

    private long tryStampBatch(
//        UUID accountId,
        EntryRecordBatchHandler entryRecordBatchHandler,
        long entriesOffset,
        int entriesCount,
        long totalAmount
    ) {
        long currentStampOffset = stampOffset.get();

        try {
            currentStampOffset = stampOffset.incrementAndGet();

//        if (!writeIndex.compareAndSet(currentWriteIndex, currentWriteIndex + 1)) {
//            log.trace("Failed to acquire write slot, another thread got it");
//            return false;
//        }
            STAMP_MEMORY_BARRIER_STATUS.setRelease(ringBufferSegment, currentStampOffset, MemoryBarrierOperationStatus.HANDLING);
            //long slotOffset = slotOffset(currentStampOffset);
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

            STAMP_MEMORY_BARRIER_STATUS.setRelease(ringBufferSegment, currentStampOffset, MemoryBarrierOperationStatus.COMPLETED);

//            log.debug("Successfully published batch for account {}: {} entries", accountId, entriesCount);
            return entriesCountBatchSize(entriesCount);
        } catch (Exception exception) {
            // Откатываем write index при ошибке
            stampOffset.compareAndSet(currentStampOffset + 1, currentStampOffset);
//            log.error("Error publishing batch for account {}", accountId, exception);
            throw new RuntimeException("Failed to publish batch", exception);
        }
    }

    private void writePostgresBatchHeader(
        long batchSlotOffset,
//        UUID accountId,
        int entryCount,
        long totalAmount
    ) {
        // PostgreSQL signature
        MemorySegment.copy(
            MemorySegment.ofArray(postgresSignature), 0,
            ringBufferSegment,
            batchSlotOffset + POSTGRES_SIGNATURE_OFFSET,
            postgresSignature.length
        );

        // PostgreSQL flags
        ringBufferSegment.set(ValueLayout.JAVA_INT, batchSlotOffset + POSTGRES_FLAGS_OFFSET, 0);

        // PostgreSQL extension
        ringBufferSegment.set(ValueLayout.JAVA_INT, batchSlotOffset + POSTGRES_EXTENSION_OFFSET, 0);

        // Наши metadata
//        ringBufferSegment.set(
//            ValueLayout.JAVA_LONG,
//            batchSlotOffset + BATCH_ACCOUNT_ID_MOST_SIGNIFICANT_OFFSET,
//            accountId.getMostSignificantBits()
//        );
//        ringBufferSegment.set(
//            ValueLayout.JAVA_LONG,
//            batchSlotOffset + BATCH_ACCOUNT_ID_LEAST_SIGNIFICANT_OFFSET,
//            accountId.getLeastSignificantBits()
//        );
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

        // ✅ Zero-copy transfer от account layout к batch ring buffer
        MemorySegment.copy(
            sourceLayout.memorySegment(),
            entriesOffset,
            ringBufferSegment,
            batchSlotOffset,
            copySize
        );

//        log.trace("Copied {} PostgreSQL binary records ({} bytes) to batch slot",
//            entryCount, copySize);
    }

    private void writePostgresTerminator(long batchSlotOffset, int entriesCount) {
        long terminatorOffset = batchSlotOffset + BATCH_BODY_OFFSET
            + ((long) entriesCount * EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE);

        // PostgreSQL binary format terminator
        ringBufferSegment.set(ValueLayout.JAVA_SHORT, terminatorOffset, (short) -1);
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
        EntryRecordBatchHandler entryRecordBatchHandler,
        long entriesOffset,
        int entryRecordCount,
        long totalAmount
    ) {
        final long entriesBatchSize = tryStampBatch(entryRecordBatchHandler, entriesOffset, entryRecordCount, totalAmount);
        offsetForward(entriesBatchSize);
        return offsetBarrierPassed(entriesBatchSize);

//        return barrier;
//        if (offset + ENTRY_SIZE >= arenaSize) {
//            return false;
//        }
//        offset = offset + ENTRY_SIZE;
    }

    public void offsetForward(long batchRawSize) {
//        final long length = entryRecordCount * this.batchSize() + BATCH_BODY_OFFSET;
        if (offset + batchRawSize >= arenaSize) {
            offset = 0;
        } else {
            offset = offset + batchRawSize;
        }
//        return offset;
    }

    public boolean offsetBarrierPassed(long batchRawSize) {
//        final long length = entryRecordCount * this.batchSize() + BATCH_BODY_OFFSET;
        return offset == 0 || (offset + batchRawSize) == (arenaSize >> 1);
    }

    public void resetOffset() {
        offset = 0;
    }

    public String getDiagnostics() {
        return String.format(
            "PostgresBinaryBatchLayout[capacity=%d, size=%d, utilization=%.1f%%, " +
                "write_index=%d, read_index=%d, total_batches=%d]",
            maxBatches, size(), getUtilization(),
            stampOffset.get(), readOffset.get(),
            ringBufferSegment.get(ValueLayout.JAVA_LONG, TOTAL_BATCHES_OFFSET)
        );
    }

    public long size() {
        return Math.max(0, stampOffset.get() - readOffset.get());
    }

    public boolean isEmpty() {
        return readOffset.get() >= stampOffset.get();
    }

    public boolean isFull() {
        return stampOffset.get() - readOffset.get() >= maxBatches;
    }

    public double getUtilization() {
        return (double) size() / maxBatches * 100.0;
    }

    public boolean isHealthy() {
        long write = stampOffset.get();
        long read = readOffset.get();

        if (read > write) {
            log.error("Ring buffer inconsistency: read_index={} > write_index={}", read, write);
            return false;
        }

        if (write - read > maxBatches) {
            log.error("Ring buffer overflow: write_index={}, read_index={}, capacity={}",
                write, read, maxBatches);
            return false;
        }

        return true;
    }

    private long calculateChecksum(EntryRecordBatchHandler entryRecordBatchHandler, long entryRecordCount) {
        long checksum = 0;
        long length = entryRecordCount * this.maxBatchSlotSize;

        // ✅ XOR checksum по 8-байтовым блокам
        for (int i = 0; i <= length - BATCH_CHECKSUM_SIZE; i += 8) {
            long value = entryRecordBatchHandler.memorySegment().get(ValueLayout.JAVA_LONG, offset + i);
            checksum ^= value;
        }

        // ✅ Обрабатываем остаток
        for (long i = length - (length % BATCH_CHECKSUM_SIZE); i < length; i++) {
            checksum ^= entryRecordBatchHandler.memorySegment().get(ValueLayout.JAVA_LONG, offset + i);
        }

        return checksum;
    }
}
