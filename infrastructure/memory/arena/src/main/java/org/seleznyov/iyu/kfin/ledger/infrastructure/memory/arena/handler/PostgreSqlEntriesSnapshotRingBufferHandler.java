package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler;

import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.prevprocessor.RingBufferProcessor;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

@Slf4j
@Data
@Accessors(fluent = true)
public class PostgreSqlEntriesSnapshotRingBufferHandler implements RingBufferHandler {

    private static final VarHandle STAMP_STATUS_MEMORY_BARRIER;
    private static final VarHandle WAL_SEQUENCE_ID_MEMORY_BARRIER;
    private static final VarHandle READ_OFFSET_MEMORY_BARRIER;
    private static final VarHandle STAMP_OFFSET_MEMORY_BARRIER;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            // Для статических полей
            STAMP_STATUS_MEMORY_BARRIER = lookup.findVarHandle(
                PostgreSqlEntriesSnapshotRingBufferHandler.class, "stampStatus", int.class);
            WAL_SEQUENCE_ID_MEMORY_BARRIER = lookup.findVarHandle(
                PostgreSqlEntryRecordRingBufferHandler.class, "walSequenceId", long.class);
            READ_OFFSET_MEMORY_BARRIER = lookup.findVarHandle(
                PostgreSqlEntriesSnapshotRingBufferHandler.class, "readOffset", long.class);
            STAMP_OFFSET_MEMORY_BARRIER = lookup.findVarHandle(
                PostgreSqlEntriesSnapshotRingBufferHandler.class, "stampOffset", long.class);
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

    private static final int BATCH_SNAPSHOTS_COUNT_OFFSET = (int) (BATCH_ACCOUNT_ID_LEAST_SIGNIFICANT_OFFSET + BATCH_ACCOUNT_ID_TYPE.byteSize());
    private static final ValueLayout BATCH_SNAPSHOTS_COUNT_TYPE = ValueLayout.JAVA_INT;

    private static final int BATCH_TOTAL_AMOUNT_OFFSET = (int) (BATCH_SNAPSHOTS_COUNT_OFFSET + BATCH_SNAPSHOTS_COUNT_TYPE.byteSize());
    private static final ValueLayout BATCH_TOTAL_AMOUNT_TYPE = ValueLayout.JAVA_LONG;

    private static final int BATCH_TIMESTAMP_OFFSET = (int) (BATCH_TOTAL_AMOUNT_OFFSET + BATCH_TOTAL_AMOUNT_TYPE.byteSize());
    private static final ValueLayout BATCH_TIMESTAMP_TYPE = ValueLayout.JAVA_LONG;

    private static final int BATCH_HEADER_SIZE_RAW = (int) (BATCH_TIMESTAMP_OFFSET + BATCH_TIMESTAMP_TYPE.byteSize());
    private static final int BATCH_HEADER_SIZE = (BATCH_HEADER_SIZE_RAW + CPU_CACHE_LINE_SIZE - 1) & -CPU_CACHE_LINE_SIZE;

    private static final int BATCH_BODY_OFFSET = BATCH_HEADER_SIZE;

    private final MemorySegment ringBufferSegment;
    //    private final long maxBatches;
    private final long maxBatchSlotSize;
    private final long maxSnapshotsPerBatch;
    private final long maxBatches;
    private final byte[] postgresSignature = "PGCOPY\n\377\r\n\0".getBytes();

    private long offset;
    private long arenaSize;
    private long arenaHalfSize;

    private long walSequenceId;
    private int stampStatus;
    private long readOffset = 0;
    private long stampOffset = 0;

//    private final AtomicLong writeIndex = new AtomicLong(0);
//    private final AtomicLong readIndex = new AtomicLong(0);

//    private final AtomicLong stampOffset = new AtomicLong(0);
//    private final AtomicLong readOffset = new AtomicLong(0);

    public PostgreSqlEntriesSnapshotRingBufferHandler(MemorySegment memorySegment, long maxSnapshotsPerBatch) {
        this.ringBufferSegment = memorySegment;
//        this.maxBatches = maxBatches;
        this.arenaSize = memorySegment.byteSize();
        this.maxSnapshotsPerBatch = maxSnapshotsPerBatch;//(this.arenaSize - POSTGRES_HEADER_SIZE) / batchElementSize;
        this.maxBatchSlotSize = BATCH_HEADER_SIZE
            + (maxSnapshotsPerBatch + EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE)
            + 2;
        this.offset = 0;
        this.maxBatches = this.arenaSize / this.maxBatchSlotSize;

        this.arenaHalfSize = this.arenaSize >> 1;

        ringBufferSegment.asSlice(0, METADATA_SIZE).fill((byte) 0);
        ringBufferSegment.set(ValueLayout.JAVA_LONG, WRITE_INDEX_OFFSET, 0L);
        ringBufferSegment.set(ValueLayout.JAVA_LONG, READ_INDEX_OFFSET, 0L);
        ringBufferSegment.set(ValueLayout.JAVA_LONG, TOTAL_BATCHES_OFFSET, 0L);
    }

    public long walSequenceId(long batchOffset) {
        final int status = (int) STAMP_STATUS_MEMORY_BARRIER.getAcquire(ringBufferSegment, batchOffset);
        if (status != MemoryBarrierOperationStatus.COMPLETED.ordinal()) {
            throw new IllegalStateException("Batch not ready: " + status);
        }
        return ringBufferSegment.get(ValueLayout.JAVA_LONG, batchOffset + BATCH_BODY_OFFSET + EntriesSnapshotSharedBatchHandler.WAL_SEQUENCE_ID_OFFSET);
//        return (long) STAMP_MEMORY_BARRIER_WAL_SEQUENCE_ID.getAcquire(
//            ringBufferSegment,
//            batchOffset + BATCH_BODY_OFFSET + EntryRecordBatchHandler.WAL_SEQUENCE_ID_OFFSET
//        );
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
        return snapshotsCountBatchSize(
            ringBufferSegment.get(ValueLayout.JAVA_INT, batchOffset + BATCH_SNAPSHOTS_COUNT_OFFSET)
        );
    }

    public long batchSize() {
        return snapshotsCountBatchSize(
            ringBufferSegment.get(ValueLayout.JAVA_INT, offset + BATCH_SNAPSHOTS_COUNT_OFFSET)
        );
    }

    @Override
    public long batchElementsCount(long batchOffset) {
        return ringBufferSegment.get(ValueLayout.JAVA_INT, batchOffset + BATCH_SNAPSHOTS_COUNT_OFFSET);
    }

    public long batchElementsCount() {
        return ringBufferSegment.get(ValueLayout.JAVA_INT, offset + BATCH_SNAPSHOTS_COUNT_OFFSET);
    }

    public long batchTotalAmount(long batchOffset) {
        return ringBufferSegment.get(ValueLayout.JAVA_INT, batchOffset + BATCH_TOTAL_AMOUNT_OFFSET);
    }

    public long batchTotalAmount() {
        return ringBufferSegment.get(ValueLayout.JAVA_INT, offset + BATCH_TOTAL_AMOUNT_OFFSET);
    }

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

        final long slotCount = ringBufferSegment.get(ValueLayout.JAVA_INT, currentReadOffset + BATCH_SNAPSHOTS_COUNT_OFFSET);
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

        long lastSlotSequenceId = ringBufferSegment.get(ValueLayout.JAVA_LONG, currentReadOffset + EntryRecordBatchHandler.WAL_SEQUENCE_ID_OFFSET);
        long slotOffset = currentReadOffset + slotSize;
        while (slotOffset < batchSize + currentReadOffset && slotOffset < slotOffsetBarrier) {
            final long nextSlotCount = ringBufferSegment.get(ValueLayout.JAVA_INT, slotOffset + BATCH_SNAPSHOTS_COUNT_OFFSET);
            final long nextSlotSize = BATCH_HEADER_SIZE
                + (nextSlotCount * EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE)
                + 2;
            lastSlotSequenceId = ringBufferSegment.get(ValueLayout.JAVA_LONG, currentReadOffset + EntryRecordBatchHandler.WAL_SEQUENCE_ID_OFFSET);
            slotOffset += nextSlotSize;
        }
        long nextReadOffset = slotOffset;

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
//                readOffset.compareAndSet(currentReadOffset + 1, currentReadOffset);
                VarHandle.releaseFence();
                log.warn("Failed to process batch at slot {}, rolling back", currentReadOffset);
                throw new IllegalStateException("Failed to process batch");
            }

            return lastSlotSequenceId;

        } catch (Exception e) {
            // Откатываем read index при ошибке
            VarHandle.releaseFence();
            log.error("Error consuming batch at slot {}", currentReadOffset, e);
            throw new RuntimeException("Failed to consume batch", e);
        }
    }

    private long tryStampBatch(
        EntriesSnapshotSharedBatchHandler snapshotSharedBatchHandler,
        long entriesOffset,
        int snapshotsCount,
        long totalAmount
    ) {
        long copySize = (long) snapshotsCount * EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE;
        int maxAttempts = 100;
        int spinAttempts = 20;
        int yieldAttempts = 50;
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            long currentReadOffset = (long) READ_OFFSET_MEMORY_BARRIER.getAcquire(this);
            long currentStampOffset = (long) STAMP_OFFSET_MEMORY_BARRIER.getAndAdd(this, copySize);

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

//        if (!writeIndex.compareAndSet(currentWriteIndex, currentWriteIndex + 1)) {
//            log.trace("Failed to acquire write slot, another thread got it");
//            return false;
//        }

                //long slotOffset = slotOffset(currentStampOffset);

                VarHandle.storeStoreFence();

                writePostgresBatchHeader(currentStampOffset, snapshotsCount, totalAmount);
                copyPostgresBinaryRecords(currentStampOffset, entriesOffset, snapshotSharedBatchHandler, snapshotsCount);
                writePostgresTerminator(currentStampOffset, snapshotsCount);

                final long checksum = calculateBatchChecksum(currentStampOffset, snapshotsCount);

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

//                log.debug("Successfully published batch for account {}: {} snapshots", accountId, snapshotsCount);
                return snapshotsCountBatchSize(snapshotsCount);
            } catch (Exception exception) {
                // Откатываем write index при ошибке
                VarHandle.releaseFence();
                STAMP_STATUS_MEMORY_BARRIER.setRelease(this, MemoryBarrierOperationStatus.ERROR.memoryOrdinal());
//            stampOffset.compareAndSet(currentStampOffset + 1, currentStampOffset);
//            log.error("Error publishing batch for account {}", accountId, exception);
                throw new RuntimeException("Failed to publish batch", exception);
            }
        }
        throw new IllegalStateException("Failed to publish batch after " + maxAttempts + " attempts");
    }

    private void writePostgresBatchHeader(long batchSlotOffset, int entryCount, long totalAmount) {
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
        ringBufferSegment.set(ValueLayout.JAVA_INT, batchSlotOffset + BATCH_SNAPSHOTS_COUNT_OFFSET, entryCount);
        ringBufferSegment.set(ValueLayout.JAVA_LONG, batchSlotOffset + BATCH_TOTAL_AMOUNT_OFFSET, totalAmount);
        ringBufferSegment.set(
            ValueLayout.JAVA_LONG,
            batchSlotOffset + BATCH_TIMESTAMP_OFFSET,
            System.currentTimeMillis()
        );
    }

    private void copyPostgresBinaryRecords(
        long batchSlotOffset,
        long snapshotsOffset,
        EntriesSnapshotSharedBatchHandler sourceLayout,
        int entriesCount
    ) {
        long copySize = (long) entriesCount * EntriesSnapshotSharedBatchHandler.POSTGRES_ENTRIES_SNAPSHOT_SIZE;

        // ✅ Zero-copy transfer от account layout к batch ring buffer
        MemorySegment.copy(
            sourceLayout.memorySegment(),
            snapshotsOffset,
            ringBufferSegment,
            batchSlotOffset,
            copySize
        );

//        log.trace("Copied {} PostgreSQL binary records ({} bytes) to batch slot",
//            entryCount, copySize);
    }

    private void writePostgresTerminator(long batchSlotOffset, int snapshotsCount) {
        long terminatorOffset = batchSlotOffset + BATCH_BODY_OFFSET
            + ((long) snapshotsCount * EntriesSnapshotSharedBatchHandler.POSTGRES_ENTRIES_SNAPSHOT_SIZE);

        // PostgreSQL binary format terminator
        ringBufferSegment.set(ValueLayout.JAVA_SHORT, terminatorOffset, (short) -1);
    }

    private long calculateBatchChecksum(long batchSlotOffset, int snapshotsCount) {
        long checksum = 0;
        long dataOffset = batchSlotOffset + BATCH_BODY_OFFSET;
        long dataSize = (long) snapshotsCount * EntriesSnapshotSharedBatchHandler.POSTGRES_ENTRIES_SNAPSHOT_SIZE;

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

    private long snapshotsCountBatchSize(int snapshotsCount) {
        return BATCH_HEADER_SIZE
            + (snapshotsCount + EntriesSnapshotSharedBatchHandler.POSTGRES_ENTRIES_SNAPSHOT_SIZE)
            + 2;
    }

    private void clearBatchSlot(long batchSlotOffset) {
        // Очищаем весь slot для переиспользования
        ringBufferSegment.asSlice(batchSlotOffset, batchSize(batchSlotOffset)).fill((byte) 0);
    }

    public boolean stampBatchForwardPassBarrier(
//        UUID accountId,
        EntriesSnapshotSharedBatchHandler snapshotBatchHandler,
        long snapshotsOffset,
        int snapshotsCount,
        long totalAmount
    ) {
        final long entriesBatchSize = tryStampBatch(snapshotBatchHandler, snapshotsOffset, snapshotsCount, totalAmount);
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

//    public String getDiagnostics() {
//        return String.format(
//            "PostgresBinaryBatchLayout[capacity=%d, size=%d, utilization=%.1f%%, " +
//                "write_index=%d, read_index=%d, total_batches=%d]",
//            maxBatches, size(), getUtilization(),
//            stampOffset.get(), readOffset.get(),
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
//
//    private long calculateChecksum(EntriesSnapshotSharedBatchHandler snapshotSharedBatchHandler, long snapshotsCount) {
//        long checksum = 0;
//        long length = snapshotsCount * this.maxBatchSlotSize;
//
//        // ✅ XOR checksum по 8-байтовым блокам
//        for (int i = 0; i <= length - BATCH_CHECKSUM_SIZE; i += 8) {
//            long value = snapshotSharedBatchHandler.memorySegment().get(ValueLayout.JAVA_LONG, offset + i);
//            checksum ^= value;
//        }
//
//        // ✅ Обрабатываем остаток
//        for (long i = length - (length % BATCH_CHECKSUM_SIZE); i < length; i++) {
//            checksum ^= snapshotSharedBatchHandler.memorySegment().get(ValueLayout.JAVA_LONG, offset + i);
//        }
//
//        return checksum;
//    }
}
