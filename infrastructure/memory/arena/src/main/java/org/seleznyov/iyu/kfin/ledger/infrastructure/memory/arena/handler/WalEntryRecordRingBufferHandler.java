package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler;

import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.prevprocessor.RingBufferProcessor;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.locks.LockSupport;

@Slf4j
@Data
@Accessors(fluent = true)
public class WalEntryRecordRingBufferHandler implements RingBufferHandler {

    private static final VarHandle STAMP_STATUS_MEMORY_BARRIER;
    private static final VarHandle WAL_SEQUENCE_ID_MEMORY_BARRIER;
    private static final VarHandle READ_OFFSET_MEMORY_BARRIER;
    private static final VarHandle STAMP_OFFSET_MEMORY_BARRIER;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            // Для статических полей
            STAMP_STATUS_MEMORY_BARRIER = lookup.findVarHandle(
                WalEntryRecordRingBufferHandler.class, "stampStatus", int.class);
            WAL_SEQUENCE_ID_MEMORY_BARRIER = lookup.findVarHandle(
                WalEntryRecordRingBufferHandler.class, "walSequenceId", long.class);
            READ_OFFSET_MEMORY_BARRIER = lookup.findVarHandle(
                WalEntryRecordRingBufferHandler.class, "readOffset", long.class);
            STAMP_OFFSET_MEMORY_BARRIER = lookup.findVarHandle(
                WalEntryRecordRingBufferHandler.class, "stampOffset", long.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final static int CPU_CACHE_LINE_SIZE = 64;

    // ===== RING BUFFER METADATA =====

    private static final int WRITE_INDEX_OFFSET = 0;                    // 8 bytes
    private static final int READ_INDEX_OFFSET = 8;                     // 8 bytes
    private static final int TOTAL_BATCHES_OFFSET = 16;                 // 8 bytes
    public static final int METADATA_SIZE = 64;                        // Cache-aligned

    // ===== WAL FILE BATCH HEADER =====

    private static final int FILE_BATCH_ID_OFFSET = 0;
    private static final ValueLayout FILE_BATCH_ID_TYPE = ValueLayout.JAVA_LONG;

    private static final int FILE_BATCH_TIMESTAMP_OFFSET = (int) (FILE_BATCH_ID_OFFSET + FILE_BATCH_ID_TYPE.byteSize());
    private static final ValueLayout FILE_BATCH_TIMESTAMP_TYPE = ValueLayout.JAVA_LONG;

    private static final int FILE_BATCH_ACCOUNT_COUNT_OFFSET = (int) (FILE_BATCH_TIMESTAMP_OFFSET + FILE_BATCH_TIMESTAMP_TYPE.byteSize());
    private static final ValueLayout FILE_BATCH_ACCOUNT_COUNT_TYPE = ValueLayout.JAVA_INT;

    private static final int FILE_BATCH_TOTAL_OPERATIONS_OFFSET = (int) (FILE_BATCH_ACCOUNT_COUNT_OFFSET + FILE_BATCH_ACCOUNT_COUNT_TYPE.byteSize());
    private static final ValueLayout FILE_BATCH_TOTAL_OPERATIONS_TYPE = ValueLayout.JAVA_INT;

    private static final int FILE_BATCH_TOTAL_SIZE_OFFSET = (int) (FILE_BATCH_TOTAL_OPERATIONS_OFFSET + FILE_BATCH_TOTAL_OPERATIONS_TYPE.byteSize());
    private static final ValueLayout FILE_BATCH_TOTAL_SIZE_TYPE = ValueLayout.JAVA_INT;

    private static final int FILE_BATCH_SHARD_ID_OFFSET = (int) (FILE_BATCH_TOTAL_SIZE_OFFSET + FILE_BATCH_TOTAL_SIZE_TYPE.byteSize());
    private static final ValueLayout FILE_BATCH_SHARD_ID_TYPE = ValueLayout.JAVA_INT;

    private static final int FILE_BATCH_RESERVED_OFFSET = (int) (FILE_BATCH_SHARD_ID_OFFSET + FILE_BATCH_SHARD_ID_TYPE.byteSize());
    private static final ValueLayout FILE_BATCH_RESERVED_TYPE = ValueLayout.JAVA_LONG;

    private static final int FILE_BATCH_CHECKSUM_OFFSET = (int) (FILE_BATCH_RESERVED_OFFSET + FILE_BATCH_RESERVED_TYPE.byteSize());
    private static final ValueLayout FILE_BATCH_CHECKSUM_TYPE = ValueLayout.JAVA_LONG;

    private static final int FILE_BATCH_HEADER_SIZE_RAW = (int) (FILE_BATCH_CHECKSUM_OFFSET + FILE_BATCH_CHECKSUM_TYPE.byteSize());

    private static final int FILE_BATCH_HEADER_SIZE = (FILE_BATCH_HEADER_SIZE_RAW + CPU_CACHE_LINE_SIZE - 1) & -CPU_CACHE_LINE_SIZE;

    // ===== ACCOUNT BATCH HEADER (within file batch) =====

    public static final int BATCH_WAL_SEQUENCE_ID_OFFSET = FILE_BATCH_HEADER_SIZE;
    public static final ValueLayout BATCH_WAL_SEQUENCE_ID_TYPE = ValueLayout.JAVA_LONG;

    private static final int WAL_BATCH_SHARD_ID_OFFSET = (int) (BATCH_WAL_SEQUENCE_ID_OFFSET + BATCH_WAL_SEQUENCE_ID_TYPE.byteSize());
    private static final ValueLayout WAL_BATCH_SHARD_ID_TYPE = ValueLayout.JAVA_INT;

    private static final int BATCH_ACCOUNT_ID_MSB_OFFSET = (int) (WAL_BATCH_SHARD_ID_OFFSET + WAL_BATCH_SHARD_ID_TYPE.byteSize());
    private static final ValueLayout BATCH_ACCOUNT_ID_TYPE = ValueLayout.JAVA_LONG;
    private static final int BATCH_ACCOUNT_ID_LSB_OFFSET = (int) (BATCH_ACCOUNT_ID_MSB_OFFSET + BATCH_ACCOUNT_ID_TYPE.byteSize());

    private static final int BATCH_ENTRIES_COUNT_OFFSET = (int) (BATCH_ACCOUNT_ID_LSB_OFFSET + BATCH_ACCOUNT_ID_TYPE.byteSize());
    private static final ValueLayout BATCH_ENTRIES_COUNT_TYPE = ValueLayout.JAVA_INT;

    private static final int BATCH_DATA_SIZE_OFFSET = (int) (BATCH_ENTRIES_COUNT_OFFSET + BATCH_ENTRIES_COUNT_TYPE.byteSize());
    private static final ValueLayout BATCH_DATA_SIZE_TYPE = ValueLayout.JAVA_INT;

    private static final int BATCH_TOTAL_AMOUNT_OFFSET = (int) (BATCH_DATA_SIZE_OFFSET + BATCH_DATA_SIZE_TYPE.byteSize());
    private static final ValueLayout BATCH_TOTAL_AMOUNT_TYPE = ValueLayout.JAVA_LONG;

    private static final int BATCH_ENTRIES_OFFSET_OFFSET = (int) (BATCH_TOTAL_AMOUNT_OFFSET + BATCH_TOTAL_AMOUNT_TYPE.byteSize());
    private static final ValueLayout BATCH_ENTRIES_OFFSET_TYPE = ValueLayout.JAVA_LONG;

    private static final int BATCH_TIMESTAMP_OFFSET = (int) (BATCH_ENTRIES_OFFSET_OFFSET + BATCH_ENTRIES_OFFSET_TYPE.byteSize());
    private static final ValueLayout BATCH_TIMESTAMP_TYPE = ValueLayout.JAVA_LONG;

    private static final int BATCH_CHECKSUM_OFFSET = (int) (BATCH_TIMESTAMP_OFFSET + BATCH_TIMESTAMP_TYPE.byteSize());
    private static final ValueLayout BATCH_CHECKSUM_TYPE = ValueLayout.JAVA_LONG;

    private static final int BATCH_RESERVED_OFFSET = (int) (BATCH_CHECKSUM_OFFSET + BATCH_CHECKSUM_TYPE.byteSize());
    private static final ValueLayout BATCH_RESERVED_TYPE = ValueLayout.JAVA_LONG;

    private static final int BATCH_HEADER_SIZE_RAW = (int) (BATCH_RESERVED_OFFSET + BATCH_RESERVED_TYPE.byteSize());

    private static final int BATCH_HEADER_SIZE = (BATCH_HEADER_SIZE_RAW + CPU_CACHE_LINE_SIZE - 1) & -CPU_CACHE_LINE_SIZE;

    private static final int BATCH_BODY_OFFSET = BATCH_HEADER_SIZE;

    // ===== INSTANCE FIELDS =====

    private final MemorySegment ringBufferSegment;
    private final int shardId;
    private final long maxBatchesPerFile;
    //    private long currentSlotOffset = METADATA_SIZE;
    private final long maxBatchSlotSize;
    private final long batchElementSize;
    private final long maxEntriesPerBatch;
    private final long maxBatches;

    private long arenaSize;

    private int stampStatus;
//    private long walSequenceId;
    private long stampOffset;
    private long readOffset;

    public WalEntryRecordRingBufferHandler(
        MemorySegment memorySegment,
        long maxEntriesPerBatch,
        long maxBatchesPerFile,
        int shardId
    ) {
        this.ringBufferSegment = memorySegment;
//        this.maxBatches = maxBatches;
        this.arenaSize = memorySegment.byteSize();
        this.maxEntriesPerBatch = maxEntriesPerBatch;//(this.arenaSize - POSTGRES_HEADER_SIZE) / batchElementSize;
        this.maxBatchSlotSize = BATCH_HEADER_SIZE
            + (this.maxEntriesPerBatch * EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE)
            + CPU_CACHE_LINE_SIZE; // padding for checksum and alignment
        this.batchElementSize = BATCH_HEADER_SIZE
            + EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE; // padding for checksum and alignment
//        this.maxBatchSlotSize = BATCH_HEADER_SIZE
//            + (maxAccountBatchesPerFile + EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE)
//            + 2;
//        this.maxBatches = this.arenaSize / this.maxBatchSlotSize;


//        ringBufferSegment.asSlice(0, METADATA_SIZE).fill((byte) 0);
//        ringBufferSegment.set(ValueLayout.JAVA_LONG, WRITE_INDEX_OFFSET, 0L);
//        ringBufferSegment.set(ValueLayout.JAVA_LONG, READ_INDEX_OFFSET, 0L);
//        ringBufferSegment.set(ValueLayout.JAVA_LONG, TOTAL_BATCHES_OFFSET, 0L);


        this.shardId = shardId;
        this.maxBatchesPerFile = maxBatchesPerFile;//(this.arenaSize - POSTGRES_HEADER_SIZE) / batchElementSize;

        // Calculate slot sizing
//        long maxPostgreSQLDataPerAccount = 1000L * EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE; // 1000 operations max
        this.maxBatches = (this.arenaSize - METADATA_SIZE) / this.maxBatchSlotSize;

        // Initialize metadata
        ringBufferSegment.asSlice(0, METADATA_SIZE).fill((byte) 0);
        ringBufferSegment.set(ValueLayout.JAVA_LONG, WRITE_INDEX_OFFSET, 0L);
        ringBufferSegment.set(ValueLayout.JAVA_LONG, READ_INDEX_OFFSET, 0L);
        ringBufferSegment.set(ValueLayout.JAVA_LONG, TOTAL_BATCHES_OFFSET, 0L);

        log.info("WAL Ring Buffer initialized: shard={}, max_slots={}, slot_size={}KB, max_account_batches={}",
            shardId, this.maxBatches, this.maxBatchSlotSize / 1024, this.maxBatchesPerFile);
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

    public long walSequenceId(long batchOffset) {
        return ringBufferSegment.get(ValueLayout.JAVA_LONG, batchOffset + BATCH_WAL_SEQUENCE_ID_OFFSET);
    }

    public long batchSize(long batchOffset, long entriesCount) {
        return EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE * entriesCount + BATCH_HEADER_SIZE;
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

//    public long nextWalSequenceId() {
//        return (long) WAL_SEQUENCE_ID_MEMORY_BARRIER.getAndAddRelease(this, 1);
//    }

//    public long readOffset() {
//        return (long) READ_OFFSET_MEMORY_BARRIER.getAndAddRelease(this, 1);
//    }

    /// /        return METADATA_SIZE + ((batchIndex % maxBatches) * batchSlotSize);
//        final long slotOffset = batchIndex * batchSlotSize;
//        return METADATA_SIZE + (batchIndex % maxBatches) * batchSlotSize);
//    }
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

        try {
            long batchSlotOffset = currentReadOffset;
            long batchSize = batchSize(batchSlotOffset);

            // Пытаемся занять слот для чтения
//            final long slotCount = ringBufferSegment.get(ValueLayout.JAVA_INT, currentReadOffset + BATCH_ENTRIES_COUNT_OFFSET);
//            final long slotSize = BATCH_HEADER_SIZE
//                + (slotCount * EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE)
//                + 2;

            long nextReadOffset = currentReadOffset + batchSize;

            // ✅ Обрабатываем batch
            final long processed = ringBufferProcessor.process(this, batchSlotOffset, batchSize);

            if (processed == batchSize) {
                // ✅ Очищаем slot для переиспользования
                // clearBatchSlot(batchSlotOffset);
                log.debug("Successfully processed and cleared batch at slot {}", currentReadOffset);
                READ_OFFSET_MEMORY_BARRIER.setRelease(this, nextReadOffset);
            } else {
                // Откатываем read index если обработка не удалась
//                READ_OFFSET_MEMORY_BARRIER.setRelease(this, startReadOffset);
                VarHandle.releaseFence();
                log.warn("Failed to process batch at slot {}, rolling back", currentReadOffset);
                throw new IllegalStateException("Failed to process batch");
            }

            return processed;

        } catch (Exception e) {
            // Откатываем read index при ошибке
//            READ_OFFSET_MEMORY_BARRIER.setRelease(this, startReadOffset);
            VarHandle.releaseFence();
            log.error("Error consuming batch at slot {}", currentReadOffset, e);
            throw new RuntimeException("Failed to consume batch", e);
        }
    }

    private void tryStampBatch(
//        long walSequenceId,
//        UUID accountId,
        EntryRecordBatchHandler entryRecordBatchHandler,
        int entriesCount,
        long entriesOffset,
        long[] stampResult
//        long totalAmount
    ) {
        final long walBatchSize = (long) entriesCount * EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE;
//        final long walBatchSize = postgresBatchSize + BATCH_HEADER_SIZE;
        int maxAttempts = 100;
        int spinAttempts = 20;
        int yieldAttempts = 50;
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            final long currentReadOffset = (long) READ_OFFSET_MEMORY_BARRIER.getAcquire(this);
            final long currentStampOffset = METADATA_SIZE + (long) STAMP_OFFSET_MEMORY_BARRIER.getAndAdd(this, walBatchSize);
            final long currentWalSequenceId = METADATA_SIZE + (long) WAL_SEQUENCE_ID_MEMORY_BARRIER.getAndAdd(this, 1);
//        long currentReadOffset = readOffset.get();
//        final long batchSlotOffset = METADATA_SIZE + currentStampOffset;

//            if (currentStampOffset >= currentReadOffset) {
//                return -1;
//            }
            if (currentStampOffset > currentReadOffset - walBatchSize) {
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
//            currentStampOffset = stampOffset.incrementAndGet();
                STAMP_STATUS_MEMORY_BARRIER.set(this, MemoryBarrierOperationStatus.HANDLING.memoryOrdinal());
//                final long currentStampOffset = (long) STAMP_OFFSET_MEMORY_BARRIER.getAndAdd(this, walBatchSize);
                VarHandle.storeStoreFence();
//        if (!writeIndex.compareAndSet(currentWriteIndex, currentWriteIndex + 1)) {
//            log.trace("Failed to acquire write slot, another thread got it");
//            return false;
//        }
//                STAMP_STATUS_MEMORY_BARRIER.setRelease(this, MemoryBarrierOperationStatus.HANDLING.memoryOrdinal());
                //long slotOffset = slotOffset(currentStampOffset);

//            if (batchesCount >= this.maxBatchesPerFile
//                || stampOffset.get() + walBatchSize > arenaSize
//            ) {
//
//                // Finalize current file batch
//                if (entriesCount > 0) {
//                    finalizeCurrentFileBatch();
//                }
//
//                // Start new file batch
//                startNewFileBatch();
//            }
//
                writeWalBatchHeader(
                    currentStampOffset,
                    currentWalSequenceId,
//                accountId,
                    entriesCount,
                    walBatchSize,
//                totalAmount,
                    entriesOffset
                );
                copyPostgreSQLData(
                    entryRecordBatchHandler,
                    entriesOffset,
                    currentStampOffset,
                    walBatchSize//(long) entriesCount * EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE
                );
//            updateFileBatchHeader(
//                currentStampOffset,
//                entriesCount,
//                entriesCount,
//                0
//            );
/*
        long currentSlotOffset,
        int batchEntriesCount,
        int totalEntriesCount,
        long totalFileSize

 */
//            writePostgresTerminator(currentStampOffset, entriesCount);

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

//            log.debug("Successfully published batch for account {}: {} entries", accountId, entriesCount);
                stampResult[0] = walBatchSize;
                stampResult[1] = currentWalSequenceId;
                //return currentWalSequenceId;//entriesCountBatchSize(entriesCount);
            } catch (Exception exception) {
                // Откатываем write index при ошибке
                VarHandle.releaseFence();
                STAMP_OFFSET_MEMORY_BARRIER.setRelease(this, MemoryBarrierOperationStatus.ERROR.memoryOrdinal());
//            log.error("Error publishing batch for account {}", accountId, exception);
                throw new RuntimeException("Failed to publish batch", exception);
            }
        }
        throw new IllegalStateException("Failed to publish batch after " + maxAttempts + " attempts");
    }

//    private void startNewFileBatch() {
//        currentFileBatchId = generateFileBatchId();
//        currentAccountBatchCount = 0;
//        currentTotalOperations = 0;
//        currentFileBatchSize = FILE_BATCH_HEADER_SIZE;
//
//        // Write file batch header
//        writeWalBatchHeader();
//
//        log.debug("Started new WAL file batch: shard={}, id={}", shardId, currentFileBatchId);
//    }

    private void writeWalBatchHeader(
        long offset,
        long walSequenceId,
//        UUID accountId,
        int entriesCount,
        long dataSize,
//        long totalAmount,
        long entriesOffset
    ) {
        ringBufferSegment.set(ValueLayout.JAVA_LONG, offset + BATCH_WAL_SEQUENCE_ID_OFFSET, walSequenceId);
        ringBufferSegment.set(ValueLayout.JAVA_LONG, offset + WAL_BATCH_SHARD_ID_OFFSET, shardId);
//        ringBufferSegment.set(ValueLayout.JAVA_LONG, offset + BATCH_ACCOUNT_ID_MSB_OFFSET, accountId.getMostSignificantBits());
//        ringBufferSegment.set(ValueLayout.JAVA_LONG, offset + BATCH_ACCOUNT_ID_LSB_OFFSET, accountId.getLeastSignificantBits());
        ringBufferSegment.set(ValueLayout.JAVA_INT, offset + BATCH_ENTRIES_COUNT_OFFSET, entriesCount);
        ringBufferSegment.set(ValueLayout.JAVA_INT, offset + BATCH_DATA_SIZE_OFFSET, (int) dataSize);
//        ringBufferSegment.set(ValueLayout.JAVA_LONG, offset + BATCH_TOTAL_AMOUNT_OFFSET, totalAmount);
        ringBufferSegment.set(ValueLayout.JAVA_LONG, offset + BATCH_ENTRIES_OFFSET_OFFSET, entriesOffset);
        ringBufferSegment.set(ValueLayout.JAVA_LONG, offset + BATCH_TIMESTAMP_OFFSET, LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli());
    }

    private void copyPostgreSQLData(
        EntryRecordBatchHandler sourceHandler,
        long sourceOffset,
        long targetOffset,
        long dataSize
    ) {
        // Direct copy PostgreSQL-ready data без дополнительных преобразований
        MemorySegment.copy(
            sourceHandler.memorySegment(),
            sourceOffset,
            ringBufferSegment,
            targetOffset,
            dataSize
        );
    }

//    private void updateFileBatchHeader(
////        long currentSlotOffset,
//        int batchEntriesCount
////        int totalEntriesCount,
////        long totalFileSize
//    ) {
//        ringBufferSegment.set(ValueLayout.JAVA_INT, currentSlotOffset + FILE_BATCH_ACCOUNT_COUNT_OFFSET, batchEntriesCount);

    /// /        ringBufferSegment.set(ValueLayout.JAVA_INT, currentSlotOffset + FILE_BATCH_TOTAL_OPERATIONS_OFFSET, totalEntriesCount);
    /// /        ringBufferSegment.set(ValueLayout.JAVA_INT, currentSlotOffset + FILE_BATCH_TOTAL_SIZE_OFFSET, (int) totalFileSize);
//    }

//    private void finalizeCurrentFileBatch(long currentBatchSize) {
//        // Update final file batch header
//        updateFileBatchHeader();
//
//        // Calculate and write checksum
//        long checksumOffset = currentSlotOffset;
//        long checksum = calculateFileBatchChecksum();
//        ringBufferSegment.set(ValueLayout.JAVA_LONG, checksumOffset, checksum);
//        currentFileBatchSize += 8; // Add checksum size
//
//        // Update ring buffer state
//        long finalSlotSize = alignToCache(currentFileBatchSize);
//        stampOffset.incrementAndGet();
//        currentSlotOffset += finalSlotSize;
//
//        // Wrap around if necessary
//        if (currentSlotOffset + maxSlotSize > ringBufferSegment.byteSize()) {
//            currentSlotOffset = METADATA_SIZE;
//        }
//
//        // Update total batches counter
//        long totalBatches = ringBufferSegment.get(ValueLayout.JAVA_LONG, TOTAL_BATCHES_OFFSET);
//        ringBufferSegment.set(ValueLayout.JAVA_LONG, TOTAL_BATCHES_OFFSET, totalBatches + 1);
//
//        log.debug("Finalized WAL file batch: shard={}, id={}, accounts={}, operations={}, size={} bytes",
//            shardId, currentFileBatchId, currentAccountBatchCount, currentTotalOperations, currentFileBatchSize);
//    }
    private long alignToCache(long size) {
        return (size + CPU_CACHE_LINE_SIZE - 1) & -CPU_CACHE_LINE_SIZE;
    }

//    private void copyPostgresBinaryRecords(
//        long batchSlotOffset,
//        long entriesOffset,
//        EntryRecordBatchHandler sourceLayout,
//        int entriesCount
//    ) {
//        long copySize = (long) entriesCount * EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE;
//
//        // ✅ Zero-copy transfer от account layout к batch ring buffer
//        MemorySegment.copy(
//            sourceLayout.memorySegment(),
//            entriesOffset,
//            ringBufferSegment,
//            batchSlotOffset,
//            copySize
//        );
//

    /// /        log.trace("Copied {} PostgreSQL binary records ({} bytes) to batch slot",
    /// /            entryCount, copySize);
//    }

//    private void writePostgresTerminator(long batchSlotOffset, int entriesCount) {
//        long terminatorOffset = batchSlotOffset + BATCH_BODY_OFFSET
//            + ((long) entriesCount * EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE);
//
//        // PostgreSQL binary format terminator
//        ringBufferSegment.set(ValueLayout.JAVA_SHORT, terminatorOffset, (short) -1);
//    }
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
//        long walShardSequenceId,
//        UUID accountId,
        EntryRecordBatchHandler entryRecordBatchHandler,
        long entriesOffset,
        int entriesCount,
        long[] stampResult
//        long totalAmount
    ) {
//        final long beforeForwardOffset = stampOffset.getAndAccumulate(
//            entriesCount * this.batchElementSize,
//            (currentOffset, increment) -> {
//                long newOffset = currentOffset + increment;
//                return newOffset >= arenaSize ? 0 : newOffset;
//            }
//        );

        tryStampBatch(
//            walShardSequenceId,
//            accountId,
            entryRecordBatchHandler,
            entriesCount,
            entriesOffset,
            stampResult
//            totalAmount
        );
/*
        long walSequenceId,
        UUID accountId,
        EntryRecordBatchHandler entryRecordBatchHandler,
        long entriesOffset,
        int entriesCount,
        long barrierOffset,
        int operationCount,
        long totalAmount

 */
        offsetForward();
        final long entriesBatchSize = stampResult[0];
        return offsetBarrierPassed(entriesBatchSize);

//        return barrier;
//        if (offset + ENTRY_SIZE >= arenaSize) {
//            return false;
//        }
//        offset = offset + ENTRY_SIZE;
    }

    public void offsetForward() {
        final long batchRawSize = batchSize(stampOffset);
        if (stampOffset + batchRawSize >= arenaSize) {
            stampOffset = 0;
        } else {
            stampOffset = stampOffset + batchRawSize;
        }
    }

    public void offsetForward(long entriesCount) {
        final long batchRawSize = batchSize(stampOffset, entriesCount);
        if (stampOffset + batchRawSize >= arenaSize) {
            stampOffset = 0;
        } else {
            stampOffset = stampOffset + batchRawSize;
        }
//        stampOffset.accumulateAndGet(
//            entriesCount * this.batchElementSize,
//            (currentOffset, increment) -> {
//                long newOffset = currentOffset + increment;
//                return newOffset >= arenaSize ? 0 : newOffset;
//            }
//        );
//        final long length = entryRecordCount * this.batchSize() + BATCH_BODY_OFFSET;
//        if (stampOffset + batchRawSize >= arenaSize) {
//            offset = 0;
//        } else {
//            offset = offset + batchRawSize;
//        }
//        return offset;
    }

    public boolean offsetBarrierPassed(long entriesCount) {
//        final long length = entryRecordCount * this.batchSize() + BATCH_BODY_OFFSET;
//        return offset == 0 || (offset + batchRawSize) == (arenaSize >> 1);
        final long offset = (long) STAMP_OFFSET_MEMORY_BARRIER.getAcquire(this);
        return offset == 0 || (offset + (this.batchElementSize * entriesCount) + BATCH_HEADER_SIZE) == (arenaSize >> 1);
    }

//    public long batchSize(long batchOffset) {
//        return entriesCountBatchSize(
//            ringBufferSegment.get(ValueLayout.JAVA_INT, batchOffset + BATCH_ENTRIES_COUNT_OFFSET)
//        );
//    }

//    @Override
//    public long batchElementsCount(long batchOffset) {
//        return ringBufferSegment.get(ValueLayout.JAVA_INT, batchOffset + BATCH_ENTRIES_COUNT_OFFSET);
//    }

    public long passedBarrierOffset() {
        final long arenaHalfSize = arenaSize >> 1;
        final long offset = (long) STAMP_OFFSET_MEMORY_BARRIER.getAcquire(this);
        if (offset >= arenaHalfSize) {
            return arenaHalfSize;
        }
        return 0;
    }

    public int barrierPassedEntriesCount(long dataSize) {
        final long arenaHalfSize = arenaSize >> 1;
        if (stampOffset >= arenaHalfSize) {
            return (int) (dataSize - arenaHalfSize) / EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE;
        }
        return (int) dataSize / EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE;
    }

    public void resetStampOffset() {
        STAMP_OFFSET_MEMORY_BARRIER.setRelease(this, 0);
    }

    public void resetReadOffset() {
        READ_OFFSET_MEMORY_BARRIER.setRelease(this, 0);
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
