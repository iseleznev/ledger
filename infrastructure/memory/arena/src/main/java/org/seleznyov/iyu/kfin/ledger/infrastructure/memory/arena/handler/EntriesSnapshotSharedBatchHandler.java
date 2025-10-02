package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler;

import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.domain.model.snapshot.EntriesSnapshot;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Slf4j
@Data
@Accessors(fluent = true)
public class EntriesSnapshotSharedBatchHandler {

    private static final VarHandle STAMP_STATUS_MEMORY_BARRIER;
//    private static final VarHandle READ_OFFSET_MEMORY_BARRIER;
    private static final VarHandle STAMP_OFFSET_MEMORY_BARRIER;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            // Для статических полей
            STAMP_STATUS_MEMORY_BARRIER = lookup.findVarHandle(
                PostgreSqlEntryRecordRingBufferHandler.class, "stampStatus", int.class);
//            READ_OFFSET_MEMORY_BARRIER = lookup.findVarHandle(
//                PostgreSqlEntryRecordBatchRingBufferHandler.class, "readOffset", long.class);
            STAMP_OFFSET_MEMORY_BARRIER = lookup.findVarHandle(
                PostgreSqlEntryRecordRingBufferHandler.class, "stampOffset", long.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static final long BASE_EPOCH_MILLIS = LocalDateTime.of(2000, 1, 1, 0, 0, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();

    private final static int CPU_CACHE_LINE_SIZE = 64;

// Правильные константы для EntriesSnapshotSharedBatchHandler
// Основаны на модели EntriesSnapshot, а не EntryRecord

// ===== PostgreSQL binary record structure для EntriesSnapshot =====

    private static final int FIELD_COUNT_OFFSET = 0;                    // 2 bytes (short)
    private static final ValueLayout FIELD_COUNT_TYPE = ValueLayout.JAVA_SHORT;

    private static final ValueLayout LENGTH_TYPE = ValueLayout.JAVA_SHORT;

    // Field 1: ID (UUID)
    private static final int ENTRIES_SNAPSHOT_ID_LENGTH_OFFSET = (int) (FIELD_COUNT_OFFSET + FIELD_COUNT_TYPE.byteSize());
    private static final int ENTRIES_SNAPSHOT_MOST_SIGNIFICANT_OFFSET = (int) (ENTRIES_SNAPSHOT_ID_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
    private static final ValueLayout ENTRIES_SNAPSHOT_ID_TYPE = ValueLayout.JAVA_LONG;
    private static final int ENTRIES_SNAPSHOT_ID_LEAST_SIGNIFICANT_OFFSET = (int) (ENTRIES_SNAPSHOT_MOST_SIGNIFICANT_OFFSET + ENTRIES_SNAPSHOT_ID_TYPE.byteSize());

    // Field 2: Account ID (UUID)
    private static final int ACCOUNT_ID_LENGTH_OFFSET = (int) (ENTRIES_SNAPSHOT_ID_LEAST_SIGNIFICANT_OFFSET + ENTRIES_SNAPSHOT_ID_TYPE.byteSize());
    private static final int ACCOUNT_ID_MOST_SIGNIFICANT_OFFSET = (int) (ACCOUNT_ID_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
    private static final ValueLayout ACCOUNT_ID_TYPE = ValueLayout.JAVA_LONG;
    private static final int ACCOUNT_ID_LEAST_SIGNIFICANT_OFFSET = (int) (ACCOUNT_ID_MOST_SIGNIFICANT_OFFSET + ACCOUNT_ID_TYPE.byteSize());

    // Field 3: Operation Date (LocalDate -> long)
    private static final int OPERATION_DATE_LENGTH_OFFSET = (int) (ACCOUNT_ID_LEAST_SIGNIFICANT_OFFSET + ACCOUNT_ID_TYPE.byteSize());
    private static final int OPERATION_DATE_OFFSET = (int) (OPERATION_DATE_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
    private static final ValueLayout OPERATION_DATE_TYPE = ValueLayout.JAVA_LONG;

    // Field 4: Balance (long)
    private static final int BALANCE_LENGTH_OFFSET = (int) (OPERATION_DATE_OFFSET + OPERATION_DATE_TYPE.byteSize());
    private static final int BALANCE_OFFSET = (int) (BALANCE_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
    private static final ValueLayout BALANCE_TYPE = ValueLayout.JAVA_LONG;

    // Field 5: Last Entry Record ID (UUID)
    private static final int LAST_ENTRY_RECORD_ID_LENGTH_OFFSET = (int) (BALANCE_OFFSET + BALANCE_TYPE.byteSize());
    private static final int LAST_ENTRY_RECORD_ID_MOST_SIGNIFICANT_OFFSET = (int) (LAST_ENTRY_RECORD_ID_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
    private static final ValueLayout LAST_ENTRY_RECORD_ID_TYPE = ValueLayout.JAVA_LONG;
    private static final int LAST_ENTRY_RECORD_ID_LEAST_SIGNIFICANT_OFFSET = (int) (LAST_ENTRY_RECORD_ID_MOST_SIGNIFICANT_OFFSET + LAST_ENTRY_RECORD_ID_TYPE.byteSize());

    // Field 6: Last Entry Ordinal (long)
    private static final int LAST_ENTRY_ORDINAL_LENGTH_OFFSET = (int) (LAST_ENTRY_RECORD_ID_LEAST_SIGNIFICANT_OFFSET + LAST_ENTRY_RECORD_ID_TYPE.byteSize());
    private static final int LAST_ENTRY_ORDINAL_OFFSET = (int) (LAST_ENTRY_ORDINAL_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
    private static final ValueLayout LAST_ENTRY_ORDINAL_TYPE = ValueLayout.JAVA_LONG;

    // Field 7: Operations Count (int)
    private static final int OPERATIONS_COUNT_LENGTH_OFFSET = (int) (LAST_ENTRY_ORDINAL_OFFSET + LAST_ENTRY_ORDINAL_TYPE.byteSize());
    private static final int OPERATIONS_COUNT_OFFSET = (int) (OPERATIONS_COUNT_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
    private static final ValueLayout OPERATIONS_COUNT_TYPE = ValueLayout.JAVA_INT;

    // Field 8: Reason (EntriesSnapshotReasonType -> short)
    private static final int REASON_LENGTH_OFFSET = (int) (OPERATIONS_COUNT_OFFSET + OPERATIONS_COUNT_TYPE.byteSize());
    private static final int REASON_OFFSET = (int) (REASON_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
    private static final ValueLayout REASON_TYPE = ValueLayout.JAVA_SHORT;

    // Field 9: Created At (Instant -> long)
    private static final int CREATED_AT_LENGTH_OFFSET = (int) (REASON_OFFSET + REASON_TYPE.byteSize());
    private static final int CREATED_AT_OFFSET = (int) (CREATED_AT_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
    private static final ValueLayout CREATED_AT_TYPE = ValueLayout.JAVA_LONG;

    // Field 10: Duration Millis (long)
    private static final int DURATION_MILLIS_LENGTH_OFFSET = (int) (CREATED_AT_OFFSET + CREATED_AT_TYPE.byteSize());
    private static final int DURATION_MILLIS_OFFSET = (int) (DURATION_MILLIS_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
    private static final ValueLayout DURATION_MILLIS_TYPE = ValueLayout.JAVA_LONG;

    private static final int ORDINAL_LENGTH_OFFSET = (int) (DURATION_MILLIS_OFFSET + DURATION_MILLIS_TYPE.byteSize());
    private static final int ORDINAL_OFFSET = (int) (ORDINAL_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
    private static final ValueLayout ORDINAL_TYPE = ValueLayout.JAVA_LONG;

    private static final int WAL_SEQUENCE_ID_LENGTH_OFFSET = (int) (ORDINAL_OFFSET + ORDINAL_TYPE.byteSize());
    public static final int WAL_SEQUENCE_ID_OFFSET = (int) (WAL_SEQUENCE_ID_LENGTH_OFFSET + LENGTH_TYPE.byteSize());
    private static final ValueLayout WAL_SEQUENCE_ID_TYPE = ValueLayout.JAVA_LONG;

    private static final int POSTGRES_ENTRIES_SNAPSHOT_RAW_SIZE = (int) (WAL_SEQUENCE_ID_OFFSET + WAL_SEQUENCE_ID_TYPE.byteSize());

    public static final int POSTGRES_ENTRIES_SNAPSHOT_SIZE = (POSTGRES_ENTRIES_SNAPSHOT_RAW_SIZE + CPU_CACHE_LINE_SIZE - 1) & -CPU_CACHE_LINE_SIZE;

    private final MemorySegment memorySegment;

    private long stampOffset = 0;
    private final long arenaSize;

    // Эти поля не требуют atomic операций, так как они используются
    // только для локальной статистики и не влияют на корректность работы с памятью
    private volatile long totalAmount = 0;
    private volatile long operationsCount = 0;
    private volatile long lastAccessTime;
    private long startEpochMillis = 0;
    private long minutesSinceEpoch = 0;
    private long snapshotOrdinal = 0;

    public EntriesSnapshotSharedBatchHandler(
        MemorySegment memorySegment,
        long totalAmount,
        long lastAccessTime
    ) {
        this.memorySegment = memorySegment;
        this.totalAmount = totalAmount;
        this.arenaSize = memorySegment.byteSize();
        this.lastAccessTime = lastAccessTime;
        resetOrdinal();
        log.debug("Created PostgresBinaryEntryRecordLayout: segment_size={}KB, record_size={} bytes",
            arenaSize / 1024, POSTGRES_ENTRIES_SNAPSHOT_SIZE);
//        this.memoryLayout = MemoryLayout.structLayout(
//            ValueLayout.JAVA_LONG.withName("balance"),
//            ValueLayout.JAVA_SHORT.withName("entryType"),
//            ValueLayout.JAVA_LONG.withName("createdAt"),
//            MemoryLayout.sequenceLayout(16, ValueLayout.JAVA_BYTE).withName("accountId"),
//            MemoryLayout.sequenceLayout(16, ValueLayout.JAVA_BYTE).withName("transactionId"),
//            MemoryLayout.sequenceLayout(16, ValueLayout.JAVA_BYTE).withName("idempotencyKey")
//        );
    }

    public long snapshotOrdinal() {
        return snapshotOrdinal;
    }

    public void snapshotOrdinal(long snapshotOrdinal) {
        this.snapshotOrdinal = snapshotOrdinal;
    }

    public long ordinal() {
        return memorySegment.get(ValueLayout.JAVA_LONG, ORDINAL_OFFSET);
    }

    public void ordinal(long ordinal) {
        memorySegment.set(ValueLayout.JAVA_LONG, ORDINAL_OFFSET, ordinal);
    }

    /**
     * ОПТИМИЗАЦИЯ 1: Атомарная запись с резервированием места
     * Избегает race conditions и множественных вызовов get()
     * Метод возвращает следующее смещение, при этом, если
     * смещение достигло половины зарезервированной памяти
     * (достигло барьера в середине), то значение вернется
     * отрицательным
     */
    public long stampSnapshotForwardPassBarrier(EntriesSnapshot snapshot) {
        // Атомарно резервируем место и получаем текущий offset для записи
        final long beforeForwardOffset = stampOffset.getAndAccumulate(
            POSTGRES_ENTRIES_SNAPSHOT_SIZE,
            (currentOffset, increment) -> {
                long newOffset = currentOffset + increment;
                return newOffset >= arenaSize ? 0 : newOffset;
            }
        );

        // Записываем в зарезервированное место без дополнительных atomic операций
        stampEntriesSnapshot(snapshot, beforeForwardOffset);

        // Проверяем барьер на основе нового offset
        long nextOffset = beforeForwardOffset + POSTGRES_ENTRIES_SNAPSHOT_SIZE;
        if (nextOffset >= arenaSize) {
            nextOffset = 0;
        }

        if (nextOffset == (arenaSize >> 1)) {
            return -nextOffset;
        }
        return nextOffset;
    }

    /**
     * ОПТИМИЗАЦИЯ 3: Запись без повторных вызовов sharedOffset.get()
     */
    private void stampEntriesSnapshot(EntriesSnapshot snapshot, long offset) {
        // Все операции используют переданный offset
        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset + ENTRIES_SNAPSHOT_MOST_SIGNIFICANT_OFFSET,
            snapshot.id().getMostSignificantBits());
        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset + ENTRIES_SNAPSHOT_ID_LEAST_SIGNIFICANT_OFFSET,
            snapshot.id().getLeastSignificantBits());

        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset + ACCOUNT_ID_MOST_SIGNIFICANT_OFFSET,
            snapshot.accountId().getMostSignificantBits());
        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset + ACCOUNT_ID_LEAST_SIGNIFICANT_OFFSET,
            snapshot.accountId().getLeastSignificantBits());

        memorySegment.set(ValueLayout.JAVA_LONG, offset + OPERATION_DATE_OFFSET,
            snapshot.operationDay().atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli());

        memorySegment.set(ValueLayout.JAVA_LONG, offset + BALANCE_OFFSET, snapshot.balance());

        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset + LAST_ENTRY_RECORD_ID_MOST_SIGNIFICANT_OFFSET,
            snapshot.lastEntryRecordId().getMostSignificantBits());
        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset + LAST_ENTRY_RECORD_ID_LEAST_SIGNIFICANT_OFFSET,
            snapshot.lastEntryRecordId().getLeastSignificantBits());

        memorySegment.set(ValueLayout.JAVA_LONG, offset + LAST_ENTRY_ORDINAL_OFFSET,
            snapshot.lastEntryOrdinal());

        memorySegment.set(ValueLayout.JAVA_INT, offset + OPERATIONS_COUNT_OFFSET,
            snapshot.operationsCount());

        memorySegment.set(ValueLayout.JAVA_SHORT, offset + REASON_OFFSET,
            (short) snapshot.reason().ordinal());

        memorySegment.set(ValueLayout.JAVA_LONG, offset + CREATED_AT_OFFSET,
            snapshot.createdAt().toEpochMilli());

        memorySegment.set(ValueLayout.JAVA_LONG, offset + DURATION_MILLIS_OFFSET,
            snapshot.durationMillis());

        memorySegment.set(ValueLayout.JAVA_LONG, offset + ORDINAL_OFFSET, snapshotOrdinal);

        snapshotOrdinal++;
        if (snapshotOrdinal == Long.MAX_VALUE) {
            resetOrdinal();
        }
    }

    /**
     * ОПТИМИЗАЦИЯ 5: Методы для получения текущего состояния без лишних atomic операций
     */
    public long getCurrentOffset() {
        return stampOffset.get();
    }

    /**
     * ОПТИМИЗАЦИЯ 6: Упрощенные методы без множественных get() вызовов
     */
    public void offsetForward() {
        stampOffset.accumulateAndGet(
            POSTGRES_ENTRIES_SNAPSHOT_SIZE,
            (offset, offsetShift) ->
                offset + offsetShift < arenaSize
                    ? offset + offsetShift
                    : 0
        );
    }

    public boolean offsetBarrierPassed() {
        final long offset = stampOffset.get();
        return offset == 0 || (offset + POSTGRES_ENTRIES_SNAPSHOT_SIZE) == (arenaSize >> 1);
    }

    public int barrierPassedSnapshotsCount() {
        final long offset = stampOffset.get();
        final long arenaHalfSize = arenaSize >> 1;
        if (offset == arenaHalfSize) {
            return (int) (arenaSize - arenaHalfSize) / POSTGRES_ENTRIES_SNAPSHOT_SIZE;
        }
        return (int) arenaHalfSize / POSTGRES_ENTRIES_SNAPSHOT_SIZE;
    }

    // Методы для статистики остаются с volatile полями
    public long totalAmount() {
        return totalAmount;
    }

    public long operationsCount() {
        return operationsCount;
    }

    public void resetOperationsCount() {
        operationsCount = 0;
    }

    private void resetOrdinal() {
        this.startEpochMillis = LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli() - BASE_EPOCH_MILLIS;
        this.minutesSinceEpoch = this.startEpochMillis / 60_000;
        this.snapshotOrdinal = this.minutesSinceEpoch << 32; //& 0xFFFFFFFFFFF00000L;//| (sequence.get() & 0xFFFFF)
    }
}