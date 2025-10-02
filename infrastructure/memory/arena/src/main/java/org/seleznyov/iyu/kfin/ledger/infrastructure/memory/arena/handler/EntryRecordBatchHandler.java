package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler;

import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.domain.model.entryrecord.EntryRecord;
import org.seleznyov.iyu.kfin.ledger.domain.model.entryrecord.EntryType;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.AccountsPartitionHashTable;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration.LedgerConfiguration;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.locks.LockSupport;

import static org.seleznyov.iyu.kfin.ledger.domain.model.entryrecord.EntryType.DEBIT;

@Slf4j
@Data
@Accessors(fluent = true)
public class EntryRecordBatchHandler {

    private static final long BASE_EPOCH_MILLIS = LocalDateTime.of(2000, 1, 1, 0, 0, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();

    private static final VarHandle STAMP_STATUS_MEMORY_BARRIER;
    private static final VarHandle READ_OFFSET_MEMORY_BARRIER;
    private static final VarHandle STAMP_OFFSET_MEMORY_BARRIER;
    private static final VarHandle PASSED_BARRIER_BATCH_AMOUNT_MEMORY_BARRIER;
    private static final VarHandle PASSED_BARRIER_BATCH_AMOUNT_ACCUMULATOR_MEMORY_BARRIER;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            // Для статических полей
            STAMP_STATUS_MEMORY_BARRIER = lookup.findVarHandle(
                EntryRecordBatchHandler.class, "stampStatus", int.class);
            READ_OFFSET_MEMORY_BARRIER = lookup.findVarHandle(
                EntryRecordBatchHandler.class, "readOffset", long.class);
            STAMP_OFFSET_MEMORY_BARRIER = lookup.findVarHandle(
                EntryRecordBatchHandler.class, "stampOffset", long.class);
            PASSED_BARRIER_BATCH_AMOUNT_MEMORY_BARRIER = lookup.findVarHandle(
                EntryRecordBatchHandler.class, "passedBarrierBatchAmount", long.class);
            PASSED_BARRIER_BATCH_AMOUNT_ACCUMULATOR_MEMORY_BARRIER = lookup.findVarHandle(
                EntryRecordBatchHandler.class, "passedBarrierBatchAmountAccumulator", long.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final static int CPU_CACHE_LINE_SIZE = 64;

    // ===== PostgreSQL binary record structure =====

    private static final int FIELD_COUNT_OFFSET = 0;
    private static final ValueLayout FIELD_COUNT_TYPE = ValueLayout.JAVA_SHORT;

    // Field 1: ID (UUID)
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
//    private final UUID entriesAccountId;

    private final AccountsPartitionHashTable accountsStateTable;
//    private long walShardSequenceId;
    private int stampStatus;
    private long stampOffset;
    private long readOffset;
    private long arenaSize;
    private long passedBarrierBatchAmount = 0;
    private long passedBarrierBatchAmountAccumulator = 0;
    private long operationsCount = 0;
    private long startEpochMillis = 0;
    private long minutesSinceEpoch = 0;
    private long actualAccountsCount = 0;
    private volatile long lastAccessTime;
//    private final MemoryLayout memoryLayout;

    public EntryRecordBatchHandler(
        MemorySegment memorySegment,
        long walShardSequenceId,
        LedgerConfiguration ledgerConfiguration
    ) {
//        this.walShardSequenceId = walShardSequenceId;
        this.memorySegment = memorySegment;
        final Arena balancesArena = Arena.ofConfined();
        this.accountsStateTable = new AccountsPartitionHashTable(balancesArena, ledgerConfiguration.hotAccounts().maxCount(), 16);
        this.offset = 0;
        this.arenaSize = memorySegment.byteSize();
        this.lastAccessTime = lastAccessTime;
        log.debug("Created PostgresBinaryEntryRecordLayout: segment_size={}KB, record_size={} bytes",
            arenaSize / 1024, POSTGRES_ENTRY_RECORD_SIZE);
//        this.memoryLayout = MemoryLayout.structLayout(
//            ValueLayout.JAVA_LONG.withName("balance"),
//            ValueLayout.JAVA_SHORT.withName("entryType"),
//            ValueLayout.JAVA_LONG.withName("createdAt"),
//            MemoryLayout.sequenceLayout(16, ValueLayout.JAVA_BYTE).withName("accountId"),
//            MemoryLayout.sequenceLayout(16, ValueLayout.JAVA_BYTE).withName("transactionId"),
//            MemoryLayout.sequenceLayout(16, ValueLayout.JAVA_BYTE).withName("idempotencyKey")
//        );
    }

    public long totalAmount(UUID accountId) {
        return accountsStateTable.getBalance(accountId);
    }

    public void resetTotalAmount(UUID accountId) {
        accountsStateTable.putBalance(accountId, 0);
    }

    public long entriesCount(UUID accountId) {
        return accountsStateTable.getCount(accountId);
    }

    public void resetEntriesCount(UUID accountId) {
        accountsStateTable.putCount(accountId, 0);
    }

    public long entryOrdinal(UUID accountId) {
        return accountsStateTable.getOrdinal(accountId);
    }

    public long operationsCount() {
        return operationsCount;
    }

    public void resetOperationsCount() {
        operationsCount = 0;
    }

    public UUID accountId() {
        final long mostSignificant = memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + ACCOUNT_ID_MOST_SIGNIFICANT_OFFSET);
        final long leastSignificant = memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + ACCOUNT_ID_LEAST_SIGNIFICANT_OFFSET);
        return new UUID(leastSignificant, mostSignificant);
    }

    public void accountId(UUID accountId) {
        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset + ACCOUNT_ID_MOST_SIGNIFICANT_OFFSET, accountId.getMostSignificantBits());
        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset + ACCOUNT_ID_LEAST_SIGNIFICANT_OFFSET, accountId.getLeastSignificantBits());
    }

    public EntryType entryType() {
        return EntryType.ofShort(
            memorySegment.get(ValueLayout.JAVA_SHORT, offset + ENTRY_TYPE_OFFSET)
        );
    }

    public void entryType(EntryType entryType) {
        memorySegment.set(
            ValueLayout.JAVA_SHORT,
            offset + ENTRY_TYPE_OFFSET,
            entryType.shortFromEntryType()
        );
    }

    public void registerAccount(UUID accountId, long balance) {
        actualAccountsCount++;
        final long offset = actualAccountsCount << 3;
        accountsStateTable.putBalance(accountId, balance);
    }

    public void accountBalance(UUID accountId, long balance) {
        accountsStateTable.putBalance(accountId, balance);
    }

    public long accountBalance(UUID accountId) {
        return accountsStateTable.getBalance(accountId);
    }

    public long amount() {
        return memorySegment.get(ValueLayout.JAVA_LONG, offset + AMOUNT_OFFSET);
    }

    public void amount(long balance) {
        memorySegment.set(ValueLayout.JAVA_LONG, offset + AMOUNT_OFFSET, balance);
    }

    public LocalDateTime createdAt() {
        return LocalDateTime.ofInstant(
            Instant.ofEpochMilli(
                memorySegment.get(ValueLayout.JAVA_LONG, offset + CREATED_AT_OFFSET)
            ),
            ZoneId.from(ZoneOffset.UTC)
        );
    }

    public Instant instantCreatedAt() {
        return Instant.ofEpochMilli(
            memorySegment.get(ValueLayout.JAVA_LONG, offset + CREATED_AT_OFFSET)
        );
    }

    public void createdAt(LocalDateTime createdAt) {
        memorySegment.set(
            ValueLayout.JAVA_LONG,
            offset + CREATED_AT_OFFSET,
            createdAt.toInstant(ZoneOffset.UTC).toEpochMilli()
        );
    }

    public void createdAt(Instant createdAt) {
        memorySegment.set(
            ValueLayout.JAVA_LONG,
            offset + CREATED_AT_OFFSET,
            createdAt.toEpochMilli()
        );
    }

    public LocalDate operationDay() {
        return LocalDate.ofInstant(
            Instant.ofEpochMilli(
                memorySegment.get(ValueLayout.JAVA_LONG, offset + OPERATION_DAY_OFFSET)
            ).truncatedTo(ChronoUnit.DAYS),
            ZoneId.from(ZoneOffset.UTC)
        );
    }

    public Instant instantOperationDay() {
        return Instant.ofEpochMilli(
            memorySegment.get(ValueLayout.JAVA_LONG, offset + OPERATION_DAY_OFFSET)
        );
    }

    public void operationDay(LocalDate operationDay) {
        memorySegment.set(
            ValueLayout.JAVA_LONG,
            offset + OPERATION_DAY_OFFSET,
            operationDay.atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli()
        );
    }

    public void operationDay(Instant operationDay) {
        memorySegment.set(
            ValueLayout.JAVA_LONG,
            offset + OPERATION_DAY_OFFSET,
            operationDay.toEpochMilli()
        );
    }

    public UUID entryRecordId() {
        final long mostSignificant = memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + ENTRY_RECORD_ID_MSB_OFFSET);
        final long leastSignificant = memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + ENTRY_RECORD_ID_LSB_OFFSET);
        return new UUID(leastSignificant, mostSignificant);
    }

    public void entryRecordId(UUID entryRecordId) {
        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset + ENTRY_RECORD_ID_MSB_OFFSET, entryRecordId.getMostSignificantBits());
        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset + ENTRY_RECORD_ID_LSB_OFFSET, entryRecordId.getLeastSignificantBits());
    }

    public UUID transactionId() {
        final long mostSignificant = memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + TRANSACTION_ID_MOST_SIGNIFICANT_OFFSET);
        final long leastSignificant = memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + TRANSACTION_ID_LEAST_SIGNIFICANT_OFFSET);
        return new UUID(leastSignificant, mostSignificant);
    }

    public void transactionId(UUID transactionId) {
        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset + TRANSACTION_ID_MOST_SIGNIFICANT_OFFSET, transactionId.getMostSignificantBits());
        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset + TRANSACTION_ID_LEAST_SIGNIFICANT_OFFSET, transactionId.getLeastSignificantBits());
    }

    public UUID idempotencyKey() {
        final long mostSignificant = memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + IDEMPOTENCY_KEY_MOST_SIGNIFICANT_OFFSET);
        final long leastSignificant = memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + IDEMPOTENCY_KEY_LEAST_SIGNIFICANT_OFFSET);
        return new UUID(leastSignificant, mostSignificant);
    }

    public void idempotencyKey(UUID idempotencyKey) {
        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset + IDEMPOTENCY_KEY_MOST_SIGNIFICANT_OFFSET, idempotencyKey.getMostSignificantBits());
        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset + IDEMPOTENCY_KEY_LEAST_SIGNIFICANT_OFFSET, idempotencyKey.getLeastSignificantBits());
    }

    public String currencyCode() {
        final byte[] bytes = new byte[8];
        MemorySegment.copy(
            memorySegment,
            offset + CURRENCY_CODE_OFFSET,
            MemorySegment.ofArray(bytes),
            0,
            CURRENCY_CODE_TYPE.byteSize()
        );
        return new String(bytes, StandardCharsets.UTF_8);
//        final long currencyCode = memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + CURRENCY_CODE_OFFSET);
//        return Arrays.toString(ByteBuffer.allocate(Long.BYTES).putLong(currencyCode).array());
    }

    public void currencyCode(String currencyCode) {
//        final long currencyCodeLong = ByteBuffer.wrap(currencyCode.getBytes()).getLong();
//        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset + CURRENCY_CODE_OFFSET, currencyCodeLong);
        MemorySegment.copy(
            MemorySegment.ofArray(currencyCode.getBytes(StandardCharsets.UTF_8)),
            0,
            memorySegment,
            offset + CURRENCY_CODE_OFFSET,
            currencyCode.length()
        );
    }

    public long ordinal() {
        return memorySegment.get(ValueLayout.JAVA_LONG, offset + ORDINAL_OFFSET);
    }

    public void ordinal(long ordinal) {
        memorySegment.set(ValueLayout.JAVA_LONG, offset + ORDINAL_OFFSET, ordinal);
    }

    public long passedBarrierOffset() {
        final long arenaHalfSize = arenaSize >> 1;
        if (offset >= arenaHalfSize) {
            return arenaHalfSize;
        }
        return 0;
    }

    public int barrierPassedEntriesCount() {
        final long arenaHalfSize = arenaSize >> 1;
        if (offset >= arenaHalfSize) {
            return (int) (offset - arenaHalfSize) / POSTGRES_ENTRY_RECORD_SIZE;
        }
        return (int) (offset) / POSTGRES_ENTRY_RECORD_SIZE;
    }

    public boolean stampEntryRecordForwardPassBarrier(EntryRecord entryRecord) {
        int maxAttempts = 100;
        int spinAttempts = 20;
        int yieldAttempts = 50;
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            long currentReadOffset = (long) READ_OFFSET_MEMORY_BARRIER.getAcquire(this) % this.arenaSize;
            long currentStampOffset = (long) STAMP_OFFSET_MEMORY_BARRIER.getAndAdd(this, POSTGRES_ENTRY_RECORD_SIZE) % this.arenaSize;

            if (currentStampOffset > currentReadOffset - POSTGRES_ENTRY_RECORD_SIZE) {
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

            STAMP_STATUS_MEMORY_BARRIER.set(this, MemoryBarrierOperationStatus.HANDLING.memoryOrdinal());

//            operationsCount++;
            long hashTableOffset = accountsStateTable.getOffset(entryRecord.accountId());
            if (hashTableOffset == -1) {
                //TODO вставить новое значение
                hashTableOffset = accountsStateTable.put(
                    entryRecord.accountId(),
                    0,//может быть, стоит загрузить из базы?
                    0,
                    0
                );
            }
            long entryOrdinal = accountsStateTable.getOrdinal(hashTableOffset) + 1;
            long entryBalance = accountsStateTable.getBalance(hashTableOffset);
            if (DEBIT.equals(entryRecord.entryType()) && entryBalance < entryRecord.amount()) {
                throw new IllegalStateException("Insufficient balance for debit: " + entryRecord + " (balance=" + entryBalance + ")");
            }

            VarHandle.storeStoreFence();

            stampEntryRecord(entryRecord);

            if (entryOrdinal == Long.MAX_VALUE) {
                resetOrdinal(entryRecord.accountId());
                entryOrdinal = 0;
            }
            accountsStateTable.update(
                hashTableOffset,
                accountsStateTable.getBalance(hashTableOffset) + entryRecord.amount(),
                entryOrdinal,
                accountsStateTable.getCount(hashTableOffset) + 1
            );
            final boolean barrierPassed = offsetBarrierPassed();
            if (barrierPassed) {
                resetPassedBarrierBatchAmountAccumulator();
            }

            passedBarrierBatchAmountAccumulator += entryRecord.amount();

            VarHandle.releaseFence();

            STAMP_STATUS_MEMORY_BARRIER.setRelease(this, MemoryBarrierOperationStatus.COMPLETED.memoryOrdinal());
//            offsetForward();
//        accountsStateMap.putBalance(
//            entryRecord.accountId(),
//            accountsStateMap.getBalance(entryRecord.accountId()) + entryRecord.amount()
//        );
//        accountsStateMap.putCount(
//            entryRecord.accountId(),
//            accountsStateMap.getCount(entryRecord.accountId()) + 1
//        );
            return barrierPassed;
        }
//        return barrier;
//        if (offset + ENTRY_SIZE >= arenaSize) {
//            return false;
//        }
//        offset = offset + ENTRY_SIZE;
        throw new IllegalStateException("Failed to stamp entry record forward pass barrier");
    }

    public void offsetForward() {
        if (offset + POSTGRES_ENTRY_RECORD_SIZE >= arenaSize) {
            offset = 0;
//            log.debug("PostgresBinaryEntryRecordLayout offset wrapped around to 0");
        } else {
            offset = offset + POSTGRES_ENTRY_RECORD_SIZE;
        }
    }

    public boolean offsetBarrierPassed() {
        return offset == 0 || (offset + POSTGRES_ENTRY_RECORD_SIZE) == (arenaSize >> 1);
//        if (offset + ENTRY_SIZE >= arenaSize) {
//            return true;
//        }
//        return (offset + ENTRY_SIZE) == (arenaSize >> 1);
    }

    public void resetOffset() {
        offset = 0;
    }

    private void stampEntryRecord(EntryRecord entryRecord) {
        memorySegment.set(ValueLayout.JAVA_LONG, offset + AMOUNT_OFFSET, entryRecord.amount());
        memorySegment.set(
            ValueLayout.JAVA_SHORT,
            offset + ENTRY_TYPE_OFFSET,
            entryRecord.entryType().shortFromEntryType()
        );

        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset + ENTRY_RECORD_ID_MSB_OFFSET, entryRecord.id().getMostSignificantBits());
        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset + ENTRY_RECORD_ID_LSB_OFFSET, entryRecord.id().getLeastSignificantBits());

        memorySegment.set(
            ValueLayout.JAVA_LONG,
            offset + CREATED_AT_OFFSET,
            entryRecord.createdAt().toInstant(ZoneOffset.UTC).toEpochMilli()
        );

        memorySegment.set(
            ValueLayout.JAVA_LONG,
            offset + OPERATION_DAY_OFFSET,
            entryRecord.operationDay().atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli()
        );

        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset + ACCOUNT_ID_MOST_SIGNIFICANT_OFFSET, entryRecord.accountId().getMostSignificantBits());
        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset + ACCOUNT_ID_LEAST_SIGNIFICANT_OFFSET, entryRecord.accountId().getLeastSignificantBits());

        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset + TRANSACTION_ID_MOST_SIGNIFICANT_OFFSET, entryRecord.transactionId().getMostSignificantBits());
        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset + TRANSACTION_ID_LEAST_SIGNIFICANT_OFFSET, entryRecord.transactionId().getLeastSignificantBits());

        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset + IDEMPOTENCY_KEY_MOST_SIGNIFICANT_OFFSET, entryRecord.idempotencyKey().getMostSignificantBits());
        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset + IDEMPOTENCY_KEY_LEAST_SIGNIFICANT_OFFSET, entryRecord.idempotencyKey().getLeastSignificantBits());

        MemorySegment.copy(
            MemorySegment.ofArray(entryRecord.currencyCode().getBytes(StandardCharsets.UTF_8)),
            0,
            memorySegment,
            offset + CURRENCY_CODE_OFFSET,
            entryRecord.currencyCode().length()
        );

        final long entryOrdinal = accountsStateTable().getOrdinal(entryRecord.accountId());

        memorySegment.set(ValueLayout.JAVA_LONG, offset + ORDINAL_OFFSET, entryOrdinal);

//        memorySegment.set(ValueLayout.JAVA_LONG, offset + WAL_SEQUENCE_ID_OFFSET, walShardSequenceId);

//        amount(entryRecord.amount());
//        entryType(entryRecord.entryType());
//        entryRecordId(entryRecord.transactionId());
//        createdAt(entryRecord.createdAt());
//        operationDay(entryRecord.createdAt().toLocalDate());
//        accountId(entryRecord.accountId());
//        transactionId(entryRecord.transactionId());
//        idempotencyKey(entryRecord.idempotencyKey());
//        currencyCode("USD");
//        ordinal(entryOrdinal);
    }

    private void resetPassedBarrierBatchAmountAccumulator() {
        passedBarrierBatchAmount = passedBarrierBatchAmountAccumulator;
        passedBarrierBatchAmountAccumulator = 0;
    }

    private void resetOrdinal(UUID accountId) {
        this.startEpochMillis = LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli() - BASE_EPOCH_MILLIS;
        this.minutesSinceEpoch = this.startEpochMillis / 60_000;
        final long ordinal = this.minutesSinceEpoch << 32;
        accountsStateTable.putOrdinal(accountId, ordinal);
        //this.entryOrdinal = this.minutesSinceEpoch << 32; //& 0xFFFFFFFFFFF00000L;//| (sequence.get() & 0xFFFFF)
    }
}
