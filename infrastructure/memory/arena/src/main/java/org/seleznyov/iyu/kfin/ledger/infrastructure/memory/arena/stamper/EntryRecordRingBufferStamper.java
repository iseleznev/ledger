package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.stamper;

import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.EntryRecordOffsetConstants;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.ringbuffer.SWSRRingBufferHandler;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import static org.seleznyov.iyu.kfin.ledger.domain.model.entryrecord.EntryType.CREDIT;
import static org.seleznyov.iyu.kfin.ledger.domain.model.entryrecord.EntryType.DEBIT;
import static org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.EntryRecordOffsetConstants.*;
import static org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.TransferRequestOffsetConstants.*;
import static org.seleznyov.iyu.kfin.ledger.shared.utils.Uuid7Utils.nextUuid7;

public class EntryRecordRingBufferStamper implements RingBufferStamper {

    private final MemorySegment transferRingBufferMemorySegment;
    private long transferOffset;
    private long debitAccountEntryRecordOrdinal;
    private long creditAccountEntryRecordOrdinal;


    public EntryRecordRingBufferStamper(SWSRRingBufferHandler transactionRingBuffer) {
        this.transferRingBufferMemorySegment = transactionRingBuffer.memorySegment();
    }

    public void transactionOffset(long offset) {
        this.transferOffset = offset;
    }

    @Override
    public void stamp(MemorySegment memorySegment, long stampOffset) {
        stampEntryRecord(
            memorySegment,
            stampOffset,
            transferRingBufferMemorySegment.get(ValueLayout.JAVA_LONG, transferOffset + TRANSFER_DEBIT_ACCOUNT_ID_MSB_OFFSET),
            transferRingBufferMemorySegment.get(ValueLayout.JAVA_LONG, transferOffset + TRANSFER_DEBIT_ACCOUNT_ID_LSB_OFFSET),
            DEBIT.shortFromEntryType(),
            debitAccountEntryRecordOrdinal
        );
        stampEntryRecord(
            memorySegment,
            stampOffset + POSTGRES_ENTRY_RECORD_SIZE,
            transferRingBufferMemorySegment.get(ValueLayout.JAVA_LONG, transferOffset + TRANSFER_CREDIT_ACCOUNT_ID_MSB_OFFSET),
            transferRingBufferMemorySegment.get(ValueLayout.JAVA_LONG, transferOffset + TRANSFER_CREDIT_ACCOUNT_ID_LSB_OFFSET),
            CREDIT.shortFromEntryType(),
            creditAccountEntryRecordOrdinal
        );
    }

    @Override
    public long stampRecordSize() {
        return TRANSFER_STAMP_SIZE;
    }

    public void creditAccountEntryRecordOrdinal(long creditAccountEntryRecordOrdinal) {
        this.creditAccountEntryRecordOrdinal = creditAccountEntryRecordOrdinal;
    }

    public void debitAccountEntryRecordOrdinal(long debitAccountEntryRecordOrdinal) {
        this.debitAccountEntryRecordOrdinal = debitAccountEntryRecordOrdinal;
    }

    private void stampEntryRecord(MemorySegment memorySegment, long stampOffset, long accountIdMsb, long accountIdLsb, short entryType, long ordinal) {
        memorySegment.set(LENGTH_TYPE, stampOffset + AMOUNT_LENGTH_OFFSET, (short) AMOUNT_TYPE.byteSize());
        memorySegment.set(
            AMOUNT_TYPE, stampOffset + AMOUNT_OFFSET,
            transferRingBufferMemorySegment.get(TRANSFER_AMOUNT_TYPE, transferOffset + TRANSFER_AMOUNT_OFFSET)
        );
        memorySegment.set(LENGTH_TYPE, stampOffset + ENTRY_TYPE_LENGTH_OFFSET, (short) ENTRY_TYPE_TYPE.byteSize());
        memorySegment.set(
            ENTRY_TYPE_TYPE,
            stampOffset + ENTRY_TYPE_OFFSET,
            entryType
        );

        final UUID entryRecordId = nextUuid7();
        final long entryRecordIdMsb = entryRecordId.getMostSignificantBits();
        final long entryRecordIdLsb = entryRecordId.getLeastSignificantBits();

        memorySegment.set(LENGTH_TYPE, stampOffset + ENTRY_RECORD_ID_LENGTH_OFFSET, (short) (ENTRY_RECORD_ID_SB_TYPE.byteSize() << 1));
        memorySegment.set(
            ENTRY_RECORD_ID_SB_TYPE,
            stampOffset + ENTRY_RECORD_ID_MSB_OFFSET,
            entryRecordIdMsb
        );
        memorySegment.set(
            ENTRY_RECORD_ID_SB_TYPE,
            stampOffset + ENTRY_RECORD_ID_LSB_OFFSET,
            entryRecordIdLsb
        );

        memorySegment.set(LENGTH_TYPE, stampOffset + CREATED_AT_LENGTH_OFFSET, (short) CREATED_AT_TYPE.byteSize());
        memorySegment.set(
            CREATED_AT_TYPE,
            stampOffset + CREATED_AT_OFFSET,
            transferRingBufferMemorySegment.get(TRANSFER_CREATED_AT_TYPE, transferOffset + TRANSFER_CREATED_AT_OFFSET)
        );

        final long operationDay = LocalDateTime.now().toInstant(ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS).toEpochMilli();

        memorySegment.set(LENGTH_TYPE, stampOffset + OPERATION_DAY_LENGTH_OFFSET, (short) OPERATION_DAY_TYPE.byteSize());
        memorySegment.set(
            OPERATION_DAY_TYPE,
            stampOffset + OPERATION_DAY_OFFSET,
            operationDay
        );

        memorySegment.set(LENGTH_TYPE, stampOffset + ACCOUNT_ID_LENGTH_OFFSET, (short) (ACCOUNT_ID_TYPE.byteSize() << 1));
        memorySegment.set(
            ACCOUNT_ID_TYPE,
            stampOffset + ACCOUNT_ID_MSB_OFFSET,
            accountIdMsb
        );
        memorySegment.set(
            ACCOUNT_ID_TYPE,
            stampOffset + ACCOUNT_ID_LSB_OFFSET,
            accountIdLsb
        );

        memorySegment.set(LENGTH_TYPE, stampOffset + ENTRY_RECORD_TRANSFER_ID_LENGTH_OFFSET, (short) (ENTRY_RECORD_TRANSFER_ID_TYPE.byteSize() << 1));
        memorySegment.set(
            ENTRY_RECORD_TRANSFER_ID_TYPE,
            stampOffset + EntryRecordOffsetConstants.ENTRY_RECORD_TRANSFER_ID_MSB_OFFSET,
            transferRingBufferMemorySegment.get(TRANSFER_ID_TYPE, transferOffset + TRANSFER_ID_MSB_OFFSET)
        );
        memorySegment.set(
            ENTRY_RECORD_TRANSFER_ID_TYPE,
            stampOffset + EntryRecordOffsetConstants.ENTRY_RECORD_TRANSFER_ID_LSB_OFFSET,
            transferRingBufferMemorySegment.get(TRANSFER_ID_TYPE, transferOffset + ENTRY_RECORD_TRANSFER_ID_LSB_OFFSET)
        );

        memorySegment.set(LENGTH_TYPE, stampOffset + IDEMPOTENCY_KEY_LENGTH_OFFSET, (short) (IDEMPOTENCY_KEY_TYPE.byteSize() << 1));
        memorySegment.set(
            IDEMPOTENCY_KEY_TYPE,
            stampOffset + IDEMPOTENCY_KEY_MSB_OFFSET,
            transferRingBufferMemorySegment.get(TRANSFER_IDEMPOTENCY_KEY_TYPE, transferOffset + TRANSFER_IDEMPOTENCY_KEY_MSB_OFFSET)
        );
        memorySegment.set(
            IDEMPOTENCY_KEY_TYPE,
            stampOffset + IDEMPOTENCY_KEY_LSB_OFFSET,
            transferRingBufferMemorySegment.get(TRANSFER_IDEMPOTENCY_KEY_TYPE, transferOffset + TRANSFER_IDEMPOTENCY_KEY_LSB_OFFSET)
        );

        memorySegment.set(LENGTH_TYPE, stampOffset + ORDINAL_LENGTH_OFFSET, (short) CURRENCY_CODE_TYPE.byteSize());
        memorySegment.set(CURRENCY_CODE_TYPE, stampOffset + ORDINAL_OFFSET, ordinal);

        memorySegment.set(LENGTH_TYPE, stampOffset + ORDINAL_LENGTH_OFFSET, (short) ORDINAL_TYPE.byteSize());
        memorySegment.set(ORDINAL_TYPE, stampOffset + ORDINAL_OFFSET, ordinal);
    }
}
