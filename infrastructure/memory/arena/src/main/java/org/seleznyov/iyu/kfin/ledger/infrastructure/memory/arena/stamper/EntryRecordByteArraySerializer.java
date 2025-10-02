package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.stamper;

import org.seleznyov.iyu.kfin.ledger.domain.model.entryrecord.EntryRecord;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;

import static org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.EntryRecordOffsetConstants.*;

public class EntryRecordByteArraySerializer {


    public void serialize(EntryRecord entryRecord, MemorySegment memorySegment, long serializeOffset) {
        memorySegment.set(ValueLayout.JAVA_LONG, serializeOffset + AMOUNT_OFFSET, entryRecord.amount());
        memorySegment.set(
            ValueLayout.JAVA_SHORT,
            serializeOffset + ENTRY_TYPE_OFFSET,
            entryRecord.entryType().shortFromEntryType()
        );

        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, serializeOffset + ENTRY_RECORD_ID_MSB_OFFSET, entryRecord.id().getMostSignificantBits());
        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, serializeOffset + ENTRY_RECORD_ID_LSB_OFFSET, entryRecord.id().getLeastSignificantBits());

        memorySegment.set(
            ValueLayout.JAVA_LONG,
            serializeOffset + CREATED_AT_OFFSET,
            entryRecord.createdAt().toInstant(ZoneOffset.UTC).toEpochMilli()
        );

        memorySegment.set(
            ValueLayout.JAVA_LONG,
            serializeOffset + OPERATION_DAY_OFFSET,
            entryRecord.operationDay().atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli()
        );

        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, serializeOffset + ACCOUNT_ID_MSB_OFFSET, entryRecord.accountId().getMostSignificantBits());
        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, serializeOffset + ACCOUNT_ID_LSB_OFFSET, entryRecord.accountId().getLeastSignificantBits());

        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, serializeOffset + ENTRY_RECORD_TRANSFER_ID_MSB_OFFSET, entryRecord.transactionId().getMostSignificantBits());
        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, serializeOffset + ENTRY_RECORD_TRANSFER_ID_LSB_OFFSET, entryRecord.transactionId().getLeastSignificantBits());

        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, serializeOffset + IDEMPOTENCY_KEY_MSB_OFFSET, entryRecord.idempotencyKey().getMostSignificantBits());
        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, serializeOffset + IDEMPOTENCY_KEY_LSB_OFFSET, entryRecord.idempotencyKey().getLeastSignificantBits());

        MemorySegment.copy(
            MemorySegment.ofArray(entryRecord.currencyCode().getBytes(StandardCharsets.UTF_8)),
            0,
            memorySegment,
            serializeOffset + CURRENCY_CODE_OFFSET,
            entryRecord.currencyCode().length()
        );

//        final long entryOrdinal = accountStateHashTable.nextOrdinal(hashTableOffset);

        memorySegment.set(ValueLayout.JAVA_LONG, serializeOffset + ORDINAL_OFFSET, entryRecord.ordinal());
    }
}
