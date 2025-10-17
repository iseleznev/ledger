package org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.stamper;

import org.seleznyov.iyu.kfin.ledgerservice.core.hashtable.AccountPartitionHashTable;

import java.lang.foreign.MemorySegment;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.seleznyov.iyu.kfin.ledgerservice.core.constants.AccountPartitionHashTableConstants.*;
import static org.seleznyov.iyu.kfin.ledgerservice.core.constants.CommonConstants.LENGTH_TYPE;
import static org.seleznyov.iyu.kfin.ledgerservice.core.constants.SnapshotOffsetConstants.*;
import static org.seleznyov.iyu.kfin.ledgerservice.core.utils.Uuid7Utils.nextUuid7;

public class SnapshotRingBufferStamper implements RingBufferStamper {

    private final MemorySegment hashTableMemorySegment;
    private long accountOffset;


    public SnapshotRingBufferStamper(AccountPartitionHashTable partitionHashTable) {
        this.hashTableMemorySegment = partitionHashTable.memorySegment();
    }

    public void accountOffset(long offset) {
        this.accountOffset = offset;
    }

    @Override
    public void stamp(MemorySegment memorySegment, long stampOffset, long availableSize, long walSequenceId, long stateSequenceId) {
        if (availableSize < POSTGRES_SNAPSHOT_SIZE) {
            throw new IllegalArgumentException("Not enough space in ring buffer to stamp snapshot");
        }
        stampSnapshot(memorySegment, stampOffset);
    }

    @Override
    public long stampRecordSize() {
        return POSTGRES_SNAPSHOT_SIZE;
    }

    private void stampSnapshot(MemorySegment memorySegment, long stampOffset) {
        // SNAPSHOT_BALANCE
        memorySegment.set(LENGTH_TYPE, stampOffset + SNAPSHOT_BALANCE_LENGTH_OFFSET, (short) SNAPSHOT_BALANCE_TYPE.byteSize());
        memorySegment.set(
            SNAPSHOT_BALANCE_TYPE, stampOffset + SNAPSHOT_BALANCE_OFFSET,
            hashTableMemorySegment.get(HASH_TABLE_ACCOUNT_BALANCE_TYPE, accountOffset + HASH_TABLE_ACCOUNT_BALANCE_OFFSET)
        );

        final UUID snapshotId = nextUuid7();
        // SNAPSHOT_ID
        memorySegment.set(LENGTH_TYPE, stampOffset + SNAPSHOT_ID_LENGTH_OFFSET, (short) (SNAPSHOT_ID_SB_TYPE.byteSize() * 2));
        memorySegment.set(SNAPSHOT_ID_SB_TYPE, stampOffset + SNAPSHOT_ID_MSB_OFFSET, snapshotId.getMostSignificantBits());
        memorySegment.set(SNAPSHOT_ID_SB_TYPE, stampOffset + SNAPSHOT_ID_LSB_OFFSET, snapshotId.getLeastSignificantBits());

        // SNAPSHOT_CREATED_AT
        memorySegment.set(LENGTH_TYPE, stampOffset + SNAPSHOT_CREATED_AT_LENGTH_OFFSET, (short) SNAPSHOT_CREATED_AT_TYPE.byteSize());
        memorySegment.set(SNAPSHOT_CREATED_AT_TYPE, stampOffset + SNAPSHOT_CREATED_AT_OFFSET, LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli());

        // SNAPSHOT_OPERATION_DAY
        memorySegment.set(LENGTH_TYPE, stampOffset + SNAPSHOT_OPERATION_DAY_LENGTH_OFFSET, (short) SNAPSHOT_OPERATION_DAY_TYPE.byteSize());
        memorySegment.set(SNAPSHOT_OPERATION_DAY_TYPE, stampOffset + SNAPSHOT_OPERATION_DAY_OFFSET, 0L); //TODO: determine operation day source

        // SNAPSHOT_ACCOUNT_ID
        memorySegment.set(LENGTH_TYPE, stampOffset + SNAPSHOT_ACCOUNT_ID_LENGTH_OFFSET, (short) (SNAPSHOT_ACCOUNT_ID_TYPE.byteSize() * 2));
        memorySegment.set(
            SNAPSHOT_ACCOUNT_ID_TYPE, stampOffset + SNAPSHOT_ACCOUNT_ID_MSB_OFFSET,
            hashTableMemorySegment.get(HASH_TABLE_ACCOUNT_ID_TYPE, accountOffset + HASH_TABLE_ACCOUNT_ID_MSB_OFFSET)
        );
        memorySegment.set(
            SNAPSHOT_ACCOUNT_ID_TYPE, stampOffset + SNAPSHOT_ACCOUNT_ID_LSB_OFFSET,
            hashTableMemorySegment.get(HASH_TABLE_ACCOUNT_ID_TYPE, accountOffset + HASH_TABLE_ACCOUNT_ID_LSB_OFFSET)
        );

        // SNAPSHOT_CURRENCY_CODE
        memorySegment.set(LENGTH_TYPE, stampOffset + SNAPSHOT_CURRENCY_CODE_LENGTH_OFFSET, (short) SNAPSHOT_CURRENCY_CODE_TYPE.byteSize());
        memorySegment.set(SNAPSHOT_CURRENCY_CODE_TYPE, stampOffset + SNAPSHOT_CURRENCY_CODE_OFFSET, 0L); //TODO: determine currency code source

        // SNAPSHOT_ORDINAL
        memorySegment.set(LENGTH_TYPE, stampOffset + SNAPSHOT_ORDINAL_LENGTH_OFFSET, (short) SNAPSHOT_ORDINAL_TYPE.byteSize());
        memorySegment.set(
            SNAPSHOT_ORDINAL_TYPE, stampOffset + SNAPSHOT_ORDINAL_OFFSET,
            hashTableMemorySegment.get(HASH_TABLE_ORDINAL_TYPE, accountOffset + HASH_TABLE_ORDINAL_OFFSET)
        );

        // SNAPSHOT_ENTRY_RECORD_ORDINAL
        memorySegment.set(LENGTH_TYPE, stampOffset + SNAPSHOT_ENTRY_RECORD_ORDINAL_LENGTH_OFFSET, (short) SNAPSHOT_ENTRY_RECORD_ORDINAL_TYPE.byteSize());
        memorySegment.set(SNAPSHOT_ENTRY_RECORD_ORDINAL_TYPE, stampOffset + SNAPSHOT_ENTRY_RECORD_ORDINAL_OFFSET, accountOffset + HASH_TABLE_ACCOUNT_ORDINAL_OFFSET);
    }
}
