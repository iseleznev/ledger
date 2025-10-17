package org.seleznyov.iyu.kfin.ledgerservice.core.actor.processor;

import org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties.LedgerConfiguration;
import org.seleznyov.iyu.kfin.ledgerservice.core.hashtable.AccountPartitionHashTable;
import org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.handler.SimpleSWSRRingBufferHandler;
import org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.stamper.SnapshotRingBufferStamper;

import java.util.UUID;

import static org.seleznyov.iyu.kfin.ledgerservice.core.constants.EntryRecordOffsetConstants.POSTGRES_ENTRY_RECORD_SIZE;
import static org.seleznyov.iyu.kfin.ledgerservice.core.constants.PartitionAmountOffsetConstants.*;
import static org.seleznyov.iyu.kfin.ledgerservice.core.utils.PartitionUtils.partitionNumber;

public class PartitionActorPartitionAmountProcessor {

    private final SimpleSWSRRingBufferHandler partitionAmountRingBufferHandler;
    private final SnapshotRingBufferStamper snapshotRingBufferStamper;
    private final SimpleSWSRRingBufferHandler snapshotRingBufferHandler;

    public PartitionActorPartitionAmountProcessor(
        SimpleSWSRRingBufferHandler partitionAmountRingBufferHandler,
        SnapshotRingBufferStamper snapshotRingBufferStamper,
        SimpleSWSRRingBufferHandler snapshotRingBufferHandler
    ) {
        this.partitionAmountRingBufferHandler = partitionAmountRingBufferHandler;
        this.snapshotRingBufferStamper = snapshotRingBufferStamper;
        this.snapshotRingBufferHandler = snapshotRingBufferHandler;
    }

    public void processAmounts(AccountPartitionHashTable accountsPartitionHashTable, LedgerConfiguration ledgerConfiguration, long actorPartitionId) {
        final long partitionAmountProcessedSize = partitionAmountRingBufferHandler.readyToProcess(ledgerConfiguration.partitionActor().partitionAmountsCountQuota() * POSTGRES_ENTRY_RECORD_SIZE, POSTGRES_ENTRY_RECORD_SIZE);
        if (partitionAmountProcessedSize >= 0) {
            long partitionProcessOffset = partitionAmountRingBufferHandler.processOffset();
            for (long offset = partitionAmountRingBufferHandler.processOffset(); offset < partitionProcessOffset + partitionAmountProcessedSize; offset += POSTGRES_ENTRY_RECORD_SIZE) {
                final long accountMsb = partitionAmountRingBufferHandler.memorySegment().get(PARTITION_AMOUNT_ACCOUNT_ID_SB_TYPE, offset + PARTITION_AMOUNT_ACCOUNT_ID_MSB_OFFSET);
                final long accountLsb = partitionAmountRingBufferHandler.memorySegment().get(PARTITION_AMOUNT_ACCOUNT_ID_SB_TYPE, offset + PARTITION_AMOUNT_ACCOUNT_ID_LSB_OFFSET);
                final UUID accountId = new UUID(accountMsb, accountLsb);
                final long amount = partitionAmountRingBufferHandler.memorySegment().get(PARTITION_AMOUNT_AMOUNT_TYPE, offset + PARTITION_AMOUNT_AMOUNT_OFFSET);;
                long accountTableOffset;
                final int accountPartitionId = partitionNumber(accountId, ledgerConfiguration.accountsPartitionsCount());
                if (accountPartitionId == actorPartitionId) {
                    accountTableOffset = accountsPartitionHashTable.getOffset(accountId);
                    if (accountTableOffset < 0) {
                        //TODO: загрузить аккаунт из WAL или из базы, если включена ленивая загрузка
                        // в противном случае сразу считаем, что это аккаунт из другой партиции или другого экземпляра приложения
                        //TODO: вставляем в таблицу только в случае, если точно знаем, что этот аккаунт должен быть в этой партиции
                        // и в этом экземпляре приложения
                        accountTableOffset = accountsPartitionHashTable.put(accountId, 0, 0, 0);
                    } else {
                        accountsPartitionHashTable.increaseBalance(accountTableOffset, amount);
                        snapshotRingBufferStamper.accountOffset(accountTableOffset);
                        //TODO: при достижении времени или количества с последнего снэпшота
                        snapshotRingBufferHandler.tryStampForward(
                            snapshotRingBufferStamper,
                            ledgerConfiguration.ringBuffer().snapshot().writeAttempts()
                        );
                    }
                }
            }
            partitionAmountRingBufferHandler.processedForward(partitionAmountProcessedSize);
        }
    }
}
