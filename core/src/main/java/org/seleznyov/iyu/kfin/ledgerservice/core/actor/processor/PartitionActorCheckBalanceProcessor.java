package org.seleznyov.iyu.kfin.ledgerservice.core.actor.processor;

import org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties.LedgerConfiguration;
import org.seleznyov.iyu.kfin.ledgerservice.core.hashtable.AccountPartitionHashTable;
import org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.handler.MWSRRingBufferHandler;
import org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.handler.SimpleSWSRRingBufferHandler;
import org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.stamper.SnapshotRingBufferStamper;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.UUID;

import static org.seleznyov.iyu.kfin.ledgerservice.core.constants.TransferRequestOffsetConstants.*;

public class PartitionActorCheckBalanceProcessor {

    private final SimpleSWSRRingBufferHandler partitionAmountRingBufferHandler;
    private final SnapshotRingBufferStamper snapshotRingBufferStamper;
    private final SimpleSWSRRingBufferHandler snapshotRingBufferHandler;
    private final MemorySegment transferRingBufferMemorySegment;

    public PartitionActorCheckBalanceProcessor(
        SimpleSWSRRingBufferHandler partitionAmountRingBufferHandler,
        SnapshotRingBufferStamper snapshotRingBufferStamper,
        SimpleSWSRRingBufferHandler snapshotRingBufferHandler,
        MWSRRingBufferHandler transferRingBufferHandler
    ) {
        this.partitionAmountRingBufferHandler = partitionAmountRingBufferHandler;
        this.snapshotRingBufferStamper = snapshotRingBufferStamper;
        this.snapshotRingBufferHandler = snapshotRingBufferHandler;
        this.transferRingBufferMemorySegment = transferRingBufferHandler.memorySegment();
    }

    public boolean checkBalance(AccountPartitionHashTable accountsPartitionHashTable, LedgerConfiguration ledgerConfiguration, long actorPartitionId, long transferOffset) {
        final long accountIdMsb = transferRingBufferMemorySegment.get(ValueLayout.JAVA_LONG, transferOffset + TRANSFER_DEBIT_ACCOUNT_ID_MSB_OFFSET);
        final long accountIdLsb = transferRingBufferMemorySegment.get(ValueLayout.JAVA_LONG, transferOffset + TRANSFER_DEBIT_ACCOUNT_ID_LSB_OFFSET);
        final long amount = transferRingBufferMemorySegment.get(ValueLayout.JAVA_LONG, transferOffset + TRANSFER_AMOUNT_OFFSET);
        final UUID debitAccountId = new UUID(accountIdMsb, accountIdLsb);
        final long accountOffset = accountsPartitionHashTable.getOffset(debitAccountId);
        final long accountTableOrdinal = accountsPartitionHashTable.getAccountTableOrdinal(accountOffset);
        final long committedTableOrdinal = accountsPartitionHashTable.committedTableOrdinal();
        final long stagedAmount = accountsPartitionHashTable.getStagedAmount(accountOffset);
        if (committedTableOrdinal > accountTableOrdinal) {
            accountsPartitionHashTable.increaseBalance(accountOffset, stagedAmount);
            accountsPartitionHashTable.updateAccountTableCurrentOrdinal(accountOffset, committedTableOrdinal);
            snapshotRingBufferStamper.accountOffset(accountOffset);
            //TODO: при достижении времени или количества с последнего снэпшота
            snapshotRingBufferHandler.tryStampForward(
                snapshotRingBufferStamper,
                ledgerConfiguration.ringBuffer().snapshot().writeAttempts()
            );
        }
        final long balance = accountsPartitionHashTable.getBalance(accountOffset);
        return balance >= amount;
    }
}
