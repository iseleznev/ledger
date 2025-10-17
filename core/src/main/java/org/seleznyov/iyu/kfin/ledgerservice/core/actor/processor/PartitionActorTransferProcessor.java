package org.seleznyov.iyu.kfin.ledgerservice.core.actor.processor;

import org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties.LedgerConfiguration;
import org.seleznyov.iyu.kfin.ledgerservice.core.constants.TransferRequestOffsetConstants;
import org.seleznyov.iyu.kfin.ledgerservice.core.hashtable.AccountPartitionHashTable;
import org.seleznyov.iyu.kfin.ledgerservice.core.hashtable.AccountPartitionLazyResizeHashTable;
import org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.handler.SWSRRingBufferHandler;
import org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.handler.SimpleSWSRRingBufferHandler;
import org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.stamper.EntryRecordRingBufferStamper;
import org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.stamper.SnapshotRingBufferStamper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.UUID;
import java.util.concurrent.locks.LockSupport;

import static org.seleznyov.iyu.kfin.ledgerservice.core.constants.EntryRecordOffsetConstants.POSTGRES_ENTRY_RECORD_SIZE;
import static org.seleznyov.iyu.kfin.ledgerservice.core.constants.TransferRequestOffsetConstants.*;
import static org.seleznyov.iyu.kfin.ledgerservice.core.model.EntryType.CREDIT;
import static org.seleznyov.iyu.kfin.ledgerservice.core.model.EntryType.DEBIT;
import static org.seleznyov.iyu.kfin.ledgerservice.core.utils.PartitionUtils.partitionNumber;

public class PartitionActorTransferProcessor {
    private static final Logger log = LoggerFactory.getLogger(PartitionActorTransferProcessor.class);


    private final SWSRRingBufferHandler transferRequestRingBufferHandler;
    private final EntryRecordRingBufferStamper entryRecordStamper;
//    private final SnapshotRingBufferStamper snapshotStamper;
    private final SWSRRingBufferHandler walRingBufferHandler;
    private final SnapshotRingBufferStamper snapshotRingBufferStamper;
    private final SimpleSWSRRingBufferHandler snapshotRingBufferHandler;
//    private final SWSRRingBufferHandler postgresRingBufferHandler;
    private final AccountPartitionHashTable accountPartitionHashTable;
    private final int actorPartitionNumber;

    public PartitionActorTransferProcessor(
        AccountPartitionHashTable accountPartitionHashTable,
        SWSRRingBufferHandler transferRequestRingBufferHandler,
        SWSRRingBufferHandler walRingBufferHandler,
        SnapshotRingBufferStamper snapshotRingBufferStamper,
        SimpleSWSRRingBufferHandler snapshotRingBufferHandler,
//        SWSRRingBufferHandler postgresRingBufferHandler,
        int actorPartitionNumber
    ) {
        this.transferRequestRingBufferHandler = transferRequestRingBufferHandler;
        this.walRingBufferHandler = walRingBufferHandler;
        this.snapshotRingBufferHandler = snapshotRingBufferHandler;
        this.snapshotRingBufferStamper = snapshotRingBufferStamper;
//        this.postgresRingBufferHandler = postgresRingBufferHandler;
        this.entryRecordStamper = new EntryRecordRingBufferStamper(this.transferRequestRingBufferHandler);
        this.accountPartitionHashTable = accountPartitionHashTable;
//        this.snapshotStamper = new SnapshotRingBufferStamper(this.accountPartitionHashTable);
        this.actorPartitionNumber = actorPartitionNumber;
    }

    public void processTransfers(AccountPartitionHashTable accountsPartitionHashTable, LedgerConfiguration ledgerConfiguration) {
        final long transferProcessSize = transferRequestRingBufferHandler.readyToProcess(ledgerConfiguration.partitionActor().transfersCountQuota() * POSTGRES_ENTRY_RECORD_SIZE, POSTGRES_ENTRY_RECORD_SIZE);
        if (transferProcessSize >= 0) {
            long transferProcessOffset = transferRequestRingBufferHandler.processOffset();
            for (long offset = transferRequestRingBufferHandler.processOffset(); offset < transferProcessOffset + transferProcessSize; offset += POSTGRES_ENTRY_RECORD_SIZE) {
                final MemorySegment memorySegment = transferRequestRingBufferHandler.memorySegment();
                final UUID creditAccount = new UUID(
                    memorySegment.get(ValueLayout.JAVA_LONG, offset + TRANSFER_CREDIT_ACCOUNT_ID_MSB_OFFSET),
                    memorySegment.get(ValueLayout.JAVA_LONG, offset + TRANSFER_CREDIT_ACCOUNT_ID_LSB_OFFSET)
                );
                final UUID debitAccount = new UUID(
                    memorySegment.get(ValueLayout.JAVA_LONG, offset + TRANSFER_DEBIT_ACCOUNT_ID_MSB_OFFSET),
                    memorySegment.get(ValueLayout.JAVA_LONG, offset + TRANSFER_DEBIT_ACCOUNT_ID_LSB_OFFSET)
                );
                final long amount = memorySegment.get(ValueLayout.JAVA_LONG, offset + TransferRequestOffsetConstants.TRANSFER_AMOUNT_OFFSET);
                long homeAccountTableOffset = 0;
                final int debitAccountPartitionNumber = partitionNumber(debitAccount, ledgerConfiguration.accountsPartitionsCount());
                int homeEntryType = DEBIT.shortFromEntryType();
                if (debitAccountPartitionNumber == actorPartitionNumber) {
                    homeAccountTableOffset = accountsPartitionHashTable.getOffset(debitAccount);
                    if (homeAccountTableOffset < 0) {
                        //TODO: загрузить аккаунт из WAL или из базы, если включена ленивая загрузка
                        // в противном случае сразу считаем, что это аккаунт из другой партиции или другого экземпляра приложения
                        //TODO: вставляем в таблицу только в случае, если точно знаем, что этот аккаунт должен быть в этой партиции
                        // и в этом экземпляре приложения
                        homeAccountTableOffset = accountsPartitionHashTable.put(debitAccount, 0, 0, 0);
                    }
                }
                long homeCandidateAccountTableOffset = 0;
                final int creditAccountPartitionId = partitionNumber(creditAccount, ledgerConfiguration.accountsPartitionsCount());
                if (creditAccountPartitionId == actorPartitionNumber) {
                    homeCandidateAccountTableOffset = accountsPartitionHashTable.getOffset(creditAccount);
                    if (homeCandidateAccountTableOffset < 0) {
                        //TODO: загрузить аккаунт из WAL или из базы, если включена ленивая загрузка
                        // в противном случае сразу считаем, что это аккаунт из другой партиции или другого экземпляра приложения
                        //TODO: вставляем в таблицу только в случае, если точно знаем, что этот аккаунт должен быть в этой партиции
                        // и в этом экземпляре приложения
                        homeCandidateAccountTableOffset = accountsPartitionHashTable.put(creditAccount, 0, 0, 0);
                    }
                }
                if (homeAccountTableOffset >= 0 || homeCandidateAccountTableOffset >= 0) {
                    final short homeAccountRate = homeAccountTableOffset >= 0
                        ? accountsPartitionHashTable.getAccountRate(homeAccountTableOffset)
                        : -1;
                    final short homeCandidateAccountRate = homeCandidateAccountTableOffset >= 0
                        ? accountsPartitionHashTable.getAccountRate(homeCandidateAccountTableOffset)
                        : -1;
                    if (homeCandidateAccountRate > homeAccountRate) {
//                        final long tempAccountTableOffset = homeAccountTableOffset;
                        homeAccountTableOffset = homeCandidateAccountTableOffset;
//                        homeCandidateAccountTableOffset = tempAccountTableOffset;
                        homeEntryType = CREDIT.shortFromEntryType();
                    }
                }
                final long nextCurrentOrdinalOffset = accountsPartitionHashTable.currentTableOrdinalForward();
                accountsPartitionHashTable.ordinalForward(homeAccountTableOffset);
                accountsPartitionHashTable.increaseStagedAmount(homeAccountTableOffset, amount);
                accountsPartitionHashTable.updateAccountTableCurrentOrdinal(homeAccountTableOffset, nextCurrentOrdinalOffset);
                if (homeEntryType == DEBIT.shortFromEntryType()) {
                    entryRecordStamper.debitAccountEntryRecordOrdinal(
                        accountsPartitionHashTable.ordinalForward(homeAccountTableOffset)
                    );
                    if (homeCandidateAccountTableOffset >= 0) {
                        entryRecordStamper.creditAccountEntryRecordOrdinal(
                            accountsPartitionHashTable.ordinalForward(homeCandidateAccountTableOffset)
                        );
                    }
                } else {
                    entryRecordStamper.creditAccountEntryRecordOrdinal(
                        accountsPartitionHashTable.ordinalForward(homeAccountTableOffset)
                    );
                    if (homeCandidateAccountTableOffset >= 0) {
                        entryRecordStamper.debitAccountEntryRecordOrdinal(
                            accountsPartitionHashTable.ordinalForward(homeCandidateAccountTableOffset)
                        );
                    }
                }
                entryRecordStamper.transactionOffset(offset);
//                snapshotStamper.accountOffset(offset);
                final boolean walStamped = walRingBufferHandler.tryStampForward(entryRecordStamper, ledgerConfiguration.ringBuffer().wal().writeAttempts());
                if (!walStamped) {
                    log.error("Failed to stamp entry record to WAL ring buffer due {} attempts", ledgerConfiguration.ringBuffer().wal().writeAttempts());
                    while (!walRingBufferHandler.tryStampForward(entryRecordStamper, ledgerConfiguration.ringBuffer().wal().writeAttempts())) {
                        LockSupport.parkNanos(100_000_000L);
                    }
                }
//                final boolean tableStamped = snapshotRingBufferHandler.tryStampForward(entryRecordStamper, ledgerConfiguration.ringBuffer().snapshot().writeAttempts());
//                if (!tableStamped) {
//                    log.error("Failed to stamp entry record to accounts file table ring buffer due {} attempts", ledgerConfiguration.ringBuffer().snapshot().writeAttempts());
//                    while (!walRingBufferHandler.tryStampForward(entryRecordStamper, ledgerConfiguration.ringBuffer().snapshot().writeAttempts())) {
//                        LockSupport.parkNanos(100_000_000L);
//                    }
//                }
//                final boolean postgresStamped = postgresRingBufferHandler.tryStampForward(entryRecordStamper, ledgerConfiguration.ringBuffer().postgres().writeAttempts());
//                if (!postgresStamped) {
//                    log.error("Failed to stamp entry record to postgres ring buffer due {} attempts", ledgerConfiguration.ringBuffer().postgres().writeAttempts());
//                    while (!walRingBufferHandler.tryStampForward(entryRecordStamper, ledgerConfiguration.ringBuffer().postgres().writeAttempts())) {
//                        LockSupport.parkNanos(100_000_000L);
//                    }
//                }
//                processingRingBufferHandler.tryStampForward(stamper, ledgerConfiguration.ringBuffer().processing().writeAttempts());
            }
            transferRequestRingBufferHandler.processedForward(transferProcessSize);
        }
    }
}
