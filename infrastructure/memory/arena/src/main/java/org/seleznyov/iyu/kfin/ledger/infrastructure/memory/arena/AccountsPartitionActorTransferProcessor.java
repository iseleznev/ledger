package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration.LedgerConfiguration;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.ringbuffer.SWMRRingBufferHandler;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.ringbuffer.SWSRRingBufferHandler;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.stamper.AccountsFileTableRingBufferStamper;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.stamper.EntryRecordRingBufferStamper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.UUID;
import java.util.concurrent.locks.LockSupport;

import static org.seleznyov.iyu.kfin.ledger.domain.model.entryrecord.EntryType.CREDIT;
import static org.seleznyov.iyu.kfin.ledger.domain.model.entryrecord.EntryType.DEBIT;
import static org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.EntryRecordOffsetConstants.POSTGRES_ENTRY_RECORD_SIZE;
import static org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.TransferRequestOffsetConstants.*;
import static org.seleznyov.iyu.kfin.ledger.shared.utils.AccountRouteUtils.partitionNumber;

public class AccountsPartitionActorTransferProcessor {
    private static final Logger log = LoggerFactory.getLogger(AccountsPartitionActorTransferProcessor.class);


    private final SWSRRingBufferHandler actorTransferRingBufferHandler;
    private final EntryRecordRingBufferStamper stamper;
    private final AccountsFileTableRingBufferStamper fileTableStamper;
    private final SWSRRingBufferHandler walRingBufferHandler;
    private final SWSRRingBufferHandler tableRingBufferHandler;
    private final SWSRRingBufferHandler postgresRingBufferHandler;

    public AccountsPartitionActorTransferProcessor(
        SWSRRingBufferHandler actorTransferRingBufferHandler,
        SWSRRingBufferHandler walRingBufferHandler,
        SWSRRingBufferHandler tableRingBufferHandler,
        SWSRRingBufferHandler postgresRingBufferHandler
    ) {
        this.actorTransferRingBufferHandler = actorTransferRingBufferHandler;
        this.walRingBufferHandler = walRingBufferHandler;
        this.tableRingBufferHandler = tableRingBufferHandler;
        this.postgresRingBufferHandler = postgresRingBufferHandler;
        this.stamper = new EntryRecordRingBufferStamper(this.actorTransferRingBufferHandler);
        this.fileTableStamper = new AccountsFileTableRingBufferStamper(this.actorTransferRingBufferHandler);
    }

    public void processTransfers(AccountsPartitionHashTable accountsPartitionHashTable, LedgerConfiguration ledgerConfiguration, long actorPartitionId) {
        final long transferProcessSize = actorTransferRingBufferHandler.readyToProcess(ledgerConfiguration.partitionActor().transfersCountQuota() * POSTGRES_ENTRY_RECORD_SIZE, POSTGRES_ENTRY_RECORD_SIZE);
        if (transferProcessSize >= 0) {
            long transferProcessOffset = actorTransferRingBufferHandler.readOffset();
            for (long offset = actorTransferRingBufferHandler.readOffset(); offset < transferProcessOffset + transferProcessSize; offset += POSTGRES_ENTRY_RECORD_SIZE) {
                final MemorySegment memorySegment = actorTransferRingBufferHandler.memorySegment();
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
                final int debitAccountPartitionId = partitionNumber(debitAccount, ledgerConfiguration.accountsPartitionsCount());
                int homeEntryType = DEBIT.shortFromEntryType();
                if (debitAccountPartitionId == actorPartitionId) {
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
                if (creditAccountPartitionId == actorPartitionId) {
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
                final long nextCurrentOrdinalOffset = accountsPartitionHashTable.currentOrdinalSequenceForward();
                accountsPartitionHashTable.ordinalForward(homeAccountTableOffset);
                accountsPartitionHashTable.increaseStagedAmount(homeAccountTableOffset, amount);
                accountsPartitionHashTable.updateCurrentOrdinalSequence(homeAccountTableOffset, nextCurrentOrdinalOffset);
                if (homeEntryType == DEBIT.shortFromEntryType()) {
                    stamper.debitAccountEntryRecordOrdinal(
                        accountsPartitionHashTable.ordinalForward(homeAccountTableOffset)
                    );
                    if (homeCandidateAccountTableOffset >= 0) {
                        stamper.creditAccountEntryRecordOrdinal(
                            accountsPartitionHashTable.ordinalForward(homeCandidateAccountTableOffset)
                        );
                    }
                } else {
                    stamper.creditAccountEntryRecordOrdinal(
                        accountsPartitionHashTable.ordinalForward(homeAccountTableOffset)
                    );
                    if (homeCandidateAccountTableOffset >= 0) {
                        stamper.debitAccountEntryRecordOrdinal(
                            accountsPartitionHashTable.ordinalForward(homeCandidateAccountTableOffset)
                        );
                    }
                }
                stamper.transactionOffset(offset);
                fileTableStamper.transactionOffset(offset);
                final boolean walStamped = walRingBufferHandler.tryStampForward(stamper, ledgerConfiguration.ringBuffer().wal().writeAttempts());
                if (!walStamped) {
                    log.error("Failed to stamp entry record to WAL ring buffer due {} attempts", ledgerConfiguration.ringBuffer().wal().writeAttempts());
                    while (!walRingBufferHandler.tryStampForward(stamper, ledgerConfiguration.ringBuffer().wal().writeAttempts())) {
                        LockSupport.parkNanos(100_000_000L);
                    }
                }
                final boolean tableStamped = tableRingBufferHandler.tryStampForward(stamper, ledgerConfiguration.ringBuffer().accountsFileTable().writeAttempts());
                if (!tableStamped) {
                    log.error("Failed to stamp entry record to accounts file table ring buffer due {} attempts", ledgerConfiguration.ringBuffer().accountsFileTable().writeAttempts());
                    while (!walRingBufferHandler.tryStampForward(stamper, ledgerConfiguration.ringBuffer().accountsFileTable().writeAttempts())) {
                        LockSupport.parkNanos(100_000_000L);
                    }
                }
                final boolean postgresStamped = postgresRingBufferHandler.tryStampForward(stamper, ledgerConfiguration.ringBuffer().postgres().writeAttempts());
                if (!postgresStamped) {
                    log.error("Failed to stamp entry record to postgres ring buffer due {} attempts", ledgerConfiguration.ringBuffer().postgres().writeAttempts());
                    while (!walRingBufferHandler.tryStampForward(stamper, ledgerConfiguration.ringBuffer().postgres().writeAttempts())) {
                        LockSupport.parkNanos(100_000_000L);
                    }
                }
//                processingRingBufferHandler.tryStampForward(stamper, ledgerConfiguration.ringBuffer().processing().writeAttempts());
            }
            actorTransferRingBufferHandler.processedForward(transferProcessSize);
        }
    }
}
