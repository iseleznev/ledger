package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration.LedgerConfiguration;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.ringbuffer.SWSRRingBufferHandler;

import java.util.UUID;

import static org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.EntryRecordOffsetConstants.POSTGRES_ENTRY_RECORD_SIZE;
import static org.seleznyov.iyu.kfin.ledger.shared.utils.AccountRouteUtils.partitionNumber;

public class AccountsPartitionActorCommitProcessor {

    private final SWSRRingBufferHandler commitRingBufferHandler;

    public AccountsPartitionActorCommitProcessor(SWSRRingBufferHandler commitRingBufferHandler) {
        this.commitRingBufferHandler = commitRingBufferHandler;
    }

    public void processCommits(AccountsPartitionHashTable accountsPartitionHashTable, LedgerConfiguration ledgerConfiguration, long actorPartitionId) {
        final long commitProcessedSize = commitRingBufferHandler.readyToProcess(ledgerConfiguration.partitionActor().commitsCountQuota() * POSTGRES_ENTRY_RECORD_SIZE, POSTGRES_ENTRY_RECORD_SIZE);
        if (commitProcessedSize >= 0) {
            long commitProcessOffset = commitRingBufferHandler.readOffset();
            for (long offset = commitRingBufferHandler.readOffset(); offset < commitProcessOffset + commitProcessedSize; offset += POSTGRES_ENTRY_RECORD_SIZE) {
                final UUID accountId;
                final long amount = 0;
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
                    }
                }
            }
            commitRingBufferHandler.processedForward(commitProcessedSize);
        }
    }
}
