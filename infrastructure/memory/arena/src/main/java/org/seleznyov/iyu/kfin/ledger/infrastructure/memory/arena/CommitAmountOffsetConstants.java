package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import java.lang.foreign.ValueLayout;

public interface CommitAmountOffsetConstants {
    int CPU_CACHE_LINE_SIZE = 64;

    int COMMIT_ACCOUNT_ID_MSB_OFFSET = 0;
    ValueLayout COMMIT_ACCOUNT_ID_SB_TYPE = ValueLayout.JAVA_LONG;
    int COMMIT_ACCOUNT_ID_LSB_OFFSET = (int) (COMMIT_ACCOUNT_ID_MSB_OFFSET + COMMIT_ACCOUNT_ID_SB_TYPE.byteSize());

    int COMMIT_AMOUNT_OFFSET = (int) (COMMIT_ACCOUNT_ID_LSB_OFFSET + COMMIT_ACCOUNT_ID_SB_TYPE.byteSize());
    ValueLayout COMMIT_AMOUNT_TYPE = ValueLayout.JAVA_LONG;

    int POSTGRES_COMMIT_RAW_SIZE = (int) (COMMIT_AMOUNT_OFFSET + COMMIT_AMOUNT_TYPE.byteSize());

    int POSTGRES_COMMIT_SIZE = (POSTGRES_COMMIT_RAW_SIZE + CPU_CACHE_LINE_SIZE - 1) & -CPU_CACHE_LINE_SIZE;
}
