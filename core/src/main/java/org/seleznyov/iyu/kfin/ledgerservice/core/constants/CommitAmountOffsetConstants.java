package org.seleznyov.iyu.kfin.ledgerservice.core.constants;

import java.lang.foreign.ValueLayout;

import static org.seleznyov.iyu.kfin.ledgerservice.core.constants.CommonConstants.CPU_CACHE_LINE_SIZE;

public interface CommitAmountOffsetConstants {
    int COMMIT_ACCOUNT_ID_MSB_OFFSET = 0;
    ValueLayout.OfLong COMMIT_ACCOUNT_ID_SB_TYPE = ValueLayout.JAVA_LONG;
    int COMMIT_ACCOUNT_ID_LSB_OFFSET = (int) (COMMIT_ACCOUNT_ID_MSB_OFFSET + COMMIT_ACCOUNT_ID_SB_TYPE.byteSize());

    int COMMIT_AMOUNT_OFFSET = (int) (COMMIT_ACCOUNT_ID_LSB_OFFSET + COMMIT_ACCOUNT_ID_SB_TYPE.byteSize());
    ValueLayout.OfLong COMMIT_AMOUNT_TYPE = ValueLayout.JAVA_LONG;

    int POSTGRES_COMMIT_RAW_SIZE = (int) (COMMIT_AMOUNT_OFFSET + COMMIT_AMOUNT_TYPE.byteSize());

    int POSTGRES_COMMIT_SIZE = (POSTGRES_COMMIT_RAW_SIZE + CPU_CACHE_LINE_SIZE - 1) & -CPU_CACHE_LINE_SIZE;
}
