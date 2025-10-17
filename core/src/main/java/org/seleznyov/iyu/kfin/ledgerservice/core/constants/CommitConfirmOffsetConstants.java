package org.seleznyov.iyu.kfin.ledgerservice.core.constants;

import java.lang.foreign.ValueLayout;

import static org.seleznyov.iyu.kfin.ledgerservice.core.constants.CommonConstants.CPU_CACHE_LINE_SIZE;

public interface CommitConfirmOffsetConstants {
    int COMMIT_CONFIRM_PARTITION_HASH_TABLE_COMMIT_ORDINAL_OFFSET = 0;
    ValueLayout.OfLong COMMIT_CONFIRM_PARTITION_HASH_TABLE_COMMIT_ORDINAL_TYPE = ValueLayout.JAVA_LONG;

    int COMMIT_CONFIRM_RAW_SIZE = (int) (COMMIT_CONFIRM_PARTITION_HASH_TABLE_COMMIT_ORDINAL_OFFSET + COMMIT_CONFIRM_PARTITION_HASH_TABLE_COMMIT_ORDINAL_TYPE.byteSize());

    int COMMIT_CONFIRM_SIZE = (COMMIT_CONFIRM_RAW_SIZE + CPU_CACHE_LINE_SIZE - 1) & -CPU_CACHE_LINE_SIZE;
}
