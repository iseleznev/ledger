package org.seleznyov.iyu.kfin.ledgerservice.core.constants;

import java.lang.foreign.ValueLayout;

import static org.seleznyov.iyu.kfin.ledgerservice.core.constants.CommonConstants.CPU_CACHE_LINE_SIZE;

public interface PartitionAmountOffsetConstants {
    int PARTITION_AMOUNT_ACCOUNT_ID_MSB_OFFSET = 0;
    ValueLayout.OfLong PARTITION_AMOUNT_ACCOUNT_ID_SB_TYPE = ValueLayout.JAVA_LONG;
    int PARTITION_AMOUNT_ACCOUNT_ID_LSB_OFFSET = (int) (PARTITION_AMOUNT_ACCOUNT_ID_MSB_OFFSET + PARTITION_AMOUNT_ACCOUNT_ID_SB_TYPE.byteSize());

    int PARTITION_AMOUNT_AMOUNT_OFFSET = (int) (PARTITION_AMOUNT_ACCOUNT_ID_LSB_OFFSET + PARTITION_AMOUNT_ACCOUNT_ID_SB_TYPE.byteSize());
    ValueLayout.OfLong PARTITION_AMOUNT_AMOUNT_TYPE = ValueLayout.JAVA_LONG;

    int PARTITION_AMOUNT_RAW_SIZE = (int) (PARTITION_AMOUNT_AMOUNT_OFFSET + PARTITION_AMOUNT_AMOUNT_TYPE.byteSize());

    int PARTITION_AMOUNT_SIZE = (PARTITION_AMOUNT_RAW_SIZE + CPU_CACHE_LINE_SIZE - 1) & -CPU_CACHE_LINE_SIZE;
}
