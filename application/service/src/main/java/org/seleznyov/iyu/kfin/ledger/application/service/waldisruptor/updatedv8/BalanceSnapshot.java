package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor.updatedv8;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

/**
 * Balance snapshot for fast recovery
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BalanceSnapshot {

    private long snapshotId;
    private LocalDateTime timestamp;
    private long firstOffset;
    private long lastOffset;
    private long operationCount;
    private Map<UUID, AccountBalance> balances;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AccountBalance {

        private UUID accountId;
        private long balance; // In minor units
        private String currencyCode;
        private long lastOperationSequence;
        private LocalDateTime lastUpdated;
    }

    // Binary format for snapshot header (without balance data)
    public static final int HEADER_SIZE =
        8 + // snapshotId
            8 + // timestamp
            8 + // firstOffset
            8 + // lastOffset
            8 + // operationCount
            4;  // balances count
    // Total: 44 bytes header

    public static final int BALANCE_ENTRY_SIZE =
        16 + // accountId UUID
            8 +  // balance
            8 +  // currencyCode (padded)
            8 +  // lastOperationSequence
            8;   // lastUpdated
    // Total: 48 bytes per balance
}
