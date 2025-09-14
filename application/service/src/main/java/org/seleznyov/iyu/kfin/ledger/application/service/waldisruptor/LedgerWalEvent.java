package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.util.UUID;

/**
 * WAL Event for high-performance ledger operations
 * Designed for zero-GC reuse in Disruptor ring buffer
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public final class LedgerWalEvent {

    private long sequenceNumber;
    private UUID debitAccountId;
    private UUID creditAccountId;
    private long amount; // Minor units (kopecks)
    private String currencyCode;
    private LocalDate operationDate;
    private UUID transactionId;
    private UUID idempotencyKey;
    private WalStatus status;

    // Pre-allocated for performance
    private static final UUID EMPTY_UUID = new UUID(0L, 0L);

    public void reset() {
        this.sequenceNumber = 0L;
        this.debitAccountId = EMPTY_UUID;
        this.creditAccountId = EMPTY_UUID;
        this.amount = 0L;
        this.currencyCode = "RUB";
        this.operationDate = LocalDate.now();
        this.transactionId = EMPTY_UUID;
        this.idempotencyKey = EMPTY_UUID;
        this.status = WalStatus.PENDING;
    }

    public LedgerWalEvent copy() {
        return LedgerWalEvent.builder()
            .sequenceNumber(this.sequenceNumber)
            .debitAccountId(this.debitAccountId)
            .creditAccountId(this.creditAccountId)
            .amount(this.amount)
            .currencyCode(this.currencyCode)
            .operationDate(this.operationDate)
            .transactionId(this.transactionId)
            .idempotencyKey(this.idempotencyKey)
            .status(this.status)
            .build();
    }
}
