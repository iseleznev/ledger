package org.seleznyov.iyu.kfin.ledger.application.service.containeroptimized;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Entry Record entity for double-entry bookkeeping
 * Maps to ledger.entry_records table
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EntryRecord {

    private UUID id;
    private UUID accountId;
    private UUID transactionId;
    private EntryType entryType;
    private long amount; // Amount in minor currency units (cents)
    private OffsetDateTime createdAt;
    private LocalDate operationDate;
    private UUID idempotencyKey;
    private String currencyCode;
    private long entryOrdinal;

    public enum EntryType {
        DEBIT, CREDIT
    }

    /**
     * Create debit entry
     */
    public static EntryRecord createDebit(UUID accountId, UUID transactionId, long amount,
                                          LocalDate operationDate, UUID idempotencyKey, String currencyCode) {
        return EntryRecord.builder()
            .accountId(accountId)
            .transactionId(transactionId)
            .entryType(EntryType.DEBIT)
            .amount(amount)
            .operationDate(operationDate)
            .idempotencyKey(idempotencyKey)
            .currencyCode(currencyCode)
            .createdAt(OffsetDateTime.now())
            .build();
    }

    /**
     * Create credit entry
     */
    public static EntryRecord createCredit(UUID accountId, UUID transactionId, long amount,
                                           LocalDate operationDate, UUID idempotencyKey, String currencyCode) {
        return EntryRecord.builder()
            .accountId(accountId)
            .transactionId(transactionId)
            .entryType(EntryType.CREDIT)
            .amount(amount)
            .operationDate(operationDate)
            .idempotencyKey(idempotencyKey)
            .currencyCode(currencyCode)
            .createdAt(OffsetDateTime.now())
            .build();
    }

    /**
     * Create storno (reversal) entry for this entry
     */
    public EntryRecord createStorno(UUID newTransactionId, UUID newIdempotencyKey) {
        return EntryRecord.builder()
            .accountId(this.accountId)
            .transactionId(newTransactionId)
            .entryType(this.entryType) // Same type but negative effect
            .amount(this.amount) // Same amount
            .operationDate(LocalDate.now()) // New operation date
            .idempotencyKey(newIdempotencyKey)
            .currencyCode(this.currencyCode)
            .createdAt(OffsetDateTime.now())
            .build();
    }

    /**
     * Create correction entry
     */
    public static EntryRecord createCorrection(UUID accountId, UUID transactionId, EntryType entryType,
                                               long amount, LocalDate operationDate, UUID idempotencyKey,
                                               String currencyCode) {
        return EntryRecord.builder()
            .accountId(accountId)
            .transactionId(transactionId)
            .entryType(entryType)
            .amount(amount)
            .operationDate(operationDate)
            .idempotencyKey(idempotencyKey)
            .currencyCode(currencyCode)
            .createdAt(OffsetDateTime.now())
            .build();
    }

    /**
     * Check if this is a debit entry
     */
    public boolean isDebit() {
        return EntryType.DEBIT.equals(entryType);
    }

    /**
     * Check if this is a credit entry
     */
    public boolean isCredit() {
        return EntryType.CREDIT.equals(entryType);
    }

    /**
     * Get signed amount (negative for debit, positive for credit)
     */
    public long getSignedAmount() {
        return isDebit() ? -amount : amount;
    }

    /**
     * Validate entry record
     */
    public boolean isValid() {
        return accountId != null
            && transactionId != null
            && entryType != null
            && amount > 0
            && operationDate != null
            && currencyCode != null && currencyCode.length() == 3;
    }
}