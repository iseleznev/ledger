package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor.updatedv8;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Journal entry for append-only log
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JournalEntry {

    private long offset;
    private long sequenceNumber;
    private UUID debitAccountId;
    private UUID creditAccountId;
    private long amount;
    private String currencyCode;
    private LocalDate operationDate;
    private UUID transactionId;
    private UUID idempotencyKey;
    private LocalDateTime timestamp;
    private EntryStatus status;

    // Binary serialization constants
    public static final int FIXED_SIZE =
        8 + // offset
            8 + // sequenceNumber
            16 + 16 + // UUIDs (2)
            8 + // amount
            8 + // currencyCode (max 8 chars, padded)
            8 + // operationDate (epoch days + padding)
            16 + 16 + // UUIDs (2)
            8 + // timestamp (epoch millis)
            4; // status (int)
    // Total: 116 bytes per entry

    public enum EntryStatus {
        PENDING, PROCESSED, FAILED
    }

    /**
     * Serialize entry to ByteBuffer for memory-mapped storage
     */
    public void serialize(ByteBuffer buffer) {
        buffer.putLong(offset);
        buffer.putLong(sequenceNumber);

        // UUIDs as two longs each
        buffer.putLong(debitAccountId.getMostSignificantBits());
        buffer.putLong(debitAccountId.getLeastSignificantBits());
        buffer.putLong(creditAccountId.getMostSignificantBits());
        buffer.putLong(creditAccountId.getLeastSignificantBits());

        buffer.putLong(amount);

        // Currency code (padded to 8 bytes)
        byte[] currencyBytes = currencyCode.getBytes();
        buffer.put(currencyBytes);
        buffer.put(new byte[8 - currencyBytes.length]); // padding

        // Operation date as epoch days
        buffer.putLong(operationDate.toEpochDay());

        // Transaction and idempotency UUIDs
        buffer.putLong(transactionId.getMostSignificantBits());
        buffer.putLong(transactionId.getLeastSignificantBits());
        buffer.putLong(idempotencyKey.getMostSignificantBits());
        buffer.putLong(idempotencyKey.getLeastSignificantBits());

        // Proper timestamp serialization - convert to epoch millis
        long timestampMillis = timestamp.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
        buffer.putLong(timestampMillis);

        // Status as ordinal
        buffer.putInt(status.ordinal());
    }

    /**
     * Deserialize entry from ByteBuffer
     */
    public static JournalEntry deserialize(ByteBuffer buffer) {
        long offset = buffer.getLong();
        long sequenceNumber = buffer.getLong();
        UUID debitAccountId = new UUID(buffer.getLong(), buffer.getLong());
        UUID creditAccountId = new UUID(buffer.getLong(), buffer.getLong());
        long amount = buffer.getLong();
        String currencyCode = readPaddedString(buffer, 8);
        LocalDate operationDate = LocalDate.ofEpochDay(buffer.getLong());
        UUID transactionId = new UUID(buffer.getLong(), buffer.getLong());
        UUID idempotencyKey = new UUID(buffer.getLong(), buffer.getLong());

        // Proper timestamp deserialization
        long timestampMillis = buffer.getLong();
        LocalDateTime timestamp = Instant.ofEpochMilli(timestampMillis)
            .atZone(java.time.ZoneId.systemDefault())
            .toLocalDateTime();

        EntryStatus status = EntryStatus.values()[buffer.getInt()];

        return JournalEntry.builder()
            .offset(offset)
            .sequenceNumber(sequenceNumber)
            .debitAccountId(debitAccountId)
            .creditAccountId(creditAccountId)
            .amount(amount)
            .currencyCode(currencyCode)
            .operationDate(operationDate)
            .transactionId(transactionId)
            .idempotencyKey(idempotencyKey)
            .timestamp(timestamp)
            .status(status)
            .build();
    }

    private static String readPaddedString(ByteBuffer buffer, int maxLength) {
        byte[] bytes = new byte[maxLength];
        buffer.get(bytes);
        // Find actual length (remove padding)
        int actualLength = 0;
        for (int i = 0; i < maxLength; i++) {
            if (bytes[i] == 0) break;
            actualLength++;
        }
        return new String(bytes, 0, actualLength);
    }
}
