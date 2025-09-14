package org.seleznyov.iyu.kfin.ledger.application.service.percore;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

/**
 * Transaction Entry for disk persistence
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransactionEntry {

    private long timestamp;
    private UUID accountId;
    private long delta;
    private UUID operationId;
    private long sequenceNumber;
    private int checksum;

    public static final int BINARY_SIZE = 8 + 16 + 8 + 16 + 8 + 4; // 60 bytes

    public void serializeTo(ByteBuffer buffer) {
        buffer.putLong(timestamp);
        buffer.putLong(accountId.getMostSignificantBits());
        buffer.putLong(accountId.getLeastSignificantBits());
        buffer.putLong(delta);
        buffer.putLong(operationId.getMostSignificantBits());
        buffer.putLong(operationId.getLeastSignificantBits());
        buffer.putLong(sequenceNumber);
        buffer.putInt(checksum);
    }

    public static TransactionEntry deserializeFrom(ByteBuffer buffer) {
        long timestamp = buffer.getLong();
        UUID accountId = new UUID(buffer.getLong(), buffer.getLong());
        long delta = buffer.getLong();
        UUID operationId = new UUID(buffer.getLong(), buffer.getLong());
        long sequenceNumber = buffer.getLong();
        int checksum = buffer.getInt();

        return TransactionEntry.builder()
            .timestamp(timestamp)
            .accountId(accountId)
            .delta(delta)
            .operationId(operationId)
            .sequenceNumber(sequenceNumber)
            .checksum(checksum)
            .build();
    }

    public int calculateChecksum() {
        return Objects.hash(timestamp, accountId, delta, operationId, sequenceNumber);
    }
}
