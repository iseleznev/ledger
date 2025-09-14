package org.seleznyov.iyu.kfin.ledger.application.service.arena;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

// Batch data container
@Data
public class BatchData {

    private final UUID accountId;
    private final byte[] data;
    private final int entryCount;
    private final List<EntryRecord> entries; // For tracking what was in the batch
    private final long batchId = System.nanoTime();
    private volatile BatchStatus status = BatchStatus.READY;

    public BatchData(UUID accountId, byte[] data, int entryCount, List<EntryRecord> entries) {
        this.accountId = accountId;
        this.data = data;
        this.entryCount = entryCount;
        this.entries = entries != null ? new ArrayList<>(entries) : new ArrayList<>();
    }

    public BatchData(UUID accountId, byte[] data, int entryCount) {
        this(accountId, data, entryCount, List.of());
    }

    public enum BatchStatus {
        READY, PROCESSING, COMPLETED, FAILED
    }
}
