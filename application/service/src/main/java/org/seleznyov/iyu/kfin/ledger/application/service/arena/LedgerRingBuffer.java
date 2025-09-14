package org.seleznyov.iyu.kfin.ledger.application.service.arena;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.foreign.ValueLayout.*;

// Ring buffer implementation with virtual thread optimizations
@Slf4j
@Component
public class LedgerRingBuffer {

    private static final int RING_BUFFER_SIZE = 8192; // Must be power of 2
    private final BatchData[] buffer = new BatchData[RING_BUFFER_SIZE];
    private final AtomicLong writeSequence = new AtomicLong(0);
    private final AtomicLong readSequence = new AtomicLong(0);
    private final Executor processingExecutor;
    private final NamedParameterJdbcTemplate jdbcTemplate; // Injected dependency

    public LedgerRingBuffer(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.processingExecutor = Executors.newVirtualThreadPerTaskExecutor();
        startProcessing();
    }

    public boolean tryPublish(BatchData batch) {
        long currentWrite = writeSequence.get();
        long nextWrite = currentWrite + 1;

        // Check if ring buffer is full
        if (nextWrite - readSequence.get() >= RING_BUFFER_SIZE) {
            log.warn("Ring buffer is full, rejecting batch for account {}", batch.getAccountId());
            return false;
        }

        // CAS to claim slot
        if (!writeSequence.compareAndSet(currentWrite, nextWrite)) {
            return false; // Another thread claimed this slot
        }

        int index = (int) (nextWrite & (RING_BUFFER_SIZE - 1));
        buffer[index] = batch;
        batch.setStatus(BatchData.BatchStatus.PROCESSING);

        log.debug("Published batch {} to ring buffer at position {}", batch.getBatchId(), index);
        return true;
    }

    private void startProcessing() {
        // Start multiple consumer virtual threads
        for (int i = 0; i < 4; i++) {
            processingExecutor.execute(this::processBatches);
        }
    }

    private void processBatches() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                long currentRead = readSequence.get();

                if (currentRead >= writeSequence.get()) {
                    // Use Thread.onSpinWait() for virtual threads - non-blocking hint
                    Thread.onSpinWait();
                    continue;
                }

                // Try to claim next batch to process
                long nextRead = currentRead + 1;
                if (!readSequence.compareAndSet(currentRead, nextRead)) {
                    continue; // Another consumer claimed this batch
                }

                int index = (int) (nextRead & (RING_BUFFER_SIZE - 1));
                BatchData batch = buffer[index];

                if (batch != null) {
                    processBatch(batch);
                    buffer[index] = null; // Clear slot
                }

            } catch (Exception e) {
                log.error("Error processing batch", e);
            }
        }
    }

    private void processBatch(BatchData batch) {
        try {
            log.debug("Processing batch {} with {} entries", batch.getBatchId(), batch.getEntryCount());

            List<EntryRecord> entries = deserializeBatch(batch);
            insertBatchToDatabase(entries);

            batch.setStatus(BatchData.BatchStatus.COMPLETED);
            log.debug("Completed processing batch {}", batch.getBatchId());

        } catch (Exception e) {
            batch.setStatus(BatchData.BatchStatus.FAILED);
            log.error("Failed to process batch {}", batch.getBatchId(), e);
        }
    }

    private List<EntryRecord> deserializeBatch(BatchData batch) {
        List<EntryRecord> entries = new ArrayList<>(batch.getEntryCount());
        MemorySegment batchSegment = MemorySegment.ofArray(batch.getData());

        for (int i = 0; i < batch.getEntryCount(); i++) {
            long offset = i * LedgerMemoryLayouts.ENTRY_SIZE;
            EntryRecord entry = readEntryFromMemory(batchSegment, offset);
            entries.add(entry);
        }

        return entries;
    }

    private EntryRecord readEntryFromMemory(MemorySegment segment, long offset) {
        return EntryRecord.builder()
            .id(readUuidFromMemory(segment, offset))
            .accountId(readUuidFromMemory(segment, offset + 16))
            .transactionId(readUuidFromMemory(segment, offset + 32))
            .entryType(EntryRecord.EntryType.values()[segment.get(JAVA_BYTE, offset + 48)])
            .amount(segment.get(JAVA_LONG, offset + 49))
            .createdAt(Instant.ofEpochMilli(segment.get(JAVA_LONG, offset + 57)))
            .operationDate(LocalDate.ofEpochDay(segment.get(JAVA_INT, offset + 65)))
            .idempotencyKey(readUuidFromMemory(segment, offset + 69))
            .currencyCode(readCurrencyCodeFromMemory(segment, offset + 85))
            .entryOrdinal(segment.get(JAVA_LONG, offset + 93))
            .build();
    }

    private UUID readUuidFromMemory(MemorySegment segment, long offset) {
        long mostSig = segment.get(JAVA_LONG, offset);
        long leastSig = segment.get(JAVA_LONG, offset + 8);
        return new UUID(mostSig, leastSig);
    }

    private String readCurrencyCodeFromMemory(MemorySegment segment, long offset) {
        byte[] bytes = new byte[8];
        for (int i = 0; i < 8; i++) {
            bytes[i] = segment.get(JAVA_BYTE, offset + i);
        }
        return new String(bytes, StandardCharsets.UTF_8).trim();
    }

    private void insertBatchToDatabase(List<EntryRecord> entries) {
        String sql = """
            INSERT INTO ledger.entry_records 
            (id, account_id, transaction_id, entry_type, amount, created_at, 
             operation_date, idempotency_key, currency_code, entry_ordinal)
            VALUES (:id, :accountId, :transactionId, :entryType, :amount, :createdAt, 
                    :operationDate, :idempotencyKey, :currencyCode, :entryOrdinal)
            """;

        List<Map<String, Object>> batchArgs = entries.stream()
            .map(entry -> Map.<String, Object>of(
                "id", entry.getId(),
                "accountId", entry.getAccountId(),
                "transactionId", entry.getTransactionId(),
                "entryType", entry.getEntryType().name(),
                "amount", entry.getAmount(),
                "createdAt", entry.getCreatedAt(),
                "operationDate", entry.getOperationDate(),
                "idempotencyKey", entry.getIdempotencyKey(),
                "currencyCode", entry.getCurrencyCode(),
                "entryOrdinal", entry.getEntryOrdinal()
            ))
            .toList();

        jdbcTemplate.batchUpdate(sql, batchArgs.toArray(new Map[0]));
        log.debug("Inserted batch of {} entries to database", entries.size());
    }
}