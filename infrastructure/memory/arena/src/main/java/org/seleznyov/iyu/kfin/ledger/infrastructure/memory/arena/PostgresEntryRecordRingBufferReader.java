package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.domain.model.entryrecord.EntryRecord;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration.PostgreSQLConfiguration;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration.WalConfiguration;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.EntryRecordBatchHandler;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.EntryRecordRingBufferHandler;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.prevprocessor.PostgresEntryRecordRingBufferProcessor;

import javax.sql.DataSource;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

@Slf4j
public class PostgresRingBufferProcessor implements RingBufferProcessor<EntryRecord> {

    static final String STAGE_TABLE_NAME_PREFIX = "stage_entry_records";

    private static final int POSTGRES_SIGNATURE_OFFSET = 0;             // 11 bytes "PGCOPY\n\377\r\n\0"
    private static final int POSTGRES_FLAGS_OFFSET = 11;                // 4 bytes
    private static final int POSTGRES_EXTENSION_OFFSET = 15;            // 4 bytes
    private static final int POSTGRES_HEADER_SIZE = 19;

    //    private final EntryRecordRingBufferHandler postgresRingBuffer;
    private final List<PostgresEntryRecordRingBufferProcessor> stageTableProcessors;
    private final PostgresEntryRecordCheckpointWriter checkpointWriter;
    private final PostgreSQLConfiguration postgreSQLConfiguration;
    private final Arena postgresArena;
    private final int workerId;
    private final Thread workerThread;
    private volatile boolean isRunning;
    private int stageTableProcessorIndex = 0;
    private long totalErrors = 0;
    private final EntryRecordRingBufferHandler ringBufferHandler;
    private long readOffset = 0;

    public PostgresRingBufferProcessor(
//        EntryRecordRingBufferHandler postgresRingBuffer,
        int workerId,
        DataSource dataSource,
        WalConfiguration walConfiguration,
        PostgreSQLConfiguration postgreSQLConfiguration,
        EntryRecordRingBufferHandler ringBufferHandler
    ) {
        this.ringBufferHandler  = ringBufferHandler;
        this.postgreSQLConfiguration = postgreSQLConfiguration;
        //this.entryRecordBatchRingBuffer = entryRecordBatchRingBuffer;
        this.postgresArena = Arena.ofShared();
        // ✅ Создаем огромный off-heap ring buffer
        long ringBufferSize = (long) postgreSQLConfiguration.ringBufferCopyBatchesCountCapacity()
            * postgreSQLConfiguration.directCopyBatchRecordsCount()
            * EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE;
            // ringBufferSizeGB * 1024 * 1024 * 1024; // GB to bytes
        MemorySegment ringBufferSegment = postgresArena.allocate(ringBufferSize, 64);
//        this.postgresRingBuffer = postgresRingBuffer;
        this.workerId = workerId;
        stageTableProcessors = new ArrayList<>();
        for (int stageTableIndex = 0; stageTableIndex < postgreSQLConfiguration.stageTablesPerWorkerThreadCount(); stageTableIndex++) {
            final String tableName = STAGE_TABLE_NAME_PREFIX + "_w" + workerId + "t" + stageTableIndex;
            this.stageTableProcessors.add(
                new org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.prevprocessor.PostgresRingBufferProcessor(
                    new EntryRecordDirectPostgresBatchSender(dataSource, tableName),
                    workerId
                )
            );
        }
        this.checkpointWriter = new PostgresEntryRecordCheckpointWriter(
            walConfiguration.path(),
            workerId
        );
        this.workerThread = Thread.ofPlatform()
            .name("zero-alloc-entry-record-postgres-worker-" + workerId)
            .priority(Thread.NORM_PRIORITY)
            .unstarted(this::entryRecordBatchRingBuffer);
    }

    public void start() {
        if (!isRunning) {
            isRunning = true;
            workerThread.start();
        }
    }

    public void stop() {
        isRunning = false;
    }

    public void shutdown() {
        isRunning = false;
        this.postgresArena.close();
    }

    private void entryRecordBatchRingBuffer() {
        log.info("Worker {} started processing zero-allocation batches", this.workerThread);

        // ✅ Создаем consumer который работает напрямую с off-heap memory
//        final PostgresBatchProcessor processor = new  PostgresBatchProcessor(directSenders.get(workerId), workerId);
//        final PostgresCheckpointWriter checkpointWriter = new PostgresCheckpointWriter();
//            (ringSegment, batchOffset) -> {
//                return processOffHeapBatch(ringSegment, batchOffset, workerId);
//            };

//        final long parkNanos = postgreSQLConfiguration.waitingEmptyRingBufferNanos();
        final int retries = postgreSQLConfiguration.emptyIterationsYieldRetries();
        final int stageTableProcessorsCount = stageTableProcessors.size();//postgreSQLConfiguration.stageTablesPerWorkerThreadCount();

        long emptyIterations = 0;

//        while (isRunning) {
            try {
                // ✅ Пытаемся обработать батч БЕЗ copying в heap
                final org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.prevprocessor.PostgresRingBufferProcessor processor = stageTableProcessors.get(stageTableProcessorIndex);
                final long walSequenceId = ringBufferHandler.tryProcessBatch(processor, readOffset, (long) postgreSQLConfiguration.directCopyBatchRecordsCount() * EntryRecordRingBufferHandler.POSTGRES_ENTRY_RECORD_SIZE);

                if (walSequenceId > 0) {
//                    final long walSequenceId = entryRecordBatchRingBuffer.walSequenceId(batchOffset);
                    checkpointWriter.writeCheckpoint(walSequenceId);
                    stageTableProcessorIndex++;
                    if (stageTableProcessorIndex >= stageTableProcessorsCount) {
                        stageTableProcessorIndex = 0;
                    }
                    emptyIterations = 0;
                } else {
                    // Нет данных - короткая пауза
                    emptyIterations++;
                    if (emptyIterations < 100) {
                        // Сначала пробуем yield для быстрого отклика
                        Thread.yield();
                    } else if (emptyIterations < retries) {
                        // Затем короткая пауза
                        LockSupport.parkNanos(parkNanos);
                    } else {
                        // Если долго нет данных - увеличиваем паузу
                        LockSupport.parkNanos(parkNanos * 100);
                    }
                }

            } catch (Exception e) {
                totalErrors++;
                log.error("Error in zero-allocation worker {}", workerId, e);

                // Пауза при ошибке чтобы не спамить
                LockSupport.parkNanos(10_000_000L); // 10ms
            }


        log.info("Worker {} stopped processing batches", workerId);
    }


    public long processOffset() {
        return this.readOffset;
    }

    public void processOffset(long offset) {
        this.readOffset = offset;
        VarHandle.releaseFence();
    }

    @Override
    public int beforeBatchOperationGap() {
        return POSTGRES_HEADER_SIZE;
    }
}
