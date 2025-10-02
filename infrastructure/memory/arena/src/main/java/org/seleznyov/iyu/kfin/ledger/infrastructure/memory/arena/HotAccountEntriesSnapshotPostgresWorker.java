package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration.PostgreSQLConfiguration;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration.WalConfiguration;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.EntryRecordBatchHandler;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.PostgreSqlEntryRecordRingBufferHandler;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.prevprocessor.PostgresRingBufferProcessor;

import javax.sql.DataSource;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

@Slf4j
public class HotAccountEntriesSnapshotPostgresWorker {

    static final String STAGE_TABLE_NAME_PREFIX = "stage_entries_snapshots";

    private final PostgreSqlEntryRecordRingBufferHandler postgresRingBuffer;
    private final Map<Integer, PostgresRingBufferProcessor> stageTableProcessorsMap;
    private final PostgresEntryRecordCheckpointWriter checkpointWriter;
    private final PostgreSQLConfiguration postgreSQLConfiguration;
    private final Arena postgresArena;
    private final int workerId;
    private final Thread workerThread;
    private volatile boolean isRunning;
    private int stageTableProcessorIndex = 0;
    private long totalErrors = 0;

    public HotAccountEntriesSnapshotPostgresWorker(
        int workerId,
        DataSource dataSource,
        WalConfiguration walConfiguration,
        PostgreSQLConfiguration postgreSQLConfiguration
    ) {
        this.postgreSQLConfiguration = postgreSQLConfiguration;
        //this.entryRecordBatchRingBuffer = entryRecordBatchRingBuffer;
        this.postgresArena = Arena.ofShared();
        // ✅ Создаем огромный off-heap ring buffer
        long ringBufferSize = (long) postgreSQLConfiguration.ringBufferCopyBatchesCountCapacity()
            * postgreSQLConfiguration.directCopyBatchRecordsCount()
            * EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE;
            // ringBufferSizeGB * 1024 * 1024 * 1024; // GB to bytes
        MemorySegment ringBufferSegment = postgresArena.allocate(ringBufferSize, 64);
        this.postgresRingBuffer = new PostgreSqlEntryRecordRingBufferHandler(
            ringBufferSegment,
            postgreSQLConfiguration.directCopyBatchRecordsCount()
        );
        this.workerId = workerId;
        stageTableProcessorsMap = new HashMap<>();
        for (int stageTableIndex = 0; stageTableIndex < postgreSQLConfiguration.stageTablesPerWorkerThreadCount(); stageTableIndex++) {
            final String tableName = STAGE_TABLE_NAME_PREFIX + "_w" + workerId + "t" + stageTableIndex;
            this.stageTableProcessorsMap.put(
                stageTableIndex,
                new PostgresRingBufferProcessor(
                    new EntryRecordDirectPostgresBatchSender(dataSource, tableName, this.postgresRingBuffer),
                    workerId
                )
            );
        }
        this.checkpointWriter = new PostgresEntryRecordCheckpointWriter(
            walConfiguration.path(),
            workerId
        );
        this.workerThread = Thread.ofPlatform()
            .name("zero-alloc-entries-snapshot-postgres-worker-" + workerId)
            .priority(Thread.NORM_PRIORITY)
            .unstarted(this::entriesSnapshotBatchRingBuffer);
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

    public PostgreSqlEntryRecordRingBufferHandler postgresRingBuffer() {
        return postgresRingBuffer;
    }

    private void entriesSnapshotBatchRingBuffer() {
        log.info("Worker {} started processing zero-allocation batches", this.workerThread);

        // ✅ Создаем consumer который работает напрямую с off-heap memory
//        final PostgresBatchProcessor processor = new  PostgresBatchProcessor(directSenders.get(workerId), workerId);
//        final PostgresCheckpointWriter checkpointWriter = new PostgresCheckpointWriter();
//            (ringSegment, batchOffset) -> {
//                return processOffHeapBatch(ringSegment, batchOffset, workerId);
//            };

        final long parkNanos = postgreSQLConfiguration.waitingEmptyRingBufferNanos();
        final int retries = postgreSQLConfiguration.emptyIterationsYieldRetries();
        final int stageTableProcessorsCount = stageTableProcessorsMap.size();//postgreSQLConfiguration.stageTablesPerWorkerThreadCount();

        long emptyIterations = 0;

        while (isRunning) {
            try {
                // ✅ Пытаемся обработать батч БЕЗ copying в heap
                final PostgresRingBufferProcessor processor = stageTableProcessorsMap.get(stageTableProcessorIndex);
                final long walSequenceId = postgresRingBuffer.tryProcessBatch(processor);

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
        }

        log.info("Worker {} stopped processing batches", workerId);
    }
}
