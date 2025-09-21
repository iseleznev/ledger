package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration.PostgreSQLConfiguration;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration.WalConfiguration;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.PostgreSqlEntryRecordBatchRingBufferHandler;

import javax.sql.DataSource;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ✅ Zero-allocation PostgreSQL processor с shared ring buffer
 * Обрабатывает готовые PostgreSQL binary batches без создания промежуточных объектов
 */
@Slf4j
public class HotAccountWalWorkersManager {

    private static final VarHandle STRIPE_WORKER_ID;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            // Для статических полей
            STRIPE_WORKER_ID = lookup.findVarHandle(
                HotAccountWalWorkersManager.class, "stripeWorkerId", int.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    //    private final PostgreSqlEntryRecordBatchRingBufferHandler entryRecordBatchRingBuffer;
    private final HotAccountWalWorker[] workers;
//    private final Arena postgresArena;
//    private final List<Thread> postgresWorkers = new ArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean(true);

    // ===== METRICS =====

    private final AtomicLong totalBatchesProcessed = new AtomicLong(0);
    private final AtomicLong totalEntriesProcessed = new AtomicLong(0);
    private final AtomicLong totalProcessingTime = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);

//    private int stripeWorkerId = 0;

    public HotAccountWalWorkersManager(
        WalConfiguration walConfiguration,
        int ringBufferSizeGB,
        int maxBatches,
        int maxEntriesPerBatch,
        int workerThreadsCount,
        int stageTablesPerWorkerCount,
        String tableNamePrefix
    ) {
        log.info("Initializing ZeroAllocationPostgresProcessor: ring_buffer={}GB, max_batches={}, max_entries_per_batch={}, workers={}",
            ringBufferSizeGB, maxBatches, maxEntriesPerBatch, workerThreadsCount);

//        this.postgresArena = Arena.ofShared();
        // ✅ Создаем огромный off-heap ring buffer
//        long ringBufferSize = (long) ringBufferSizeGB * 1024 * 1024 * 1024; // GB to bytes
//        MemorySegment ringBufferSegment = postgresArena.allocate(ringBufferSize, 64);

//        this.entryRecordBatchRingBuffer = new PostgreSqlEntryRecordBatchRingBufferHandler(
//            ringBufferSegment,
//            maxEntriesPerBatch
//        );

        this.workers = new HotAccountWalWorker[walConfiguration.workerThreadsCount()];// new EntryRecordDirectPostgresBatchSender(dataSource, this.entryRecordBatchRingBuffer);

//        directSenders(dataSource, tableNamePrefix, workerThreadsCount, stageTablesPerWorkerCount);

        initWorkers(workerThreadsCount, walConfiguration);

        log.info("ZeroAllocationPostgresProcessor initialized successfully");
    }

    public void initWorkers(
        int workerThreadsCount,
        WalConfiguration walConfiguration
    ) {
        for (int workerId = 0; workerId < workerThreadsCount; workerId++ ) {
            final HotAccountWalWorker worker = new HotAccountWalWorker(workerId, walConfiguration);
            workers[workerId] = worker;
        }
    }

    public void startWorkers() {
        for (final HotAccountWalWorker worker : workers) {
            worker.start();
        }
    }

    // ===== MONITORING =====

    public HotAccountThreadManagerMetrics getMetrics() {
        long totalBatches = totalBatchesProcessed.get();
        long totalEntries = totalEntriesProcessed.get();
        long totalTime = totalProcessingTime.get();

        double avgBatchTime = totalBatches > 0 ? (double) totalTime / totalBatches / 1_000_000 : 0; // ms
        double avgEntryTime = totalEntries > 0 ? (double) totalTime / totalEntries / 1_000 : 0; // μs

        return HotAccountThreadManagerMetrics.builder()
            .totalBatchesProcessed(totalBatches)
            .totalEntriesProcessed(totalEntries)
            .totalErrors(totalErrors.get())
            .avgBatchProcessingTimeMs(avgBatchTime)
            .avgEntryProcessingTimeMicros(avgEntryTime)
            .ringBufferUtilization(entryRecordBatchRingBuffer.getUtilization())
            .ringBufferSize(entryRecordBatchRingBuffer.size())
            .ringBufferHealthy(entryRecordBatchRingBuffer.isHealthy())
            .build();
    }

    public String getDiagnostics() {
        HotAccountThreadManagerMetrics metrics = getMetrics();

        return String.format(
            "ZeroAllocationPostgresProcessor[batches=%d, entries=%d, errors=%d, " +
                "avg_batch_time=%.1fms, ring_buffer_util=%.1f%%, healthy=%s]",
            metrics.totalBatchesProcessed(), metrics.totalEntriesProcessed(), metrics.totalErrors(),
            metrics.avgBatchProcessingTimeMs(), metrics.ringBufferUtilization(), metrics.ringBufferHealthy()
        );
    }

    // ===== LIFECYCLE =====

    //    @PreDestroy
    public void shutdown() {
        log.info("Shutting down ZeroAllocationPostgresProcessor");

        running.set(false);

        // Ждем завершения всех workers
        for (Thread worker : postgresWorkers) {
            try {
                worker.join(5000); // 5 seconds timeout
                if (worker.isAlive()) {
                    log.warn("Worker {} did not stop within timeout, interrupting", worker.getName());
                    worker.interrupt();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for worker {} to stop", worker.getName());
            }
        }

        // Закрываем Arena
        try {
            if (postgresArena.scope().isAlive()) {
                postgresArena.close();
            }
        } catch (Exception e) {
            log.error("Error closing PostgreSQL Arena", e);
        }

        log.info("ZeroAllocationPostgresProcessor shutdown completed. Final stats: {}", getDiagnostics());
    }
}