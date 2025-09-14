package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.PostgreSqlEntryRecordBatchRingBufferHandler;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.processor.batchprocessor.PostgresBatchProcessor;
import org.springframework.beans.factory.annotation.Value;

import javax.sql.DataSource;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * ✅ Zero-allocation PostgreSQL processor с shared ring buffer
 * Обрабатывает готовые PostgreSQL binary batches без создания промежуточных объектов
 */
@Slf4j
public class HotAccountPostgresWorkersManager {

    private final PostgreSqlEntryRecordBatchRingBufferHandler entryRecordBatchRingBuffer;
    private final DirectPostgresBatchSender<PostgreSqlEntryRecordBatchRingBufferHandler> directSender;
    private final Arena postgresArena;
    private final List<Thread> postgresWorkers = new ArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean(true);

    // ===== METRICS =====

    private final AtomicLong totalBatchesProcessed = new AtomicLong(0);
    private final AtomicLong totalEntriesProcessed = new AtomicLong(0);
    private final AtomicLong totalProcessingTime = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);

    public HotAccountPostgresWorkersManager(
        DataSource dataSource,
        @Value("${ledger.postgres.ring-buffer-size-gb:1}") int ringBufferSizeGB,
        @Value("${ledger.postgres.max-batches:8192}") int maxBatches,
        @Value("${ledger.postgres.max-entries-per-batch:1000}") int maxEntriesPerBatch,
        @Value("${ledger.postgres.worker-threads:4}") int workerThreads) {

        log.info("Initializing ZeroAllocationPostgresProcessor: ring_buffer={}GB, max_batches={}, max_entries_per_batch={}, workers={}",
            ringBufferSizeGB, maxBatches, maxEntriesPerBatch, workerThreads);

        this.postgresArena = Arena.ofShared();
        // ✅ Создаем огромный off-heap ring buffer
        long ringBufferSize = (long) ringBufferSizeGB * 1024 * 1024 * 1024; // GB to bytes
        MemorySegment ringBufferSegment = postgresArena.allocate(ringBufferSize, 64);

        this.entryRecordBatchRingBuffer = new PostgreSqlEntryRecordBatchRingBufferHandler(
            ringBufferSegment,
            maxEntriesPerBatch
        );

        this.directSender = new EntryRecordDirectPostgresBatchSender(dataSource, this.entryRecordBatchRingBuffer);

        startWorkers(workerThreads);

        log.info("ZeroAllocationPostgresProcessor initialized successfully");
    }

    private void startWorkers(int workerCount) {
        for (int i = 0; i < workerCount; i++) {
            final int workerId = i;

            Thread worker = Thread.ofVirtual()
                .name("zero-alloc-postgres-worker-" + workerId)
                .start(() -> processRingBuffer(workerId));

            postgresWorkers.add(worker);
        }

        log.info("Started {} zero-allocation PostgreSQL workers", workerCount);
    }

    /**
     * ✅ Worker процесс БЕЗ heap allocations
     */
    private void processRingBuffer(int workerId) {
        log.info("Worker {} started processing zero-allocation batches", workerId);

        // ✅ Создаем consumer который работает напрямую с off-heap memory
        final PostgresBatchProcessor processor = new  PostgresBatchProcessor(directSender, workerId);
//            (ringSegment, batchOffset) -> {
//                return processOffHeapBatch(ringSegment, batchOffset, workerId);
//            };

        while (running.get()) {
            try {
                // ✅ Пытаемся обработать батч БЕЗ copying в heap
                boolean processed = entryRecordBatchRingBuffer.tryProcessBatch(processor);

                if (!processed) {
                    // Нет данных - короткая пауза
                    LockSupport.parkNanos(1_000_000L); // 1ms
                }

            } catch (Exception e) {
                totalErrors.incrementAndGet();
                log.error("Error in zero-allocation worker {}", workerId, e);

                // Пауза при ошибке чтобы не спамить
                LockSupport.parkNanos(10_000_000L); // 10ms
            }
        }

        log.info("Worker {} stopped processing batches", workerId);
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