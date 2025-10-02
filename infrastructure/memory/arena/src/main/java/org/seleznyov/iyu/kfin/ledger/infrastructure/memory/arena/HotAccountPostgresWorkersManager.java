package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration.PostgreSQLConfiguration;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration.WalConfiguration;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.PostgreSqlEntryRecordRingBufferHandler;

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
public class HotAccountPostgresWorkersManager {

    private static final VarHandle STRIPE_WORKER_ID;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            // Для статических полей
            STRIPE_WORKER_ID = lookup.findVarHandle(
                HotAccountPostgresWorkersManager.class, "stripeWorkerId", int.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    //    private final PostgreSqlEntryRecordBatchRingBufferHandler entryRecordBatchRingBuffer;
    private final HotAccountPostgresWorker[] workers;
//    private final Arena postgresArena;
//    private final List<Thread> postgresWorkers = new ArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean(true);

    // ===== METRICS =====

    private final AtomicLong totalBatchesProcessed = new AtomicLong(0);
    private final AtomicLong totalEntriesProcessed = new AtomicLong(0);
    private final AtomicLong totalProcessingTime = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);

    private int stripeWorkerId = 0;

    public HotAccountPostgresWorkersManager(
        WalConfiguration walConfiguration,
        PostgreSQLConfiguration postgreSQLConfiguration,
        DataSource dataSource,
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

        this.workers = new HotAccountPostgresWorker[postgreSQLConfiguration.workerThreadsCount()];// new EntryRecordDirectPostgresBatchSender(dataSource, this.entryRecordBatchRingBuffer);

//        directSenders(dataSource, tableNamePrefix, workerThreadsCount, stageTablesPerWorkerCount);

        initWorkers(workerThreadsCount, dataSource, walConfiguration, postgreSQLConfiguration);

        log.info("ZeroAllocationPostgresProcessor initialized successfully");
    }

    public void initWorkers(
        int workerThreadsCount,
        DataSource dataSource,
        WalConfiguration walConfiguration,
        PostgreSQLConfiguration postgreSQLConfiguration
    ) {
        for (int workerId = 0; workerId < workerThreadsCount; workerId++ ) {
            final HotAccountPostgresWorker worker = new HotAccountPostgresWorker(
                workerId,
                dataSource,
                walConfiguration,
                postgreSQLConfiguration
            );
            workers[workerId] = worker;
        }
    }

    public void startWorkers() {
        for (Integer workerId : workers.keySet()) {
            final HotAccountPostgresWorker worker = workers.get(workerId);
            worker.start();
        }
    }

    public PostgreSqlEntryRecordRingBufferHandler nextRingBufferHandler() {
        final int stripeWorkerId = (int) STRIPE_WORKER_ID.getAndAddAcquire(this, 1);

        final int workerId;
//        if (isPowerOfTwo(this.workersMap.length)) {
//            // Явная битовая маска для ясности намерений
//            workerId = stripeWorkerId & (this.workersMap.length - 1);
//        } else {
//            // Обычный modulo
            workerId = Math.abs(stripeWorkerId) % this.workers.length;
//        }

        return this.workers[workerId].postgresRingBuffer();
//
//        STRIPE_WORKER_ID.compareAndSet(this, this.workersMap.length, 0);
//        return this.workersMap[stripeWorkerId].postgresRingBuffer();
    }

//    private static boolean isPowerOfTwo(int n) {
//        return n > 0 && (n & (n - 1)) == 0;
//    }

//    private void directSenders(DataSource dataSource, String tableNamePrefix, int workerThreadsCount, int stageTablesPerWorkerCount) {
//        for (int i = 0; i < workerThreadsCount; i++) {
//            final int workerId = i;
//            for (int j = 0; j < stageTablesPerWorkerCount; j++) {
//                final String tableName = tableNamePrefix + "_w" + workerId + "t" + j;
//                this.directSenders.put(
//                    workerId,
//                    new EntryRecordDirectPostgresBatchSender(dataSource, tableName, this.entryRecordBatchRingBuffer)
//                );
//            }
//        }
//    }

//    private void startWorkers(int workerThreadsCount) {
//        for (int i = 0; i < workerThreadsCount; i++) {
//            final int workerId = i;
//
////            Thread worker = Thread.ofVirtual()
////                .name("zero-alloc-postgres-worker-" + workerId)
////                .start(() -> processRingBuffer(workerId));
//            Thread worker = Thread.ofPlatform()
//                .name("zero-alloc-postgres-worker-" + workerId)
//                .priority(Thread.NORM_PRIORITY)
//                .start(() -> processRingBuffer(workerId));
//
//            postgresWorkers.add(worker);
//        }
//
//        log.info("Started {} zero-allocation PostgreSQL workers", workerThreadsCount);
//    }

    /**
     * ✅ Worker процесс БЕЗ heap allocations
     */
//    private void processRingBuffer(int workerId) {
//        log.info("Worker {} started processing zero-allocation batches", workerId);
//
//        // ✅ Создаем consumer который работает напрямую с off-heap memory
//        final PostgresBatchProcessor processor = new  PostgresBatchProcessor(directSenders.get(workerId), workerId);
//        final PostgresCheckpointWriter checkpointWriter = new PostgresCheckpointWriter();
////            (ringSegment, batchOffset) -> {
////                return processOffHeapBatch(ringSegment, batchOffset, workerId);
////            };
//
//        while (running.get()) {
//            try {
//                // ✅ Пытаемся обработать батч БЕЗ copying в heap
//                final long walSequenceId = entryRecordBatchRingBuffer.tryProcessBatch(processor);
//
//                if (walSequenceId > 0) {
////                    final long walSequenceId = entryRecordBatchRingBuffer.walSequenceId(batchOffset);
//                    checkpointWriter.writeCheckpoint(walSequenceId);
//                }
//
//                if (walSequenceId == 0) {
//                    // Нет данных - короткая пауза
//                    Thread.sleep(1);
////                    Thread.yield();
//                    //LockSupport.parkNanos(1_000_000L); // 1ms
//                }
//
//            } catch (Exception e) {
//                totalErrors.incrementAndGet();
//                log.error("Error in zero-allocation worker {}", workerId, e);
//
//                // Пауза при ошибке чтобы не спамить
//                Thread.sleep(10);
//                //LockSupport.parkNanos(10_000_000L); // 10ms
//            }
//        }
//
//        log.info("Worker {} stopped processing batches", workerId);
//    }




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