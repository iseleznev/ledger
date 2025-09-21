package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.domain.model.entryrecord.EntryRecord;
import org.seleznyov.iyu.kfin.ledger.domain.model.entryrecord.EntryType;
import org.seleznyov.iyu.kfin.ledger.domain.model.snapshot.EntriesSnapshot;
import org.seleznyov.iyu.kfin.ledger.domain.model.snapshot.EntriesSnapshotReasonType;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration.LedgerConfiguration;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.*;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * HotAccountContext writes entry-record into batch and spreads to ring buffers when batch is full or idle for too long.
 */
@Slf4j
public class HotAccountEntryRecordWorker {

    // Account configuration
    private static final int MAX_BATCH_ELEMENTS_COUNT = 1000;
    private static final long IDLE_TIMEOUT_MS = 30 * 60 * 1000L; // 30 minutes

    // Snapshot triggers
    private static final long SNAPSHOT_TIME_INTERVAL_MS = 30_000L; // 30 seconds
    private static final int SNAPSHOT_COUNT_THRESHOLD = 50; // entries

    // === CORE FIELDS ===

    //    private final UUID accountId;
    private final EntriesSnapshotSharedBatchHandler snapshotSharedBatchHandler;
    private final PostgreSqlEntriesSnapshotBatchRingBufferHandler snapshotBatchRingBufferHandler;

    // === MEMORY MANAGEMENT ===

    //    private final Arena accountArena;
    private final EntryRecordBatchHandler entryRecordBatchHandler;
    private final HotAccountPostgresWorkersManager postgresWorkersManager;
    private final WalEntryRecordBatchRingBufferHandler walEntryRecordBatchRingBufferHandler;
//    private final WalEntryRecordBatchRingBufferHandler walEntryRecordBatchRingBufferHandler;
//    private final MemorySegment amountArraySegment;

    // === THREADING ===

    private volatile boolean shutdownRequested = false;

    // === STATE (только dedicated поток) ===

    //    private final long maxTotalBatchSize;
    private int currentBatchSize = 0;
    private long snapshotBalance = 0L;
    private long snapshotSequence = 0L;
    private volatile EntriesSnapshot lastSnapshot = null;
    private volatile long lastAccessTime = System.currentTimeMillis();

    private final ExecutorService executorService;

    // === SNAPSHOT MANAGEMENT ===

    //    private final SnapshotArenaManager snapshotManager;
    private long lastSnapshotTime = System.currentTimeMillis();
    private int entriesSinceLastSnapshot = 0;
    private final LedgerConfiguration ledgerConfiguration;


    // === METRICS ===

    private final AtomicLong totalEntriesProcessed = new AtomicLong(0);
    private final AtomicLong totalBatchesFlushed = new AtomicLong(0);
    private final AtomicLong totalFlushTime = new AtomicLong(0);
    private final AtomicLong totalSnapshotsCreated = new AtomicLong(0);
    private final AtomicLong totalProcessingErrors = new AtomicLong(0);
    private final Arena batchArena;

    // === CONSTRUCTOR ===

    public HotAccountEntryRecordWorker(
        int workerId,
        long walSequenceId,
        LedgerConfiguration ledgerConfiguration,
//        EntryRecordBatchHandler entryRecordBatchHandler,
//        ExecutorService shardDedicatedExecutorService,
        HotAccountPostgresWorkersManager postgresWorkersManager,
        EntriesSnapshotSharedBatchHandler snapshotSharedBatchHandler,
        PostgreSqlEntriesSnapshotBatchRingBufferHandler snapshotBatchRingBufferHandler,
        WalEntryRecordBatchRingBufferHandler walEntryRecordBatchRingBufferHandler
    ) {
//        this.accountId = configuration.accountId();

//        this.accountArena = Arena.ofShared();

//        final long entriesSnapshotMemorySize = (long) MAX_BATCH_ELEMENTS_COUNT * EntriesSnapshotSharedBatchHandler.POSTGRES_ENTRIES_SNAPSHOT_SIZE;
//        final MemorySegment snapshotsSegment = accountArena.allocate(entriesSnapshotMemorySize, 64);

        final int currentWorkerIndex = workerId; // Для lambda

        final BlockingQueue<Runnable> boundedQueue = new ArrayBlockingQueue<>(ledgerConfiguration.entries().workerBoundedQueueSize());

        final ThreadFactory threadFactory = runnable -> {
            Thread thread = Thread.ofPlatform()
                .name("account-stripe-" + currentWorkerIndex)
                .priority(Thread.NORM_PRIORITY + 1) // Повышенный приоритет
                .unstarted(runnable);

            // Настройки для финансового приложения
            thread.setUncaughtExceptionHandler(
                exceptionHandler.createHandler(currentWorkerIndex)
            );

            return thread;
        };

        StripeQueueRejectionHandler rejectionHandler =
            new StripeQueueRejectionHandler(currentWorkerIndex, alertingService, stripeMetrics);

        // ThreadPoolExecutor с bounded queue
        this.executorService = new ThreadPoolExecutor(
            1, 1,                          // Ровно один поток
            0L, TimeUnit.MILLISECONDS,     // Без keep-alive
            boundedQueue,                  // Bounded queue на 1000 элементов
            threadFactory,                 // Ваша ThreadFactory
            rejectionHandler              // Custom rejection handler
        );


        this.snapshotSharedBatchHandler = snapshotSharedBatchHandler;
        this.snapshotBatchRingBufferHandler = snapshotBatchRingBufferHandler;
        this.walEntryRecordBatchRingBufferHandler = walEntryRecordBatchRingBufferHandler;
        this.ledgerConfiguration = ledgerConfiguration;
//            new EntriesSnapshotBatchRingBufferHandler(
//            snapshotsSegment,
//            MAX_BATCH_ELEMENTS_COUNT
//        );

//        log.info("Creating CompleteHotAccountContext for account {}", accountId);

        // Initialize memory management
//        initializeAccountMemory(this.accountArena);
//        final long entryMemorySize = (long) MAX_BATCH_ELEMENTS_COUNT * EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE;
//        final MemorySegment entriesSegment = accountArena.allocate(entryMemorySize, 64);

        // Create PostgreSQL binary layout
        final Arena batchArena = Arena.ofShared();
        this.batchArena = batchArena;
        final MemorySegment memorySegment = batchArena.allocate((long) ledgerConfiguration.entries().batchEntriesCount() * EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE, 64);
        this.entryRecordBatchHandler = new EntryRecordBatchHandler(
            memorySegment,
            walSequenceId,
            ledgerConfiguration
        );
            //entryRecordBatchHandler;
        this.postgresWorkersManager = postgresWorkersManager;
//            new EntryRecordBatchHandler(
//            entriesSegment,
//            configuration.accountId(),
//            configuration.totalAmount(),
//            LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli()
//        );

//        this.maxTotalBatchSize = configuration.bufferEntriesMaxCount() * EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE;

        // Initialize snapshot manager
//        this.snapshotManager = new SnapshotArenaManager(
//            accountId,
//            snapshotBatchRingBufferHandler,
//            SNAPSHOT_TIME_INTERVAL_MS,
//            SNAPSHOT_COUNT_THRESHOLD
//        );

//        log.info("CompleteHotAccountContext created for account {}", accountId);
    }

    // === INITIALIZATION ===

//    private ExecutorService createDedicatedExecutor() {
//        return Executors.newSingleThreadExecutor(runnable ->
//            Thread.ofVirtual()
//                .name("hot-account-" + accountId.toString().substring(0, 8))
//                .unstarted(runnable)
//        );
//    }

    // === PUBLIC API ===

    public CompletableFuture<Boolean> entryRecord(EntryRecord entryRecord) {
        if (shutdownRequested) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("Account context is shutting down: " + entryRecord.accountId())
            );
        }

        return CompletableFuture.supplyAsync(() -> {
            final EntryRecord registerEntryRecord = entryRecord;
            lastAccessTime = System.currentTimeMillis();

            try {
                // Add entry в PostgreSQL binary format
                if (EntryType.DEBIT.equals(entryRecord.entryType())
                    && entryRecordBatchHandler.totalAmount(registerEntryRecord.accountId()) - entryRecord.amount() < 0
                ) {
                    throw new IllegalStateException("Not enough money");
                }
                final boolean barrierPassed = batchEntryRecord(entryRecord);

                // Check for snapshot creation
//                checkAndCreateSnapshots();

                // If barrier пройден - отправляем в shared ring buffer
                if (barrierPassed) {
                    log.debug("Barrier passed for account {}, flushing batch with {} entries",
                        entryRecord.accountId(), currentBatchSize);
                    flushToEntryRecordRingBuffer();

                    // Create barrier snapshot
//                    createBarrierSnapshot();
                }

                log.trace("Added entry to account {}: batch_size={}, amount={}",
                    entryRecord.accountId(), currentBatchSize, entryRecord.amount());

                if (entryRecordBatchHandler.entriesCount(entryRecord.accountId()) >= ledgerConfiguration.entries().entryRecordsCountToSnapshot()) {
                    snapshot(LocalDate.now(), entryRecord.accountId());
                    entryRecordBatchHandler.resetEntriesCount(entryRecord.accountId());
                }

                return true;

            } catch (Exception exception) {
                totalProcessingErrors.incrementAndGet();
                log.error("Error processing entry for account {} in dedicated thread", entryRecord.accountId(), exception);
                throw new RuntimeException("Entry processing failed", exception);
            }

        }, executorService);
    }

    public CompletableFuture<Long> currentBalance(UUID accountId) {
        if (shutdownRequested) {
            return CompletableFuture.completedFuture(snapshotBalance);
        }

        return CompletableFuture.supplyAsync(() -> {

            try {
                long currentBalance = entryRecordBatchHandler.totalAmount(accountId); //calculateCurrentBatchDelta();
//                long currentBalance = snapshotBalance + batchDelta;

//                log.trace("Calculated current balance for account {}: snapshot={}, delta={}, total={}",
//                    accountId, snapshotBalance, batchDelta, currentBalance);
                if (log.isTraceEnabled()) {
                    log.trace("Current balances for account {}: snapshotBalance={}, collectedBatchBalance={}, totalBalance={} (should be equals to snapshotBalance + collectedBatchBalance) ",
                        accountId, lastSnapshot.balance(), entryRecordBatchHandler.passedBarrierBatchAmount(), currentBalance);
                }

                return currentBalance;

            } catch (Exception exception) {
                log.error("Error calculating balance for account {}", accountId, exception);
                return snapshotBalance;
            }

        }, executorService);
    }

//    public CompletableFuture<Void> flushBatch() {
//        if (shutdownRequested) {
//            return CompletableFuture.completedFuture(null);
//        }
//
//        return CompletableFuture.runAsync(() -> {
//
//            if (currentBatchSize > 0) {
//                log.debug("Manual flush requested for account {}: size={}", accountId, currentBatchSize);
//                flushToEntryRecordRingBuffer();
//                createManualSnapshot();
//            }
//
//        }, dedicatedExecutor);
//    }

    public CompletableFuture<Void> asyncSnapshot(LocalDate operationDay, UUID accountId) {
        return CompletableFuture.runAsync(() -> snapshot(operationDay, accountId), executorService);
    }

    private void snapshot(LocalDate operationDay, UUID accountId) {
        long currentBalance = entryRecordBatchHandler.totalAmount(accountId);
        final EntriesSnapshot newSnapshot = createSnapshotInternal(
            accountId,
            currentBalance,
            operationDay,
            entryRecordBatchHandler.entryRecordId(),
            entryRecordBatchHandler.entryOrdinal(),
            lastSnapshot
        );
        entryRecordBatchHandler.resetOperationsCount();
        this.lastSnapshot = newSnapshot;
    }

    // === INTERNAL METHODS ===

    /**
     * Add entry напрямую в PostgreSQL binary format
     */
    private boolean batchEntryRecord(EntryRecord entry) {
        // Write в PostgreSQL binary format используя layout
        boolean barrierPassed = entryRecordBatchHandler.stampEntryRecordForwardPassBarrier(entry);

        // Write amount для scalar summation
//        long amountOffset = currentBatchSize * Long.BYTES;
//        amountArraySegment.set(ValueLayout.JAVA_LONG, amountOffset, entry.amount());

        currentBatchSize++;
        totalEntriesProcessed.incrementAndGet();
        entriesSinceLastSnapshot++;

        return barrierPassed;
    }

    /**
     * Flush в shared ring buffer БЕЗ creating промежуточных objects
     */
    private void flushToEntryRecordRingBuffer() {
        if (currentBatchSize == 0) {
//            log.trace("No entries to flush for account {}", accountId);
            return;
        }

        long startTime = System.nanoTime();

//        log.debug("Flushing {} entries from account {} to shared ring buffer",
//            currentBatchSize, accountId);

        try {
            // Calculate total delta scalar
//            long totalDelta = calculateCurrentBatchDelta();
            final long entriesOffset = entryRecordBatchHandler.offset();
            final long arenaHalfSize = entryRecordBatchHandler.arenaSize() >> 1;
            final boolean pgBarrierPassed = postgresWorkersManager.nextRingBufferHandler().stampBatchForwardPassBarrier(
//                accountId,
                entryRecordBatchHandler,
                entriesOffset < arenaHalfSize
                    ? arenaHalfSize
                    : 0,
                (int) (entriesOffset < arenaHalfSize
                    ? (entryRecordBatchHandler.arenaSize() - arenaHalfSize) / EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE
                    : arenaHalfSize / EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE),
                entryRecordBatchHandler.passedBarrierBatchAmount()
            );
            final boolean walBarrierPassed = walEntryRecordBatchRingBufferHandler.stampBatchForwardPassBarrier(

/*
        long walShardSequenceId,
        EntryRecordBatchHandler entryRecordBatchHandler,
        long entriesOffset,
        int entriesCount,
        long totalAmount

 */
                entryRecordBatchHandler.walShardSequenceId(),
//                accountId,
                entryRecordBatchHandler,
//                entriesOffset < arenaHalfSize
//                    ? arenaHalfSize
//                    : 0,
//                (int) (entriesOffset < arenaHalfSize
//                    ? (entryRecordBatchHandler.arenaSize() - arenaHalfSize) / EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE
//                    : arenaHalfSize / EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE),
                entryRecordBatchHandler.passedBarrierOffset(),
                entryRecordBatchHandler.barrierPassedEntriesCount()
//                entryRecordBatchHandler.passedBarrierBatchAmount()
            );
            final long walSequenceId = walEntryRecordBatchRingBufferHandler.nextWalSequenceId();
            entryRecordBatchHandler.walShardSequenceId(walSequenceId);
            // Send напрямую в shared ring buffer БЕЗ промежуточных objects

//            if (sent) {
//                // Successful send - update state
//                updateAccountStateAfterFlush(totalDelta);
//                clearAccountMemory();
//
//                long flushTime = System.nanoTime() - startTime;
//                totalBatchesFlushed.incrementAndGet();
//                totalFlushTime.addAndGet(flushTime);
//
//                log.debug("Successfully flushed account {} batch: {} entries, delta={}, time={}μs",
//                    accountId, currentBatchSize, totalDelta, flushTime / 1_000);
//            } else {
//                long flushTime = System.nanoTime() - startTime;
//                log.error("Failed to flush account {} batch after {}μs: shared ring buffer full",
//                    accountId, flushTime / 1_000);
//
//                throw new RuntimeException("Shared ring buffer full, cannot flush batch");
//            }

        } catch (Exception exception) {
            long flushTime = System.nanoTime() - startTime;
//            log.error("Error flushing account {} batch after {}μs",
//                accountId, flushTime / 1_000, e);
            throw new RuntimeException("Batch flush failed", exception);
        }
    }

    /**
     * Scalar calculation delta (без SIMD для compatibility)
     */
//    private long calculateCurrentBatchDelta() {
//        long sum = 0;
//
//        for (int i = 0; i < currentBatchSize; i++) {
//            long amountOffset = i * Long.BYTES;
//            sum += amountArraySegment.get(ValueLayout.JAVA_LONG, amountOffset);
//        }
//
//        log.trace("Calculated batch delta for account {}: {} from {} entries",
//            accountId, sum, currentBatchSize);
//
//        return sum;
//    }

//    private void updateAccountStateAfterFlush(long batchDelta) {
//        snapshotBalance += batchDelta;
//        snapshotSequence += currentBatchSize;
//
//        log.trace("Updated account {} state: new_balance={}, new_sequence={}",
//            accountId, snapshotBalance, snapshotSequence);
//    }
//
//    private void clearAccountMemory() {
//        currentBatchSize = 0;
//
//        // Reset layout offset
//        entryRecordBatchHandler.resetOffset();
//
//        // Clear amount array
//        amountArraySegment.fill((byte) 0);
//    }

    // === SNAPSHOT MANAGEMENT ===

//    private void checkAndCreateSnapshots() {
//        long currentTime = System.currentTimeMillis();
//        long currentBalance = snapshotBalance + calculateCurrentBatchDelta();
//
//        // Check time-based trigger
//        if (currentTime - lastSnapshotTime >= SNAPSHOT_TIME_INTERVAL_MS) {
//            createSnapshotInternal(currentBalance, SnapshotSolution.SnapshotTrigger.TIME_BASED);
//            return;
//        }
//
//        // Check count-based trigger
//        if (entriesSinceLastSnapshot >= SNAPSHOT_COUNT_THRESHOLD) {
//            createSnapshotInternal(currentBalance, SnapshotSolution.SnapshotTrigger.COUNT_BASED);
//        }
//    }

//    private void createBarrierSnapshot() {
//        long currentBalance = snapshotBalance + calculateCurrentBatchDelta();
//        createSnapshotInternal(currentBalance, SnapshotSolution.SnapshotTrigger.BARRIER_BASED);
//    }
//
//    private void createManualSnapshot() {
//        long currentBalance = snapshotBalance + calculateCurrentBatchDelta();
//        createSnapshotInternal(currentBalance, SnapshotSolution.SnapshotTrigger.MANUAL);
//    }
//
    private EntriesSnapshot createSnapshotInternal(
        UUID accountId,
        long balance,
        LocalDate operationDay,
        UUID entryRecordId,
        long entryOrdinal,
        EntriesSnapshot previousSnapshot
    ) {
        try {
            long currentTime = LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli();
            long durationSinceLastSnapshot = previousSnapshot != null
                ? currentTime - previousSnapshot.createdAt().toEpochMilli()
                : 0;

            final EntriesSnapshot snapshot = new EntriesSnapshot(
                UUID.randomUUID(),
                accountId,
                operationDay,
                balance,
                entryRecordId,
                entryOrdinal,
                entriesSinceLastSnapshot,
                EntriesSnapshotReasonType.COUNT_BASED,
                LocalDateTime.now().toInstant(ZoneOffset.UTC),
                durationSinceLastSnapshot
            );

            // Add to snapshot batch
            final long barrierPassedOffset = snapshotSharedBatchHandler.stampSnapshotForwardPassBarrier(snapshot);
//            boolean added = snapshotBatchRingBufferHandler.stampBatchForwardPassBarrier(accountId, entryRecordBatchHandler, entryRecordOffset, snapshot);

            if (barrierPassedOffset == 0 || barrierPassedOffset < 0) {
                // Update snapshot state
                final boolean barrierPassed = snapshotBatchRingBufferHandler.stampBatchForwardPassBarrier(
                    accountId,
                    snapshotSharedBatchHandler,
                    barrierPassedOffset == 0
                        ? snapshotSharedBatchHandler.arenaSize() >> 1
                        : 0,
                    0,
                    0
                );
                lastSnapshotTime = currentTime;
                entriesSinceLastSnapshot = 0;
                totalSnapshotsCreated.incrementAndGet();

//                log.debug("Created {} snapshot for account {}: balance={}, entries={}, duration={}ms",
//                    trigger, accountId, balance, snapshot.entryCount(), durationSinceLastSnapshot);
//            } else {
//                log.warn("Failed to add snapshot to batch for account {}: batch full", accountId);
            }

            return snapshot;

        } catch (Exception e) {
            log.error("Error creating snapshot for account {}", accountId, e);
            throw new RuntimeException("Snapshot creation failed", e);
        }
    }

    // === UTILITY METHODS ===

    public boolean isIdle(long idleThresholdMs) {
        return System.currentTimeMillis() - lastAccessTime > idleThresholdMs;
    }

    public boolean isIdle() {
        return isIdle(IDLE_TIMEOUT_MS);
    }

    public HotAccountMetrics getMetrics() {
        long avgFlushTime = totalBatchesFlushed.get() > 0 ?
            totalFlushTime.get() / totalBatchesFlushed.get() / 1_000 : 0; // μs

//        final EntriesSnapshot lastSnapshot = snapshotManager.getLastSnapshot();

        return HotAccountMetrics.builder()
            .currentBatchSize(currentBatchSize)
            .snapshotBalance(snapshotBalance)
            .snapshotSequence(snapshotSequence)
            .totalEntriesProcessed(totalEntriesProcessed.get())
            .totalBatchesFlushed(totalBatchesFlushed.get())
            .totalSnapshotsCreated(totalSnapshotsCreated.get())
            .totalProcessingErrors(totalProcessingErrors.get())
            .avgFlushTimeMicros(avgFlushTime)
            .lastAccessTime(lastAccessTime)
            .lastSnapshotTime(lastSnapshotTime)
            .entriesSinceLastSnapshot(entriesSinceLastSnapshot)
            .isIdle(isIdle())
            .memoryUtilization(calculateMemoryUtilization())
            .lastSnapshot(lastSnapshot)
            .build();
    }

    private double calculateMemoryUtilization() {
        long totalMemory = entryRecordBatchHandler.arenaSize();
        long usedMemory = entryRecordBatchHandler.offset();
        return (double) usedMemory / totalMemory * 100.0;
    }

    public String getDiagnostics() {
        HotAccountMetrics metrics = getMetrics();
        return String.format(
            "CompleteHotAccountContext[batch_size=%d, balance=%d, " +
                "entries=%d, batches=%d, snapshots=%d, errors=%d, idle=%s]",
            metrics.currentBatchSize, metrics.snapshotBalance,
            metrics.totalEntriesProcessed, metrics.totalBatchesFlushed,
            metrics.totalSnapshotsCreated, metrics.totalProcessingErrors, metrics.isIdle
        );
    }

    // === LIFECYCLE MANAGEMENT ===

    public void shutdown() {
//        log.info("Shutting down CompleteHotAccountContext for account {}", accountId);

        shutdownRequested = true;

        CompletableFuture<Void> finalFlush = CompletableFuture.runAsync(() -> {
            if (currentBatchSize > 0) {
//                log.info("Final flush for account {} during shutdown: {} entries",
//                    accountId, currentBatchSize);
                flushToEntryRecordRingBuffer();
            }

            // Create final snapshot
            //TODO: здесь нужно прочитать весь батч и по каждому аккаунту батча сделать снапшот
            while (entryRecordBatchHandler.offset() > 0)
            snapshot(entryRecordBatchHandler.operationDay());

            final long entriesOffset = entryRecordBatchHandler.offset();
            final long arenaHalfSize = entryRecordBatchHandler.arenaSize() >> 1;
            final long startOffset = entriesOffset < arenaHalfSize
                ? arenaHalfSize
                : 0;
            final long tailSnapshotSize = entriesOffset - startOffset;
            if (ledgerConfiguration.entries().entryRecordsCountToSnapshot() > entriesOffset - startOffset) {
                final long beforeStartOffset = startOffset == 0
                    ? entryRecordBatchHandler.arenaSize() - (ledgerConfiguration.entries().entryRecordsCountToSnapshot() - tailSnapshotSize)
                    : arenaHalfSize - (ledgerConfiguration.entries().entryRecordsCountToSnapshot() - tailSnapshotSize);

            }
            final boolean pgBarrierPassed = postgresWorkersManager.stampBatchForwardPassBarrier(
//                accountId,
                entryRecordBatchHandler,
                entriesOffset < arenaHalfSize
                    ? arenaHalfSize
                    : 0,
                (int) (entriesOffset < arenaHalfSize
                    ? (entryRecordBatchHandler.arenaSize() - arenaHalfSize) / EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE
                    : arenaHalfSize / EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE),
                entryRecordBatchHandler.passedBarrierBatchAmount()
            );



            createSnapshotInternal(
                entryRecordBatchHandler.totalAmount(),
                entryRecordBatchHandler.operationDay(),
                entryRecordBatchHandler.entryRecordId(),
                entryRecordBatchHandler.entryOrdinal(),
                lastSnapshot
            );

        }, executorService);

        try {
            finalFlush.get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
//            log.error("Error during final flush for account {}", accountId, e);
        }

        executorService.shutdown();

        try {
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }

        try {
//            if (accountArena.scope().isAlive()) {
//                accountArena.close();
//            }
        } catch (Exception e) {
//            log.error("Error closing Arena for account {}", accountId, e);
        }

//        log.info("CompleteHotAccountContext shutdown completed for account {}. Final metrics: {}",
//            accountId, getDiagnostics());
    }

    // === METRICS CLASS ===

    public static class HotAccountMetrics {

        public final int currentBatchSize;
        public final long snapshotBalance;
        public final long snapshotSequence;
        public final long totalEntriesProcessed;
        public final long totalBatchesFlushed;
        public final long totalSnapshotsCreated;
        public final long totalProcessingErrors;
        public final long avgFlushTimeMicros;
        public final long lastAccessTime;
        public final long lastSnapshotTime;
        public final int entriesSinceLastSnapshot;
        public final boolean isIdle;
        public final double memoryUtilization;
        public final EntriesSnapshot lastSnapshot;

        private HotAccountMetrics(int currentBatchSize, long snapshotBalance,
                                  long snapshotSequence, long totalEntriesProcessed, long totalBatchesFlushed,
                                  long totalSnapshotsCreated, long totalProcessingErrors, long avgFlushTimeMicros,
                                  long lastAccessTime, long lastSnapshotTime, int entriesSinceLastSnapshot,
                                  boolean isIdle, double memoryUtilization,
                                  EntriesSnapshot lastSnapshot) {
            this.currentBatchSize = currentBatchSize;
            this.snapshotBalance = snapshotBalance;
            this.snapshotSequence = snapshotSequence;
            this.totalEntriesProcessed = totalEntriesProcessed;
            this.totalBatchesFlushed = totalBatchesFlushed;
            this.totalSnapshotsCreated = totalSnapshotsCreated;
            this.totalProcessingErrors = totalProcessingErrors;
            this.avgFlushTimeMicros = avgFlushTimeMicros;
            this.lastAccessTime = lastAccessTime;
            this.lastSnapshotTime = lastSnapshotTime;
            this.entriesSinceLastSnapshot = entriesSinceLastSnapshot;
            this.isIdle = isIdle;
            this.memoryUtilization = memoryUtilization;
            this.lastSnapshot = lastSnapshot;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {

            private UUID accountId;
            private int currentBatchSize;
            private long snapshotBalance;
            private long snapshotSequence;
            private long totalEntriesProcessed;
            private long totalBatchesFlushed;
            private long totalSnapshotsCreated;
            private long totalProcessingErrors;
            private long avgFlushTimeMicros;
            private long lastAccessTime;
            private long lastSnapshotTime;
            private int entriesSinceLastSnapshot;
            private boolean isIdle;
            private double memoryUtilization;
            private EntriesSnapshot lastSnapshot;

            public Builder accountId(UUID accountId) {
                this.accountId = accountId;
                return this;
            }

            public Builder currentBatchSize(int currentBatchSize) {
                this.currentBatchSize = currentBatchSize;
                return this;
            }

            public Builder snapshotBalance(long snapshotBalance) {
                this.snapshotBalance = snapshotBalance;
                return this;
            }

            public Builder snapshotSequence(long snapshotSequence) {
                this.snapshotSequence = snapshotSequence;
                return this;
            }

            public Builder totalEntriesProcessed(long totalEntriesProcessed) {
                this.totalEntriesProcessed = totalEntriesProcessed;
                return this;
            }

            public Builder totalBatchesFlushed(long totalBatchesFlushed) {
                this.totalBatchesFlushed = totalBatchesFlushed;
                return this;
            }

            public Builder totalSnapshotsCreated(long totalSnapshotsCreated) {
                this.totalSnapshotsCreated = totalSnapshotsCreated;
                return this;
            }

            public Builder totalProcessingErrors(long totalProcessingErrors) {
                this.totalProcessingErrors = totalProcessingErrors;
                return this;
            }

            public Builder avgFlushTimeMicros(long avgFlushTimeMicros) {
                this.avgFlushTimeMicros = avgFlushTimeMicros;
                return this;
            }

            public Builder lastAccessTime(long lastAccessTime) {
                this.lastAccessTime = lastAccessTime;
                return this;
            }

            public Builder lastSnapshotTime(long lastSnapshotTime) {
                this.lastSnapshotTime = lastSnapshotTime;
                return this;
            }

            public Builder entriesSinceLastSnapshot(int entriesSinceLastSnapshot) {
                this.entriesSinceLastSnapshot = entriesSinceLastSnapshot;
                return this;
            }

            public Builder isIdle(boolean isIdle) {
                this.isIdle = isIdle;
                return this;
            }

            public Builder memoryUtilization(double memoryUtilization) {
                this.memoryUtilization = memoryUtilization;
                return this;
            }

            public Builder lastSnapshot(EntriesSnapshot lastSnapshot) {
                this.lastSnapshot = lastSnapshot;
                return this;
            }

            public HotAccountMetrics build() {
                return new HotAccountMetrics(currentBatchSize, snapshotBalance, snapshotSequence,
                    totalEntriesProcessed, totalBatchesFlushed, totalSnapshotsCreated, totalProcessingErrors,
                    avgFlushTimeMicros, lastAccessTime, lastSnapshotTime, entriesSinceLastSnapshot,
                    isIdle, memoryUtilization, lastSnapshot);
            }
        }
    }

    // === TESTING SUPPORT ===

    /**
     * For testing - direct access to internal state
     */
    public int getCurrentBatchSizeForTesting() {
        return currentBatchSize;
    }

    public long getSnapshotBalanceForTesting() {
        return snapshotBalance;
    }

    public EntryRecordBatchHandler entryRecordBatchHandler() {
        return entryRecordBatchHandler;
    }
}