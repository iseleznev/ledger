package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration.WalConfiguration;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.EntryRecordBatchHandler;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.WalEntryRecordBatchRingBufferHandler;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.locks.LockSupport;

@Slf4j
public class HotAccountWalWorker {
    static final String STAGE_TABLE_NAME_PREFIX = "stage_record_entry";

    private final WalEntryRecordBatchRingBufferHandler walRingBuffer;
    private final WalBatchWriter walBatchWriter;
    private final WalConfiguration walConfiguration;
    private final Arena walArena;
    private final int workerId;
    private final Thread workerThread;
    private volatile boolean isRunning;

    public HotAccountWalWorker(
        int workerId,
        long walFileOffset,
        WalConfiguration walConfiguration
    ) {
        this.walConfiguration = walConfiguration;
        //this.entryRecordBatchRingBuffer = entryRecordBatchRingBuffer;
        this.walArena = Arena.ofShared();
        // ✅ Создаем огромный off-heap ring buffer
        long ringBufferSize = walConfiguration.batchEntriesCount()
            * walConfiguration.ringBufferBatchesCount()
            * EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE;
        // ringBufferSizeGB * 1024 * 1024 * 1024; // GB to bytes
        final MemorySegment ringBufferSegment = walArena.allocate(ringBufferSize, 64);
        final long bufferSize = walConfiguration.writeFileBufferSize() % ringBufferSize;
        this.walRingBuffer = new WalEntryRecordBatchRingBufferHandler(
            ringBufferSegment,
            walConfiguration.batchEntriesCount(),
            (bufferSize == 0 ? walConfiguration.writeFileBufferSize() : walConfiguration.writeFileBufferSize() / ringBufferSize),
            workerId
        );
        this.workerId = workerId;
        walBatchWriter = new WalBatchWriter(
            walConfiguration,
            workerId,
            walFileOffset
        );
        this.workerThread = Thread.ofPlatform()
            .name("zero-alloc-wal-worker-" + workerId)
            .priority(Thread.NORM_PRIORITY + 1)
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
        this.walArena.close();
    }

    public WalEntryRecordBatchRingBufferHandler ringBuffer() {
        return walRingBuffer;
    }

    private void entryRecordBatchRingBuffer() {
        log.info("Worker {} started processing zero-allocation batches", this.workerThread);

        // ✅ Создаем consumer который работает напрямую с off-heap memory
//        final PostgresBatchProcessor processor = new  PostgresBatchProcessor(directSenders.get(workerId), workerId);
//        final PostgresCheckpointWriter checkpointWriter = new PostgresCheckpointWriter();
//            (ringSegment, batchOffset) -> {
//                return processOffHeapBatch(ringSegment, batchOffset, workerId);
//            };

        final long parkNanos = walConfiguration.waitingEmptyRingBufferNanos();
        final int retries = walConfiguration.emptyIterationsYieldRetries();

        long emptyIterations = 0;

        while (isRunning) {
            try {
                final long readOffset = walRingBuffer.readOffset();
                final long batchSize = walRingBuffer.batchSize(readOffset);
                final long walSequenceId = walRingBuffer.walSequenceId(readOffset);
                //final long ringBufferReadOffset = WAL_READ_OFFSET_MEMORY_BARRIER.getAndAddRelease(this, batchSize);
                // ✅ Пытаемся обработать батч БЕЗ copying в heap
                final long writtenSize = walBatchWriter.writeBatch(
                    walRingBuffer,
                    walSequenceId,
                    readOffset,
                    batchSize
                );
                if (writtenSize == batchSize) {
                    walRingBuffer.readOffset(readOffset + writtenSize);
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
                totalErrors.incrementAndGet();
                log.error("Error in zero-allocation worker {}", workerId, e);

                // Пауза при ошибке чтобы не спамить
                LockSupport.parkNanos(10_000_000L); // 10ms
            }
        }

        log.info("Worker {} stopped processing batches", workerId);
    }
}
