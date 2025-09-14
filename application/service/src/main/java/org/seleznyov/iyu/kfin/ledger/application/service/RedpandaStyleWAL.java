package org.seleznyov.iyu.kfin.ledger.application.service;

// Применение принципов Redpanda к нашему Java WAL решению

// === 1. THREAD-PER-CORE АРХИТЕКТУРА ===

import org.springframework.stereotype.Component;

import java.nio.BufferOverflowException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Timer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class RedpandaStyleWAL {

    // Один WAL writer на CPU core (как в Redpanda)
    private final int numberOfCores = Runtime.getRuntime().availableProcessors();
    private final WALCoreWriter[] coreWriters;
    private final ThreadLocal<WALCoreWriter> localWriter = new ThreadLocal<>();

    @PostConstruct
    public void initializeCoreWriters() {
        coreWriters = new WALCoreWriter[numberOfCores];

        for (int i = 0; i < numberOfCores; i++) {
            coreWriters[i] = new WALCoreWriter(i);
        }

        log.info("Initialized {} WAL writers (thread-per-core)", numberOfCores);
    }

    // === 2. SHARED-NOTHING WAL WRITER ===

    private static class WALCoreWriter {
        private final int coreId;
        private final Thread dedicatedThread;
        private final MappedByteBuffer dedicatedBuffer;
        private final RandomAccessFile dedicatedFile;

        // Lock-free очередь для этого core (как в Seastar)
        private final AtomicReferenceArray<WALRecord> lockFreeQueue;
        private final AtomicInteger writeHead = new AtomicInteger(0);
        private final AtomicInteger readTail = new AtomicInteger(0);

        // Статистика производительности
        private volatile long totalWrites = 0;
        private volatile long totalBytes = 0;
        private volatile long lastFlushTime = System.currentTimeMillis();

        WALCoreWriter(int coreId) throws IOException {
            this.coreId = coreId;

            // Создаем отдельный файл для каждого core
            String filename = String.format("wal-core-%d.log", coreId);
            this.dedicatedFile = new RandomAccessFile(filename, "rw");

            // Memory-mapped buffer только для этого core
            this.dedicatedBuffer = dedicatedFile.getChannel().map(
                FileChannel.MapMode.READ_WRITE, 0, 1024 * 1024 * 1024 // 1GB per core
            );

            // Lock-free очередь (размер = степень 2 для быстрого модуло)
            this.lockFreeQueue = new AtomicReferenceArray<>(65536);

            // Создаем выделенный поток, привязанный к CPU core
            this.dedicatedThread = createAffinityThread();
        }

        private Thread createAffinityThread() {
            return Thread.ofVirtual() // Java 21+ Virtual Thread
                .name("WAL-Core-" + coreId)
                .factory()
                .newThread(() -> {
                    try {
                        // В реальной реализации здесь был бы JNI вызов для CPU affinity
                        // setThreadAffinity(coreId);

                        runWriteLoop();
                    } catch (Exception e) {
                        log.error("WAL writer for core {} failed", coreId, e);
                    }
                });
        }

        // === 3. ВЫСОКОПРОИЗВОДИТЕЛЬНЫЙ WRITE LOOP ===

        private void runWriteLoop() {
            int batchSize = 0;
            long batchStartTime = System.nanoTime();

            while (!Thread.currentThread().isInterrupted()) {

                // Читаем из lock-free очереди
                int currentTail = readTail.get();
                WALRecord record = lockFreeQueue.get(currentTail & 65535); // Быстрый модуло

                if (record != null) {
                    // Атомарно забираем запись
                    if (lockFreeQueue.compareAndSet(currentTail & 65535, record, null)) {
                        readTail.incrementAndGet();

                        // Записываем в memory-mapped buffer
                        writeRecordToBuffer(record);
                        batchSize++;
                        totalWrites++;

                        // Уведомляем о завершении записи
                        if (record.getCompletionCallback() != null) {
                            record.getCompletionCallback().complete(null);
                        }
                    }
                }

                // === 4. АДАПТИВНОЕ BATCHING (как в Redpanda) ===

                long elapsed = System.nanoTime() - batchStartTime;
                boolean shouldFlush = batchSize >= 100 ||                    // Batch size threshold
                    elapsed >= 1_000_000 ||                  // 1ms timeout
                    (batchSize > 0 && isLowLatencyMode());   // Low latency mode

                if (shouldFlush && batchSize > 0) {
                    flushBatch();
                    batchSize = 0;
                    batchStartTime = System.nanoTime();
                }

                // Yield для других задач (как Seastar cooperative scheduling)
                if (elapsed > 100_000) { // 100μs
                    Thread.yield();
                }
            }
        }

        private void writeRecordToBuffer(WALRecord record) {
            try {
                // === 5. ZERO-COPY SERIALIZATION ===

                // Прямая запись в memory-mapped buffer без промежуточных копий
                int position = dedicatedBuffer.position();

                // Записываем заголовок записи (фиксированный размер)
                dedicatedBuffer.putLong(record.getOffset());           // 8 bytes
                dedicatedBuffer.putLong(record.getTimestamp());        // 8 bytes
                dedicatedBuffer.putInt(record.getAccountId().length()); // 4 bytes

                // Записываем переменные данные
                dedicatedBuffer.put(record.getAccountId().getBytes(StandardCharsets.UTF_8));

                // Compact BigDecimal serialization
                byte[] amountBytes = record.getAmount().unscaledValue().toByteArray();
                dedicatedBuffer.putInt(amountBytes.length);
                dedicatedBuffer.put(amountBytes);
                dedicatedBuffer.putInt(record.getAmount().scale());

                totalBytes += dedicatedBuffer.position() - position;

            } catch (BufferOverflowException e) {
                // Переключаемся на новый segment
                rollToNewSegment();
                writeRecordToBuffer(record); // Retry
            }
        }

        // === 6. АСИНХРОННЫЙ FLUSH С ПРИОРИТЕТАМИ ===

        private void flushBatch() {
            long flushStart = System.nanoTime();

            try {
                // Принудительно сбрасываем на диск
                dedicatedBuffer.force();

                long flushDuration = System.nanoTime() - flushStart;
                lastFlushTime = System.currentTimeMillis();

                // Метрики производительности
                updateFlushMetrics(flushDuration);

            } catch (Exception e) {
                log.error("Flush failed for core {}", coreId, e);
            }
        }

        // === 7. LOCK-FREE ENQUEUE ===

        public CompletableFuture<Void> enqueue(UltraHighPerformanceWAL.WALRecord record) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            record.setCompletionCallback(future);

            // Lock-free добавление в очередь
            while (true) {
                int currentHead = writeHead.get();
                int nextHead = currentHead + 1;

                // Проверяем переполнение очереди
                if (nextHead - readTail.get() >= 65536) {
                    // Очередь полная - применяем backpressure
                    future.completeExceptionally(new WALOverflowException(
                        "WAL queue full for core " + coreId));
                    return future;
                }

                // Пытаемся записать запись
                if (lockFreeQueue.compareAndSet(currentHead & 65535, null, record)) {
                    if (writeHead.compareAndSet(currentHead, nextHead)) {
                        break; // Успешно добавили
                    }
                }

                // Retry с exponential backoff
                try {
                    Thread.sleep(0, 100); // 100ns
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    future.completeExceptionally(e);
                    return future;
                }
            }

            return future;
        }
    }

    // === 8. ИНТЕЛЛЕКТУАЛЬНОЕ РАСПРЕДЕЛЕНИЕ НАГРУЗКИ ===

    public CompletableFuture<Void> writeRecord(UltraHighPerformanceWAL.WALRecord record) {
        // Выбираем наименее загруженный core writer
        WALCoreWriter writer = selectOptimalWriter(record);

        return writer.enqueue(record);
    }

    private WALCoreWriter selectOptimalWriter(UltraHighPerformanceWAL.WALRecord record) {
        // === 9. LOCALITY-AWARE ROUTING ===

        // Сначала пробуем thread-local writer (минимизируем context switching)
        WALCoreWriter localWriter = this.localWriter.get();
        if (localWriter != null && !localWriter.isOverloaded()) {
            return localWriter;
        }

        // Иначе выбираем по алгоритму наименьшей нагрузки
        WALCoreWriter bestWriter = coreWriters[0];
        long bestScore = calculateWriterScore(bestWriter);

        for (int i = 1; i < coreWriters.length; i++) {
            long score = calculateWriterScore(coreWriters[i]);
            if (score < bestScore) {
                bestScore = score;
                bestWriter = coreWriters[i];
            }
        }

        // Сохраняем как thread-local для будущих вызовов
        this.localWriter.set(bestWriter);

        return bestWriter;
    }

    private long calculateWriterScore(WALCoreWriter writer) {
        // Учитываем текущую нагрузку
        long queueSize = writer.writeHead.get() - writer.readTail.get();

        // Учитываем среднее время flush
        long avgFlushTime = writer.getAverageFlushTime();

        // Учитываем время с последнего flush (старые данные = хуже)
        long timeSinceFlush = System.currentTimeMillis() - writer.lastFlushTime;

        // Комбинированный score (меньше = лучше)
        return queueSize * 1000 + avgFlushTime + timeSinceFlush;
    }

    // === 10. АДАПТИВНЫЕ АЛГОРИТМЫ ===

    private boolean isLowLatencyMode() {
        // Автоматически определяем нужен ли low-latency режим
        long currentTps = getCurrentTransactionRate();
        double avgLatency = getAverageLatency();

        return currentTps < 10000 || avgLatency > 5.0; // 5ms
    }

    // === 11. BACKPRESSURE MANAGEMENT ===

    @Component
    public static class WALBackpressureManager {

        private final AtomicLong totalEnqueuedOps = new AtomicLong(0);
        private final AtomicLong totalProcessedOps = new AtomicLong(0);

        public boolean shouldApplyBackpressure() {
            long enqueued = totalEnqueuedOps.get();
            long processed = totalProcessedOps.get();
            long backlog = enqueued - processed;

            // Применяем backpressure если отставание > 100K операций
            if (backlog > 100_000) {
                log.warn("Applying backpressure: {} operations in backlog", backlog);
                return true;
            }

            return false;
        }

        public void waitForBackpressureRelief() throws InterruptedException {
            int attempts = 0;
            while (shouldApplyBackpressure() && attempts < 100) {
                // Exponential backoff
                Thread.sleep(Math.min(1000, 10 * (1L << Math.min(attempts, 6))));
                attempts++;
            }

            if (attempts >= 100) {
                throw new WALOverflowException("Backpressure timeout");
            }
        }
    }
}

// === 12. МОНИТОРИНГ И МЕТРИКИ (как в Redpanda) ===

@Component
public class WALPerformanceMonitor {

    private final MeterRegistry meterRegistry;

    // Детализированные метрики для каждого core
    private final Timer[] writeLatencyByCore;
    private final Counter[] operationsByCore;
    private final Gauge[] queueSizeByCore;

    public WALPerformanceMonitor(MeterRegistry meterRegistry, int numberOfCores) {
        this.meterRegistry = meterRegistry;

        writeLatencyByCore = new Timer[numberOfCores];
        operationsByCore = new Counter[numberOfCores];
        queueSizeByCore = new Gauge[numberOfCores];

        for (int i = 0; i < numberOfCores; i++) {
            final int coreId = i;

            writeLatencyByCore[i] = Timer.builder("wal.write.latency")
                .tag("core", String.valueOf(coreId))
                .register(meterRegistry);

            operationsByCore[i] = Counter.builder("wal.operations")
                .tag("core", String.valueOf(coreId))
                .register(meterRegistry);

            queueSizeByCore[i] = Gauge.builder("wal.queue.size")
                .tag("core", String.valueOf(coreId))
                .register(meterRegistry, this, monitor -> getQueueSize(coreId));
        }

        // Aggregate метрики
        meterRegistry.gauge("wal.total.throughput", this, WALPerformanceMonitor::getTotalThroughput);
        meterRegistry.gauge("wal.memory.usage.mb", this, monitor -> getMemoryUsageMB());
    }

    private double getTotalThroughput() {
        return Arrays.stream(operationsByCore)
            .mapToDouble(counter -> counter.count())
            .sum();
    }

    private long getMemoryUsageMB() {
        // Приблизительное использование памяти всеми memory-mapped буферами
        return Runtime.getRuntime().availableProcessors() * 1024L; // 1GB per core
    }
}

// Исключения
class WALOverflowException extends RuntimeException {
    public WALOverflowException(String message) {
        super(message);
    }
}