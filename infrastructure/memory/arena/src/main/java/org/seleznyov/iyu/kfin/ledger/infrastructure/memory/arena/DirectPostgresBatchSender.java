package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import lombok.extern.slf4j.Slf4j;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyOperation;
import org.postgresql.core.QueryExecutor;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.BatchRingBufferHandler;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.EntryRecordBatchHandler;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.MemorySegment;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ✅ Максимально быстрая отправка PostgreSQL binary data через QueryExecutor.sendCopyData()
 * Минимизирует copying и использует прямой доступ к PostgreSQL protocol
 */
@Slf4j
public abstract class DirectPostgresBatchSender<T extends BatchRingBufferHandler> {

    protected final DataSource dataSource;

    // ✅ Оптимальные настройки для network transmission
    protected static final int OPTIMAL_CHUNK_SIZE = 256 * 1024; // 256KB
    protected static final int CONNECTION_TIMEOUT_MS = 30_000;
    protected static final int QUERY_TIMEOUT_MS = 60_000;
    protected static final int RESULTS_ARRAY_LENGTH = 2;
    protected static final int TOTAL_SIZE_RESULTS_ARRAY_INDEX = 0;
    protected static final int ENTRIES_COUNT_RESULTS_ARRAY_INDEX = 1;
//    protected static final String COPY_SQL = """
//        COPY ledger.entry_records
//        (id, account_id, transaction_id, entry_type, amount,
//         created_at_millis, operation_date_epoch_day, idempotency_key,
//         currency_code, entry_ordinal)
//        FROM STDIN WITH (FORMAT BINARY)
//        """;

    protected abstract String copySql();

//    protected final AtomicLong totalBytesSent = new AtomicLong(0);
//    protected final AtomicLong totalEntriesSent = new AtomicLong(0);
//    protected final AtomicLong totalBatchesSent = new AtomicLong(0);
//    protected final AtomicLong totalSendTime = new AtomicLong(0);
//    protected final AtomicLong totalErrors = new AtomicLong(0);
    protected final long[] sentResults = new long[RESULTS_ARRAY_LENGTH];
    protected final T ringBufferHandler;

    public DirectPostgresBatchSender(
        DataSource dataSource,
        T ringBufferHandler
    ) {
        this.dataSource = dataSource;
        this.ringBufferHandler = ringBufferHandler;
//        log.info("Created DirectQueryExecutorSender with chunk_size={}KB", OPTIMAL_CHUNK_SIZE / 1024);
        log.info("Created DirectQueryExecutorSender with chunk_size={}KB", OPTIMAL_CHUNK_SIZE >> 10);
    }

    /**
     * ✅ Максимально быстрая отправка через QueryExecutor.sendCopyData()
     */
    public long sendDirectly(long batchSlotOffset, long batchRawSize) {

        long startTime = System.nanoTime();

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);

            // Получаем прямой доступ к QueryExecutor
            QueryExecutor queryExecutor = getQueryExecutor(connection);

            // Начинаем COPY операцию
            final CopyOperation copyOperation = queryExecutor.startCopy(copySql(), true);
            final CopyIn copyIn = (CopyIn) copyOperation;

            // ✅ Отправляем данные оптимальными chunk'ами через sendCopyData
            copyIteratively(copyIn, batchSlotOffset, batchRawSize, sentResults);
            copyIn.flushCopy();

            // Завершаем COPY
            copyIn.endCopy();

            connection.commit();

            final long sendTime = System.nanoTime() - startTime;
//            final long entriesCount = ringBufferLayout.batchEntriesCount(batchSlotOffset);
            updateMetrics(
                sentResults[TOTAL_SIZE_RESULTS_ARRAY_INDEX],
                sentResults[ENTRIES_COUNT_RESULTS_ARRAY_INDEX],
                sendTime
            );

            final long totalTime = System.nanoTime() - startTime;
            log.debug(
                "Successfully sent {} entries via QueryExecutor in {}ms",
                sentResults[TOTAL_SIZE_RESULTS_ARRAY_INDEX],
                totalTime / 1_000_000
            );

            return sentResults[TOTAL_SIZE_RESULTS_ARRAY_INDEX];

//        } catch (SQLException e) {
//            long totalTime = System.nanoTime() - startTime;
//            log.error("Error sending data via QueryExecutor after {}ms", totalTime / 1_000_000, e);
//            throw e;
//        }
        } catch (SQLException exception) {
            totalErrors.incrementAndGet();
            long sendTime = System.nanoTime() - startTime;
            log.error("Error sending data via QueryExecutor after {}ms: {}",
                sendTime / 1_000_000, exception.getMessage());
            throw new RuntimeException(exception);
        } catch (Exception exception) {
            totalErrors.incrementAndGet();
            log.error("Unexpected error in direct send", exception);
            throw new RuntimeException("Direct send failed", exception);
        }
    }

    /**
     * ✅ Получение QueryExecutor через reflection (internal API)
     */
    private QueryExecutor getQueryExecutor(Connection connection) throws SQLException {
        try {
            // Unwrap до PGConnection
            PGConnection pgConnection = connection.unwrap(PGConnection.class);

            // Получаем QueryExecutor через reflection
            Method getQueryExecutorMethod = pgConnection.getClass().getMethod("getQueryExecutor");
            QueryExecutor executor = (QueryExecutor) getQueryExecutorMethod.invoke(pgConnection);

            log.debug("Successfully obtained QueryExecutor: {}", executor.getClass().getSimpleName());
            return executor;

        } catch (Exception e) {
            log.error("Cannot access QueryExecutor via reflection", e);
            throw new SQLException("Cannot access PostgreSQL QueryExecutor", e);
        }
    }

    /**
     * ✅ Ключевой метод - отправка через sendCopyData БЕЗ stream'ов
     */
    private void copyIteratively(CopyIn copyIn, long batchSlotOffset, long batchRawSize, long[] results) throws SQLException {

        long totalSize = ringBufferHandler.ringBufferSegment().byteSize();
        long sent = 0;

        // ✅ Переиспользуемый buffer - создается один раз
        byte[] reusableChunk = new byte[(int) batchRawSize];
        long entriesCount = 0;

        log.debug("Starting to send {} bytes in chunks of {}KB", totalSize, OPTIMAL_CHUNK_SIZE / 1024);

        while (sent < batchRawSize) {
//            long remainingSize = totalSize - sent;
            long currentBatchSize = ringBufferHandler.batchSize(batchSlotOffset + sent);
            entriesCount += ringBufferHandler.batchElementsCount(batchSlotOffset + sent);
            int currentChunkSize = (int) Math.min(currentBatchSize, batchRawSize);

            // ✅ Bulk copy из MemorySegment в reusable array (единственное copying)
            //TODO: переделать на отправку по батчам, ведь у каждого батча есть заголовок
            copyMemorySegmentToArray(ringBufferHandler.ringBufferSegment(), batchSlotOffset + sent, reusableChunk, currentChunkSize);

            // ✅ Прямая отправка через network БЕЗ промежуточных stream'ов
            copyIn.writeToCopy(reusableChunk, 0, currentChunkSize);

            sent += currentChunkSize;

            if (log.isTraceEnabled() && sent % (10 * OPTIMAL_CHUNK_SIZE) == 0) {
                log.trace("Sent {} / {} bytes ({}%)", sent, totalSize, (sent * 100) / totalSize);
            }
        }

        log.debug("Completed sending {} bytes in {} chunks", totalSize, (sent + OPTIMAL_CHUNK_SIZE - 1) / OPTIMAL_CHUNK_SIZE);

        results[0] = totalSize;//calculateEntryCountFromSize(totalSize);
        results[1] = entriesCount;
    }

    /**
     * ✅ Оптимизированное copying из MemorySegment в byte array
     */
    private void copyMemorySegmentToArray(MemorySegment source, long sourceOffset,
                                          byte[] target, int copySize) {
        try {
            // Создаем slice без дополнительных проверок
//            MemorySegment sourceSlice = source.asSlice(sourceOffset, copySize);
            MemorySegment targetSlice = MemorySegment.ofArray(target);//.asSlice(0, copySize);

            // Bulk memory copy (JVM оптимизирует это до memcpy)
            MemorySegment.copy(source, sourceOffset, targetSlice, 0, copySize);

        } catch (Exception e) {
            log.error("Error copying memory segment to array: source_offset={}, copy_size={}",
                sourceOffset, copySize, e);
            throw new RuntimeException("Memory copy failed", e);
        }
    }

    private int calculateEntryCountFromSize(long totalSize) {
        // Структура PostgreSQL binary batch:
        // - PostgreSQL header: 19 bytes
        // - Entry records: N * POSTGRES_RECORD_SIZE
        // - Terminator: 2 bytes
        long dataSize = totalSize - 19 - 2;
        int entryCount = (int) (dataSize / EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE);

        log.trace("Calculated entry count: {} from total_size={} bytes", entryCount, totalSize);
        return entryCount;
    }

    private void updateMetrics(long bytesSent, long entriesSent, long sendTimeNanos) {
        totalBytesSent.addAndGet(bytesSent);
        totalBatchesSent.incrementAndGet();
        totalEntriesSent.addAndGet(entriesSent);
        totalSendTime.addAndGet(sendTimeNanos);
    }

    /**
     * Альтернативный метод через оптимизированный InputStream
     */
    public int sendViaOptimizedInputStream(MemorySegment postgresBinarySegment) throws SQLException {

        long startTime = System.nanoTime();

        try (Connection connection = createOptimizedConnection()) {
            connection.setAutoCommit(false);

            PGConnection pgConnection = connection.unwrap(PGConnection.class);
            org.postgresql.copy.CopyManager copyManager = pgConnection.getCopyAPI();

            // Используем оптимизированный InputStream
            try (InputStream optimizedStream = new OptimizedPostgresBinaryInputStream(postgresBinarySegment)) {

                long insertedRows = copyManager.copyIn(copySql(), optimizedStream);

                connection.commit();

                long sendTime = System.nanoTime() - startTime;
                int entryCount = calculateEntryCountFromSize(postgresBinarySegment.byteSize());
                updateMetrics(postgresBinarySegment.byteSize(), entryCount, sendTime);

                log.debug("Successfully sent via optimized InputStream in {}ms: {} rows",
                    sendTime / 1_000_000, insertedRows);

                return (int) insertedRows;
            }

        } catch (SQLException exception) {
            totalErrors.incrementAndGet();
            long sendTime = System.nanoTime() - startTime;
            log.error("Error sending via optimized InputStream after {}ms", sendTime / 1_000_000, exception);
            throw exception;
        } catch (IOException exception) {
            totalErrors.incrementAndGet();
            long sendTime = System.nanoTime() - startTime;
            log.error("Error sending via optimized InputStream after {}ms", sendTime / 1_000_000, exception);
            throw new RuntimeException(exception);
        }
    }


    // ===== METRICS & MONITORING =====

    public DirectPostgresBatchSender.DirectQueryExecutorMetrics getMetrics() {
        long batches = totalBatchesSent.get();
        long entries = totalEntriesSent.get();
        long totalTimeNanos = totalSendTime.get();

        double avgBatchTimeMs = batches > 0 ? (double) totalTimeNanos / batches / 1_000_000 : 0;
        double avgEntryTimeMicros = entries > 0 ? (double) totalTimeNanos / entries / 1_000 : 0;
        double throughputBytesPerSec = totalTimeNanos > 0 ?
            (double) totalBytesSent.get() / totalTimeNanos * 1_000_000_000 : 0;

        return new DirectPostgresBatchSender.DirectQueryExecutorMetrics(
            totalBatchesSent.get(),
            totalEntriesSent.get(),
            totalBytesSent.get(),
            totalErrors.get(),
            avgBatchTimeMs,
            avgEntryTimeMicros,
            throughputBytesPerSec
        );
    }

    public String getDiagnostics() {
        DirectPostgresBatchSender.DirectQueryExecutorMetrics metrics = getMetrics();
        return String.format(
            "DirectQueryExecutorSender[batches=%d, entries=%d, bytes=%dMB, errors=%d, " +
                "avg_batch_time=%.1fms, throughput=%.1fMB/s]",
            metrics.totalBatchesSent, metrics.totalEntriesSent, metrics.totalBytesSent / 1024 / 1024,
            metrics.totalErrors, metrics.avgBatchTimeMs, metrics.throughputBytesPerSec / 1024 / 1024
        );
    }

    public static class DirectQueryExecutorMetrics {

        public final long totalBatchesSent;
        public final long totalEntriesSent;
        public final long totalBytesSent;
        public final long totalErrors;
        public final double avgBatchTimeMs;
        public final double avgEntryTimeMicros;
        public final double throughputBytesPerSec;

        public DirectQueryExecutorMetrics(long totalBatchesSent, long totalEntriesSent, long totalBytesSent,
                                          long totalErrors, double avgBatchTimeMs, double avgEntryTimeMicros,
                                          double throughputBytesPerSec) {
            this.totalBatchesSent = totalBatchesSent;
            this.totalEntriesSent = totalEntriesSent;
            this.totalBytesSent = totalBytesSent;
            this.totalErrors = totalErrors;
            this.avgBatchTimeMs = avgBatchTimeMs;
            this.avgEntryTimeMicros = avgEntryTimeMicros;
            this.throughputBytesPerSec = throughputBytesPerSec;
        }
    }

    /**
     * Создание оптимизированного connection
     */
    private Connection createOptimizedConnection() throws SQLException {
        Connection connection = dataSource.getConnection();

        // Оптимизация connection для bulk operations
        connection.setNetworkTimeout(null, CONNECTION_TIMEOUT_MS);

        // Если это PostgreSQL connection, применяем дополнительные оптимизации
        if (connection instanceof PGConnection || connection.isWrapperFor(PGConnection.class)) {
            try {
                PGConnection pgConn = connection.unwrap(PGConnection.class);
                // Можно установить дополнительные параметры здесь
                log.trace("Applied PostgreSQL-specific optimizations");
            } catch (SQLException e) {
                log.debug("Could not apply PostgreSQL optimizations: {}", e.getMessage());
            }
        }

        return connection;
    }

    // ===== TESTING SUPPORT =====

    /**
     * Тестовый метод для проверки connectivity
     */
    public boolean testConnection() {
        try (Connection connection = createOptimizedConnection()) {
            QueryExecutor executor = getQueryExecutor(connection);
            log.info("Successfully tested PostgreSQL connection and QueryExecutor access");
            return true;
        } catch (Exception e) {
            log.error("Connection test failed", e);
            return false;
        }
    }

    /**
     * Сброс метрик (для тестирования)
     */
    public void resetMetrics() {
        totalBytesSent.set(0);
        totalBatchesSent.set(0);
        totalEntriesSent.set(0);
        totalSendTime.set(0);
        totalErrors.set(0);
        log.debug("DirectQueryExecutorSender metrics reset");
    }
}