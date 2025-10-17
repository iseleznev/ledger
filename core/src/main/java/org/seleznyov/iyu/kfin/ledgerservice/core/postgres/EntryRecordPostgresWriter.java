package org.seleznyov.iyu.kfin.ledgerservice.core.postgres;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyOperation;
import org.postgresql.core.QueryExecutor;
import org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties.LedgerConfiguration;
import org.seleznyov.iyu.kfin.ledgerservice.core.constants.EntryRecordOffsetConstants;
import org.seleznyov.iyu.kfin.ledgerservice.core.constants.PostgresConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;

public class EntryRecordPostgresWriter {

    private static final Logger log = LoggerFactory.getLogger(EntryRecordPostgresWriter.class);

    private static final String COPY_SQL = """
        (id, account_id, transaction_id, entry_type, amount,
         created_at_millis, operation_date_epoch_day, idempotency_key,
         currency_code, entry_ordinal)
        FROM STDIN WITH (FORMAT BINARY)
        """;

    private final LedgerConfiguration ledgerConfiguration;
    private final DataSource dataSource;
    private final int optimalChunkSize;// = 1024 * 1024;
    private final int optimalChunkRecordsCount;// = 1024 * 1024;
    private final byte[] reusableChunk;
    private final long[] results = new long[2];
//    private final long batchSlotOffset = 0;
//    private final long batchRawSize = optimalChunkSize;
//    private final byte[] reusableChunk = new byte[(int) batchRawSize];
//    private final long[] sentResults = new long[2];

    public EntryRecordPostgresWriter(
        LedgerConfiguration ledgerConfiguration,
        DataSource dataSource
    ) {
        this.ledgerConfiguration = ledgerConfiguration;
        this.dataSource = dataSource;
        this.optimalChunkRecordsCount = ledgerConfiguration.postgres().batchRecordsCount();
        this.optimalChunkSize = this.optimalChunkRecordsCount * EntryRecordOffsetConstants.POSTGRES_ENTRY_RECORD_SIZE;
        this.reusableChunk = new byte[this.optimalChunkSize];
    }

    public long write(
        MemorySegment memorySegment,
        long sourceOffset,
        int writeSize
    ) {
        long startTime = System.nanoTime();

        try (
            Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);

            // Получаем прямой доступ к QueryExecutor
            QueryExecutor queryExecutor = queryExecutor(connection);

            // Начинаем COPY операцию
            final CopyOperation copyOperation = queryExecutor.startCopy(COPY_SQL, true);
            final CopyIn copyIn = (CopyIn) copyOperation;

            // ✅ Отправляем данные оптимальными chunk'ами через sendCopyData
            copyIteratively(memorySegment, copyIn, sourceOffset, writeSize, reusableChunk, results);
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
        } catch (
            SQLException exception) {
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

    private void copyIteratively(MemorySegment memorySegment, CopyIn copyIn, long batchSlotOffset, int batchRawSize, byte[] reusableChunk, long[] results) throws SQLException {

        long totalSize = memorySegment.byteSize();
        long maxAcceptableSize = batchRawSize;
        if (maxAcceptableSize > totalSize - batchSlotOffset) {
            maxAcceptableSize = totalSize - batchSlotOffset;
        }
        int sent = 0;

        // ✅ Переиспользуемый buffer - создается один раз
//        byte[] reusableChunk = new byte[(int) batchRawSize];
        long entriesCount = 0;

        log.debug("Starting to send {} bytes in chunks of {}KB", totalSize, optimalChunkSize / 1024);

        while (sent < batchRawSize) {
//            long remainingSize = totalSize - sent;
            int acceptableSize = batchRawSize - sent;

            if (acceptableSize > optimalChunkSize) {
                acceptableSize = optimalChunkSize;
            }
//            entriesCount += batchSlotOffset + sent;
            int currentChunkSize = acceptableSize + 2;

            // ✅ Bulk copy из MemorySegment в reusable array (единственное copying)
            //TODO: переделать на отправку по батчам, ведь у каждого батча есть заголовок
            final int postgresChunkSize = copyMemorySegmentToArray(
                memorySegment,
                batchSlotOffset + sent,
                reusableChunk,
                currentChunkSize
            );

            // ✅ Прямая отправка через network БЕЗ промежуточных stream'ов
            copyIn.writeToCopy(reusableChunk, 0, postgresChunkSize);

            sent += currentChunkSize;

            if (log.isTraceEnabled() && sent % (10 * optimalChunkSize) == 0) {
                log.trace("Sent {} / {} bytes ({}%)", sent, totalSize, (sent * 100L) / totalSize);
            }
        }

        log.debug("Completed sending {} bytes in {} chunks", totalSize, (sent + optimalChunkSize - 1) / optimalChunkSize);

        results[0] = totalSize;//calculateEntryCountFromSize(totalSize);
        results[1] = entriesCount;
    }

    /**
     * ✅ Оптимизированное copying из MemorySegment в byte array
     */
    private int copyMemorySegmentToArray(
        MemorySegment source,
        long sourceOffset,
        byte[] target,
        int copySize
    ) {
        try {
            // Создаем slice без дополнительных проверок
//            MemorySegment sourceSlice = source.asSlice(sourceOffset, copySize);
            long dataOffset = sourceOffset + PostgresConstants.POSTGRES_HEADER_SIZE;
            long terminatorOffset = dataOffset + copySize;

            MemorySegment targetMemorySlice = MemorySegment.ofArray(target);//.asSlice(0, copySize);

            // Bulk memory copy (JVM оптимизирует это до memcpy)
            MemorySegment.copy(PostgresConstants.POSTGRES_SIGNATURE_MEMORY_SEGMENT, 0, targetMemorySlice, 0, PostgresConstants.POSTGRES_HEADER_SIZE);
            MemorySegment.copy(source, sourceOffset, targetMemorySlice, dataOffset, copySize);
            targetMemorySlice.set(ValueLayout.JAVA_SHORT, terminatorOffset, (short) -1);

            return PostgresConstants.POSTGRES_HEADER_SIZE + copySize + (int) ValueLayout.JAVA_SHORT.byteSize();
        } catch (Exception e) {
            log.error("Error copying memory segment to array: source_offset={}, copy_size={}",
                sourceOffset, copySize, e);
            throw new RuntimeException("Memory copy failed", e);
        }
    }

    private QueryExecutor queryExecutor(Connection connection) throws SQLException {
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
}
