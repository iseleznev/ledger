package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyOut;
import org.postgresql.core.QueryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.sql.Connection;
import java.sql.SQLException;

public class DirectPostgresBatchReader {

    static final Logger log = LoggerFactory.getLogger(DirectPostgresBatchReader.class);

    private static final String SQL = """
        String query = String.format(""\"
                COPY (
                    SELECT * FROM ledger.entry_records\s
                    WHERE account_id = '%s'
                ) TO STDOUT (FORMAT BINARY)
                ""\", accountId);
        """;

    private final DataSource dataSource;

    public DirectPostgresBatchReader(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public MemorySegment readIntoMemorySegment(
        String query,
        Arena arena,
        long estimatedSize
    ) throws SQLException {

        long startTime = System.nanoTime();

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);

            PGConnection pgConnection = connection.unwrap(PGConnection.class);
            QueryExecutor queryExecutor = getQueryExecutor(pgConnection);

            CopyOut copyOut = (CopyOut) queryExecutor.startCopy(query, false);

            // ✅ Аллоцируем off-heap память
            final MemorySegment targetSegment = arena.allocate(estimatedSize, 64); // 64-byte aligned

            long offset = 0;
//            byte[] readBuffer = new byte[OPTIMAL_READ_CHUNK_SIZE];

            while (true) {
                byte[] readBuffer = copyOut.readFromCopy();
                final long bytesRead = readBuffer == null ? 0 : readBuffer.length;

                if (readBuffer == null || readBuffer.length == 0) {
                    break;
                }

                // Проверка размера
                if (offset + bytesRead > targetSegment.byteSize()) {
                    throw new SQLException(
                        String.format("MemorySegment too small: need %d, have %d",
                            offset + bytesRead, targetSegment.byteSize())
                    );
                }

                // ✅ Копируем напрямую в MemorySegment (JVM оптимизирует до memcpy)
                MemorySegment.copy(
                    MemorySegment.ofArray(readBuffer), 0,
                    targetSegment, offset,
                    bytesRead
                );

                offset += bytesRead;
            }

            copyOut.cancelCopy();
            connection.commit();

            long readTime = System.nanoTime() - startTime;
//            updateMetrics(offset, 0, readTime);

            log.debug("Successfully read {} bytes into MemorySegment in {}ms",
                offset, readTime / 1_000_000);

            // Возвращаем slice с реальным размером данных
            return targetSegment.asSlice(0, offset);

        } catch (SQLException e) {
//            totalErrors.incrementAndGet();
            log.error("Error reading into MemorySegment", e);
            throw e;
        }
    }

    private QueryExecutor getQueryExecutor(PGConnection pgConnection) throws SQLException {
        try {
            java.lang.reflect.Method method = pgConnection.getClass().getMethod("getQueryExecutor");
            QueryExecutor executor = (QueryExecutor) method.invoke(pgConnection);

            log.trace("Successfully obtained QueryExecutor: {}",
                executor.getClass().getSimpleName());
            return executor;

        } catch (Exception e) {
            log.error("Cannot access QueryExecutor via reflection", e);
            throw new SQLException("Cannot access PostgreSQL QueryExecutor", e);
        }
    }

//    private void updateMetrics(long bytesRead, long entriesRead, long readTimeNanos) {
//        totalBytesRead.addAndGet(bytesRead);
//        totalBatchesRead.incrementAndGet();
//        totalEntriesRead.addAndGet(entriesRead);
//        totalReadTime.addAndGet(readTimeNanos);
//    }
}
