package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.PostgreSqlEntriesSnapshotRingBufferHandler;

import javax.sql.DataSource;

/**
 * ✅ Максимально быстрая отправка PostgreSQL binary data через QueryExecutor.sendCopyData()
 * Минимизирует copying и использует прямой доступ к PostgreSQL protocol
 */
@Slf4j
public class EntriesSnapshotDirectPostgresBatchSender extends DirectPostgresBatchSender<PostgreSqlEntriesSnapshotRingBufferHandler> {

    private static final String COPY_SQL = """
        COPY ledger.account_snapshots
        (snapshot_id, account_id, balance, entry_count, sequence_number,
         trigger_type, created_at_millis, duration_millis)
        FROM STDIN WITH (FORMAT BINARY)
        """;

    public EntriesSnapshotDirectPostgresBatchSender(DataSource dataSource, PostgreSqlEntriesSnapshotRingBufferHandler ringBufferHandler) {
        super(dataSource, ringBufferHandler);
    }

    @Override
    protected String copySql() {
        return COPY_SQL;
    }
}