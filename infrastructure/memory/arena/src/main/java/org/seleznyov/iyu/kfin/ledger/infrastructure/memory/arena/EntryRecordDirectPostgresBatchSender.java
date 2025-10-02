package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.PostgreSqlEntryRecordRingBufferHandler;

import javax.sql.DataSource;

/**
 * ✅ Максимально быстрая отправка PostgreSQL binary data через QueryExecutor.sendCopyData()
 * Минимизирует copying и использует прямой доступ к PostgreSQL protocol
 */
@Slf4j
public class EntryRecordDirectPostgresBatchSender extends DirectPostgresBatchSender<PostgreSqlEntryRecordRingBufferHandler> {

    private static final String COPY_SQL = """
        (id, account_id, transaction_id, entry_type, amount,
         created_at_millis, operation_date_epoch_day, idempotency_key,
         currency_code, entry_ordinal)
        FROM STDIN WITH (FORMAT BINARY)
        """;

    private final String tableName;
    private final String sql;

    public EntryRecordDirectPostgresBatchSender(DataSource dataSource, String tableName) {
        super(dataSource);
        this.tableName = tableName;
        this.sql = "COPY " + this.tableName + " " + COPY_SQL;
    }

    @Override
    protected String copySql() {
        return this.sql;
    }
}