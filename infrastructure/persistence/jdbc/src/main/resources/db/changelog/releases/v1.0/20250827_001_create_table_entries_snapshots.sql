-- liquibase formatted sql

-- changeset Igor_Seleznyov:20250825_004_create_table_ entries_snapshots
-- comment: Create  entries_snapshots table for balance optimization
CREATE SEQUENCE ledger_snapshot_ordinal_sequence
    AS BIGINT
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE CACHE 10;

CREATE TABLE ledger.entries_snapshots
(
    id                   UUID        NOT NULL DEFAULT generate_uuid_v7_precise(),
    account_id           UUID        NOT NULL,
    operation_date       DATE        NOT NULL,
    balance              BIGINT      NOT NULL,
    last_entry_record_id UUID        NOT NULL,
    last_entry_ordinal   BIGINT      NOT NULL,
    operations_count     INTEGER     NOT NULL DEFAULT 0,
    snapshot_ordinal     BIGINT      NOT NULL DEFAULT nextval('ledger_snapshot_ordinal_sequence'),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT entries_snapshots_pk PRIMARY KEY (id)
);

-- Основной индекс для быстрого поиска последнего snapshot'а
CREATE INDEX idx_ entries_snapshots_account_date_ordinal
    ON ledger.entries_snapshots (account_id, operation_date DESC, snapshot_ordinal DESC);

-- Уникальный индекс предотвращающий дублирование snapshot'ов
CREATE UNIQUE INDEX idx_ entries_snapshots_unique_account_date_ordinal
    ON ledger.entries_snapshots (account_id, operation_date, snapshot_ordinal);

-- Индекс для поиска по last_entry_ordinal (для валидации)
CREATE INDEX idx_ entries_snapshots_last_entry_ordinal
    ON ledger.entries_snapshots (last_entry_ordinal);

-- Индекс для cleanup старых snapshot'ов
CREATE INDEX idx_ entries_snapshots_created_at
    ON ledger.entries_snapshots (created_at);

-- Индекс для мониторинга количества operations
CREATE INDEX idx_ entries_snapshots_operations_count
    ON ledger.entries_snapshots (operations_count DESC)
    WHERE operations_count > 0;

-- Foreign key reference (если нужно строгое consistency)
ALTER TABLE ledger.entries_snapshots
    ADD CONSTRAINT fk_ entries_snapshots_last_entry
        FOREIGN KEY (last_entry_record_id) REFERENCES ledger.entry_records(id);

-- Constraint для проверки корректности данных
ALTER TABLE ledger.entries_snapshots
    ADD CONSTRAINT entries_snapshots_operations_count_chk
        CHECK (operations_count >= 0);

ALTER TABLE ledger.entries_snapshots
    ADD CONSTRAINT entries_snapshots_last_ordinal_chk
        CHECK (last_entry_ordinal > 0);

COMMENT
ON TABLE ledger. entries_snapshots IS 'Account balance snapshots for performance optimization';
COMMENT
ON COLUMN ledger. entries_snapshots.id IS 'UUID v7 primary key';
COMMENT
ON COLUMN ledger. entries_snapshots.account_id IS 'Account identifier';
COMMENT
ON COLUMN ledger. entries_snapshots.operation_date IS 'Date of the snapshot';
COMMENT
ON COLUMN ledger. entries_snapshots.balance IS 'Account balance at snapshot creation time';
COMMENT
ON COLUMN ledger. entries_snapshots.last_entry_record_id IS 'Last processed entry record ID';
COMMENT
ON COLUMN ledger. entries_snapshots.last_entry_ordinal IS 'Last processed entry ordinal for fast filtering';
COMMENT
ON COLUMN ledger. entries_snapshots.operations_count IS 'Total operations processed at snapshot time';
COMMENT
ON COLUMN ledger. entries_snapshots.snapshot_ordinal IS 'Snapshot order for same date (multiple snapshots per day)';
COMMENT
ON COLUMN ledger. entries_snapshots.created_at IS 'Snapshot creation timestamp';

-- rollback
DROP TABLE IF EXISTS ledger.entries_snapshots CASCADE;
DROP SEQUENCE IF EXISTS ledger_snapshot_ordinal_sequence;