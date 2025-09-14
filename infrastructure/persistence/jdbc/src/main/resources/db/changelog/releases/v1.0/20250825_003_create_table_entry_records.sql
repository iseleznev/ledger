-- liquibase formatted sql

-- changeset Igor_Seleznyov:20250825_003_create_table_entry_records
-- comment: Create entry_records table with sequence and indexes
CREATE SEQUENCE ledger_entry_ordinal_sequence
    AS BIGINT
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE CACHE 100;

CREATE TABLE ledger.entry_records
(
    id              UUID        NOT NULL DEFAULT generate_uuid_v7_precise(),
    account_id      UUID        NOT NULL,
    transaction_id  UUID        NOT NULL,
    entry_type      VARCHAR(6)  NOT NULL,
    amount          BIGINT      NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    operation_date  DATE        NOT NULL,
    idempotency_key UUID        NOT NULL,
    currency_code   VARCHAR(8)  NOT NULL,
    entry_ordinal   BIGINT      NOT NULL DEFAULT nextval('ledger_entry_ordinal_sequence'),
    CONSTRAINT entry_records_pk PRIMARY KEY (id)
);

-- Constraint для проверки типа проводки
ALTER TABLE ledger.entry_records
    ADD CONSTRAINT entry_records_entry_type_chk
        CHECK (entry_type IN ('DEBIT', 'CREDIT'));

-- Основной индекс для быстрого поиска операций по аккаунту
CREATE INDEX idx_entry_records_account_operation_ordinal
    ON ledger.entry_records (account_id, operation_date DESC, entry_ordinal DESC);

-- Индекс для проверки идемпотентности
CREATE INDEX idx_entry_records_idempotency
    ON ledger.entry_records (idempotency_key) WHERE idempotency_key IS NOT NULL;

-- Дополнительный индекс для быстрого поиска по entry_ordinal
CREATE INDEX idx_entry_records_ordinal
    ON ledger.entry_records (entry_ordinal);

-- Индекс для быстрого поиска по transaction_id
CREATE INDEX idx_entry_records_transaction_id
    ON ledger.entry_records (transaction_id);

-- Составной индекс для оптимизации snapshot queries
CREATE INDEX idx_entry_records_account_ordinal_type
    ON ledger.entry_records (account_id, entry_ordinal, entry_type) INCLUDE (amount);

COMMENT
ON TABLE ledger.entry_records IS 'Double-entry bookkeeping records';
COMMENT
ON COLUMN ledger.entry_records.id IS 'UUID v7 primary key with timestamp ordering';
COMMENT
ON COLUMN ledger.entry_records.account_id IS 'Account identifier';
COMMENT
ON COLUMN ledger.entry_records.transaction_id IS 'Transaction identifier linking DEBIT/CREDIT pair';
COMMENT
ON COLUMN ledger.entry_records.entry_type IS 'Entry type: DEBIT or CREDIT';
COMMENT
ON COLUMN ledger.entry_records.amount IS 'Amount in minor currency units (cents)';
COMMENT
ON COLUMN ledger.entry_records.operation_date IS 'Business date of the operation';
COMMENT
ON COLUMN ledger.entry_records.idempotency_key IS 'Key for ensuring operation idempotency';
COMMENT
ON COLUMN ledger.entry_records.entry_ordinal IS 'Sequential ordering for consistent snapshot boundaries';

-- rollback
DROP TABLE IF EXISTS ledger.entry_records CASCADE;
DROP SEQUENCE IF EXISTS ledger_entry_ordinal_sequence;