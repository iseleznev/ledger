-- liquibase formatted sql

-- changeset Igor_Seleznyov:20250828_001_create_table_wal_entries
-- comment: Create Write-Ahead Log table for hot account durability
CREATE TABLE ledger.wal_entries
(
    id                UUID PRIMARY KEY     DEFAULT gen_random_uuid(),
    sequence_number   BIGINT      NOT NULL,
    debit_account_id  UUID        NOT NULL,
    credit_account_id UUID        NOT NULL,
    amount            BIGINT      NOT NULL CHECK (amount > 0),
    currency_code     VARCHAR(3)  NOT NULL,
    operation_date    DATE        NOT NULL,
    transaction_id    UUID        NOT NULL,
    idempotency_key   UUID        NOT NULL,
    status            VARCHAR(20) NOT NULL CHECK (status IN ('PENDING', 'PROCESSING', 'PROCESSED', 'FAILED', 'RECOVERED')),
    created_at        TIMESTAMP   NOT NULL DEFAULT NOW(),
    processed_at      TIMESTAMP NULL,
    error_message     TEXT NULL,

    -- Constraints
    CONSTRAINT wal_entries_sequence_unique UNIQUE (sequence_number),
    CONSTRAINT wal_entries_idempotency_unique UNIQUE (idempotency_key)
);

-- Indexes for performance
-- Primary index for recovery queries (most critical)
CREATE INDEX idx_wal_entries_status_sequence ON ledger.wal_entries (status, sequence_number) WHERE status IN ('PENDING', 'PROCESSING');

-- Index for cleanup operations
CREATE INDEX idx_wal_entries_cleanup ON ledger.wal_entries (status, created_at) WHERE status IN ('PROCESSED', 'FAILED');

-- Index for monitoring and statistics
CREATE INDEX idx_wal_entries_created_at ON ledger.wal_entries (created_at);

-- Index for transaction lookup
CREATE INDEX idx_wal_entries_transaction_id ON ledger.wal_entries (transaction_id);

-- Comments for documentation
COMMENT
ON TABLE ledger.wal_entries IS 'Write-Ahead Log for hot account processing durability';
COMMENT
ON COLUMN ledger.wal_entries.sequence_number IS 'Sequential ordering from disruptor for recovery';
COMMENT
ON COLUMN ledger.wal_entries.status IS 'Processing status: PENDING -> PROCESSING -> PROCESSED/FAILED';
COMMENT
ON COLUMN ledger.wal_entries.idempotency_key IS 'Unique key to prevent duplicate processing';

-- rollback
DROP TABLE IF EXISTS ledger.wal_entries CASCADE;
