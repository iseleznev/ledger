-- liquibase formatted sql

-- changeset Igor_Seleznyov:20250901_001_add_immutable_columns
-- comment: Add columns for immutable data architecture

-- Add parent_id to WAL entries for status tracking
ALTER TABLE ledger.wal_entries
    ADD COLUMN parent_id UUID REFERENCES ledger.wal_entries(id),
ADD COLUMN is_current BOOLEAN NOT NULL DEFAULT true;

-- Index for finding current WAL entries
CREATE INDEX idx_wal_entries_current ON ledger.wal_entries (transaction_id, is_current) WHERE is_current = true;
CREATE INDEX idx_wal_entries_parent_id ON ledger.wal_entries (parent_id) WHERE parent_id IS NOT NULL;

-- Add correction tracking to entry records
ALTER TABLE ledger.entry_records
    ADD COLUMN original_entry_id UUID REFERENCES ledger.entry_records(id),
ADD COLUMN correction_type VARCHAR(20),
ADD COLUMN is_active BOOLEAN NOT NULL DEFAULT true;

-- Constraint for correction types
ALTER TABLE ledger.entry_records
    ADD CONSTRAINT entry_records_correction_type_chk
        CHECK (correction_type IS NULL OR correction_type IN ('STORNO', 'CORRECTION', 'ADJUSTMENT'));

-- Index for finding active entries
CREATE INDEX idx_entry_records_active ON ledger.entry_records (account_id, is_active, entry_ordinal DESC) WHERE is_active = true;
CREATE INDEX idx_entry_records_original ON ledger.entry_records (original_entry_id) WHERE original_entry_id IS NOT NULL;

-- Add versioning to snapshots (they're already immutable by design)
ALTER TABLE ledger.entries_snapshots
    ADD COLUMN is_current BOOLEAN NOT NULL DEFAULT true;

-- Index for current snapshots
CREATE INDEX idx_entries_snapshots_current ON ledger.entries_snapshots (account_id, is_current, operation_date DESC) WHERE is_current = true;

-- Create sequence for WAL entry versioning
CREATE SEQUENCE ledger_wal_version_sequence
    AS BIGINT
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 100;

-- Add version column to WAL entries
ALTER TABLE ledger.wal_entries
    ADD COLUMN version_number BIGINT NOT NULL DEFAULT nextval('ledger_wal_version_sequence');

-- Create index on version for ordering
CREATE INDEX idx_wal_entries_version ON ledger.wal_entries (transaction_id, version_number DESC);

COMMENT ON COLUMN ledger.wal_entries.parent_id IS 'Reference to previous version of this WAL entry';
COMMENT ON COLUMN ledger.wal_entries.is_current IS 'Flag indicating if this is the current version';
COMMENT ON COLUMN ledger.wal_entries.version_number IS 'Sequential version number for ordering';

COMMENT ON COLUMN ledger.entry_records.original_entry_id IS 'Reference to original entry for corrections/storno';
COMMENT ON COLUMN ledger.entry_records.correction_type IS 'Type of correction: STORNO, CORRECTION, ADJUSTMENT';
COMMENT ON COLUMN ledger.entry_records.is_active IS 'Flag indicating if entry is active (not cancelled)';

COMMENT ON COLUMN ledger.entries_snapshots.is_current IS 'Flag indicating if this is the current snapshot for the date';

-- rollback
DROP INDEX IF EXISTS idx_wal_entries_current;
DROP INDEX IF EXISTS idx_wal_entries_parent_id;
DROP INDEX IF EXISTS idx_wal_entries_version;
DROP INDEX IF EXISTS idx_entry_records_active;
DROP INDEX IF EXISTS idx_entry_records_original;
DROP INDEX IF EXISTS idx_entries_snapshots_current;

ALTER TABLE ledger.wal_entries DROP COLUMN IF EXISTS parent_id;
ALTER TABLE ledger.wal_entries DROP COLUMN IF EXISTS is_current;
ALTER TABLE ledger.wal_entries DROP COLUMN IF EXISTS version_number;

ALTER TABLE ledger.entry_records DROP COLUMN IF EXISTS original_entry_id;
ALTER TABLE ledger.entry_records DROP COLUMN IF EXISTS correction_type;
ALTER TABLE ledger.entry_records DROP COLUMN IF EXISTS is_active;

ALTER TABLE ledger.entries_snapshots DROP COLUMN IF EXISTS is_current;

DROP SEQUENCE IF EXISTS ledger_wal_version_sequence;