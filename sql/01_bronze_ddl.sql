-- 01_bronze_ddl.sql
-- Bronze layer: raw ingestion, schema-on-read.
-- All business columns are TEXT — no type coercion at this stage.
-- Lineage metadata columns prefixed with underscore.

CREATE TABLE IF NOT EXISTS bronze.raw_tickets (
    -- Original columns (all TEXT for schema-on-read)
    ticket_id           TEXT,
    created_at          TEXT,
    resolved_at         TEXT,
    category            TEXT,
    priority            TEXT,
    status              TEXT,
    building            TEXT,
    description         TEXT,
    submitted_by        TEXT,
    assigned_to         TEXT,
    resolution_notes    TEXT,
    cost                TEXT,
    sla_hours           TEXT,

    -- Lineage metadata
    _source_file        TEXT NOT NULL,
    _ingested_at        TIMESTAMP NOT NULL DEFAULT NOW(),
    _row_hash           TEXT NOT NULL,       -- SHA-256 of full row for dedup
    _raw_row_number     INTEGER NOT NULL,    -- 1-indexed position in source CSV

    -- Dedup constraint: same row hash = same data, skip re-insert
    CONSTRAINT uq_bronze_row_hash UNIQUE (_row_hash)
);

CREATE INDEX IF NOT EXISTS idx_bronze_ticket_id ON bronze.raw_tickets(ticket_id);
CREATE INDEX IF NOT EXISTS idx_bronze_ingested_at ON bronze.raw_tickets(_ingested_at);
