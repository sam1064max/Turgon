-- 02_silver_ddl.sql
-- Silver layer: cleansed, deduplicated, properly typed, validated.
-- Each row traces back to bronze via _bronze_row_hash.

CREATE TABLE IF NOT EXISTS silver.tickets (
    ticket_id           VARCHAR(30) PRIMARY KEY,
    created_at          TIMESTAMP,
    resolved_at         TIMESTAMP,
    category            VARCHAR(30),         -- Normalized to canonical categories
    priority            VARCHAR(10),         -- Critical, High, Medium, Low
    status              VARCHAR(20),
    building            VARCHAR(50),
    description         TEXT,
    submitted_by        VARCHAR(50),
    assigned_to         VARCHAR(50),
    resolution_notes    TEXT,
    cost                NUMERIC(12, 2),
    sla_hours           INTEGER,

    -- Derived columns
    resolution_hours    NUMERIC(10, 2),      -- (resolved_at - created_at) in hours
    is_sla_breached     BOOLEAN,             -- resolution_hours > sla_hours
    is_resolved         BOOLEAN,             -- status in (Resolved, Closed)

    -- Cleaning metadata
    _bronze_row_hash    TEXT NOT NULL,        -- Link back to bronze
    _cleaning_flags     JSONB DEFAULT '[]',  -- Array of rules applied to this row
    _cleaned_at         TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_silver_category ON silver.tickets(category);
CREATE INDEX IF NOT EXISTS idx_silver_priority ON silver.tickets(priority);
CREATE INDEX IF NOT EXISTS idx_silver_building ON silver.tickets(building);
CREATE INDEX IF NOT EXISTS idx_silver_created ON silver.tickets(created_at);
CREATE INDEX IF NOT EXISTS idx_silver_status ON silver.tickets(status);
