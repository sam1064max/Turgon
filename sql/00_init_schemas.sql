-- 00_init_schemas.sql
-- Creates separated schemas for each medallion layer + lineage tracking.
-- Runs on PostgreSQL startup via docker-entrypoint-initdb.d.

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS lineage;

-- Lineage: tracks every pipeline stage execution
CREATE TABLE IF NOT EXISTS lineage.pipeline_runs (
    run_id          SERIAL PRIMARY KEY,
    stage           VARCHAR(20) NOT NULL,          -- bronze, silver, gold
    started_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMP,
    rows_in         INTEGER,
    rows_out        INTEGER,
    rows_dropped    INTEGER DEFAULT 0,
    rows_deduped    INTEGER DEFAULT 0,
    status          VARCHAR(20) DEFAULT 'RUNNING', -- RUNNING, SUCCESS, FAILED
    error_message   TEXT,
    details         JSONB                          -- extra metadata per stage
);

-- Lineage: tracks transformations applied
CREATE TABLE IF NOT EXISTS lineage.transformations (
    id              SERIAL PRIMARY KEY,
    run_id          INTEGER REFERENCES lineage.pipeline_runs(run_id),
    source_table    VARCHAR(100) NOT NULL,
    target_table    VARCHAR(100) NOT NULL,
    rule_name       VARCHAR(100),
    rule_description TEXT,
    rows_affected   INTEGER,
    applied_at      TIMESTAMP DEFAULT NOW()
);

-- Agent proposals: human-in-the-loop approval gate
CREATE TABLE IF NOT EXISTS lineage.agent_proposals (
    proposal_id     SERIAL PRIMARY KEY,
    agent_name      VARCHAR(50) NOT NULL,
    proposal_type   VARCHAR(50) NOT NULL,           -- cleaning_rule, schema, aggregation
    proposal        JSONB NOT NULL,                  -- the actual proposal
    status          VARCHAR(20) DEFAULT 'PENDING',   -- PENDING, APPROVED, REJECTED
    created_at      TIMESTAMP DEFAULT NOW(),
    reviewed_at     TIMESTAMP,
    reviewer_notes  TEXT
);

-- Agent metrics: cost & performance tracking
CREATE TABLE IF NOT EXISTS lineage.agent_metrics (
    id              SERIAL PRIMARY KEY,
    agent_name      VARCHAR(50) NOT NULL,
    action          VARCHAR(100),
    tokens_in       INTEGER,
    tokens_out      INTEGER,
    cost_usd        NUMERIC(10, 6),
    latency_ms      INTEGER,
    model           VARCHAR(50),
    created_at      TIMESTAMP DEFAULT NOW()
);
