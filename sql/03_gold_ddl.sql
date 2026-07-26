-- 03_gold_ddl.sql
-- Gold layer: business-ready aggregations.

-- 1. SLA Performance: breach rates by category, building, priority
CREATE TABLE IF NOT EXISTS gold.sla_performance (
    category            VARCHAR(30),
    building            VARCHAR(50),
    priority            VARCHAR(10),
    total_tickets       INTEGER,
    resolved_tickets    INTEGER,
    sla_breached        INTEGER,
    sla_met             INTEGER,
    breach_rate_pct     NUMERIC(5, 2),
    avg_resolution_hrs  NUMERIC(10, 2),
    median_resolution_hrs NUMERIC(10, 2),
    _refreshed_at       TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (category, building, priority)
);

-- 2. Vendor Performance: scorecards per assigned vendor/team
CREATE TABLE IF NOT EXISTS gold.vendor_performance (
    assigned_to         VARCHAR(50),
    total_tickets       INTEGER,
    resolved_tickets    INTEGER,
    avg_resolution_hrs  NUMERIC(10, 2),
    avg_cost            NUMERIC(12, 2),
    total_cost          NUMERIC(14, 2),
    sla_breach_rate_pct NUMERIC(5, 2),
    top_category        VARCHAR(30),       -- most common category for this vendor
    _refreshed_at       TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (assigned_to)
);

-- 3. Ticket Volume Trends: weekly aggregations for planning
CREATE TABLE IF NOT EXISTS gold.ticket_volume_trends (
    week_start          DATE,
    category            VARCHAR(30),
    building            VARCHAR(50),
    tickets_opened      INTEGER,
    tickets_resolved    INTEGER,
    resolution_rate_pct NUMERIC(5, 2),
    avg_cost            NUMERIC(12, 2),
    avg_resolution_hrs  NUMERIC(10, 2),
    _refreshed_at       TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (week_start, category, building)
);
