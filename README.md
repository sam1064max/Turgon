# Turgon: Medallion Pipeline + Agentic Acceleration

[![CI Pipeline](https://github.com/sam1064max/Turgon/actions/workflows/ci.yml/badge.svg)](https://github.com/sam1064max/Turgon/actions/workflows/ci.yml)
[![Python Version](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/release/python-3110/)
[![Docker](https://img.shields.io/badge/docker-ready-brightgreen.svg)](https://www.docker.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-App-FF4B4B.svg)](http://localhost:8501)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A production-quality **bronze → silver → gold** data pipeline for messy operational support tickets (~10k rows), with autonomous AI agents that accelerate pipeline construction and an interactive **Streamlit Analytics Dashboard**.

## 🌟 Key Features
- **Medallion Architecture**: Fully isolated `bronze`, `silver`, `gold`, and `lineage` schemas in PostgreSQL.
- **Robust 15-Rule Cleansing**: Multi-format date parsing, category canonicalization, column swap detection, priority mapping, sentinel resolution, and submitter name normalization.
- **AI Agentic Acceleration**: Data Quality Profiler Agent, Semantic Category Classifier, and Gold Schema Design Agent.
- **Interactive Dashboard**: Streamlit frontend for visual lineage inspection, data quality analytics, and AI agent testing.
- **CI/CD Integration**: Automated GitHub Actions workflow for linting and test execution.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                          docker-compose                              │
│                                                                      │
│  ┌──────────────┐     ┌───────────────────────────────────────────┐  │
│  │  PostgreSQL   │     │           Python Pipeline                 │  │
│  │               │     │                                           │  │
│  │  bronze.*  ◄──┼─────┤  1. bronze.py    (raw ingestion)          │  │
│  │  silver.*  ◄──┼─────┤  2. silver.py    (cleanse & transform)    │  │
│  │  gold.*    ◄──┼─────┤  3. gold.py      (aggregations)           │  │
│  │               │     │                                           │  │
│  │  lineage.* ◄──┼─────┤  4. AI Agents (OpenAI API)               │  │
│  │               │     │     ├─ data_quality_agent.py              │  │
│  │               │     │     ├─ semantic_classifier.py             │  │
│  │               │     │     └─ gold_design_agent.py               │  │
│  └──────────────┘     └───────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
```

### Schema Separation

Each medallion layer lives in a separate PostgreSQL schema:

| Schema | Purpose | Tables |
|--------|---------|--------|
| `bronze` | Raw ingestion, all TEXT columns, zero data loss | `raw_tickets` |
| `silver` | Cleansed, typed, deduplicated, validated | `tickets` |
| `gold` | Business-ready aggregations | `sla_performance`, `vendor_performance`, `ticket_volume_trends` |
| `lineage` | Pipeline tracking & governance | `pipeline_runs`, `transformations`, `agent_proposals`, `agent_metrics` |

### Data Flow

```
raw_tickets.csv
    │
    ▼
[Bronze] Raw ingestion + lineage metadata
    │     - All columns as TEXT (schema-on-read)
    │     - SHA-256 row hash for dedup
    │     - Source file, timestamp, row number
    │
    ▼
[Silver] 15 cleaning rules applied
    │     - 6+ date formats parsed (including Unix epoch)
    │     - ~60 categories → 10 canonical groups
    │     - 20 priority variants → 4 levels
    │     - Costs cleaned ($, TBD, -1 sentinels)
    │     - Dedup, junk removal, name normalization
    │
    ▼
[Gold] 3 business aggregations
      - SLA Performance (by category/building/priority)
      - Vendor Scorecards (resolution time, cost, SLA rate)
      - Ticket Volume Trends (weekly, for planning)
```

---

## How to Run

### Prerequisites
- Docker & Docker Compose
- OpenAI API key (for agents; pipeline works without it)

### Quick Start

```bash
# 1. Clone and configure
cp .env.example .env
# Edit .env: add your OPENAI_API_KEY (optional)

# 2. Run the full pipeline + Streamlit dashboard
docker-compose up --build

# Access the Streamlit Analytics Dashboard at: http://localhost:8501

# 3. (Optional) Run agents separately
docker-compose run pipeline python scripts/run_agents.py
```

### Run Locally (without Docker)

```bash
# Start PostgreSQL (e.g., via Docker)
docker-compose up -d db

# Install dependencies
pip install -r requirements.txt

# Run pipeline
python scripts/run_pipeline.py

# Launch Streamlit Frontend Dashboard
streamlit run app.py

# Run agents
python scripts/run_agents.py

# Run test suite
pytest tests/ -v
```

### CLI Options

```bash
# Run specific pipeline stages
python scripts/run_pipeline.py --stage bronze
python scripts/run_pipeline.py --stage silver
python scripts/run_pipeline.py --stage gold

# Run specific agents
python scripts/run_agents.py --agent quality    # Data Quality Agent
python scripts/run_agents.py --agent classify   # Semantic Classifier
python scripts/run_agents.py --agent gold       # Gold Design Agent
```

---

## Part 1: Medallion Pipeline

### Bronze Layer

**Principle**: Schema-on-read, zero data loss, full lineage.

- All 13 columns stored as `TEXT` — no type coercion
- Lineage columns added: `_source_file`, `_ingested_at`, `_row_hash`, `_raw_row_number`
- **Idempotent**: SHA-256 row hash prevents duplicate inserts on re-runs
- Encoding handled at read time (`utf-8` with error replacement)

### Silver Layer

**15 cleaning rules**, each documented with rationale:

| # | Rule | What It Does | Why |
|---|------|-------------|-----|
| 1 | Parse dates | Handle ISO, US, EU text, AM/PM, Unix epoch (6+ formats) | Dates are the most inconsistent column |
| 2 | Normalize categories | Map ~60 raw values → 10 canonical groups | `hvac`, `HVAC`, `A/C`, `AC`, `air conditioning` all mean the same thing |
| 3 | Fix category/description swaps | 33 rows have full descriptions in the category field | Column data was entered in the wrong field |
| 4 | Normalize priorities | Map 20 variants → 4 levels (Critical/High/Medium/Low) | `lo`, `hi`, `crit`, `urgent!!!`, `MED` → consistent levels |
| 5 | Clean costs | Strip `$`, null out `TBD`/`error`/`-1` sentinels | Sentinel values aren't real costs |
| 6 | Clean SLA hours | Remove `-1`/`0`/`999` sentinels | No 0-hour or 999-hour SLA is real |
| 7 | Validate temporal logic | Flag `resolved_at < created_at` (don't drop) | Data entry errors — kept for investigation |
| 8 | Deduplicate | By row hash (exact dupes) and ticket_id (keep first) | 21 duplicate ticket IDs in raw data |
| 9 | Remove junk rows | Filter test/admin/system submitters + DELETE ME categories | 36 non-real tickets |
| 10 | Normalize submitter names | `J. Smith` → `John Smith`, `bob martinez` → `Robert Martinez` | Same person appears 3 different ways |
| 11 | Clean status values | `unknown`/`???` → NULL | Not real statuses |
| 12 | Clean building values | `unknown`/`???` → NULL | Not real building names |
| 13 | Handle null ticket_ids | Generate `TKT-SYNTH-0001` etc. | 22 rows with missing IDs |
| 14 | Fix encoding | `â€\"` → `—` (em-dash) | UTF-8 mojibake from source system |
| 15 | Compute derived fields | `resolution_hours`, `is_sla_breached`, `is_resolved` | Pre-calculated for gold layer aggregations |

**Idempotent**: Truncates silver table and rebuilds from bronze on each run.

### Gold Layer

Three aggregation models, each justified by stakeholder needs:

#### 1. `gold.sla_performance`
**Audience**: Operations managers
**Purpose**: SLA breach rates by category × building × priority

| Column | Description |
|--------|-------------|
| `breach_rate_pct` | Percentage of tickets that missed their SLA target |
| `avg_resolution_hrs` | Average time to resolve |
| `median_resolution_hrs` | Median resolution time (resistant to outliers) |

**Why this matters**: Ops managers need to know which building/category combinations consistently miss SLAs to prioritize corrective action.

#### 2. `gold.vendor_performance`
**Audience**: Procurement team
**Purpose**: Vendor scorecards for contract negotiations

| Column | Description |
|--------|-------------|
| `avg_resolution_hrs` | How fast the vendor resolves tickets |
| `avg_cost` / `total_cost` | Cost efficiency |
| `sla_breach_rate_pct` | Vendor's SLA compliance |
| `top_category` | What type of work this vendor handles most |

**Why this matters**: Procurement needs objective data to evaluate vendor contracts and decide renewals.

#### 3. `gold.ticket_volume_trends`
**Audience**: Facilities planning
**Purpose**: Weekly ticket volumes for resource forecasting

| Column | Description |
|--------|-------------|
| `tickets_opened` / `tickets_resolved` | Volume per week |
| `resolution_rate_pct` | Are we keeping up? |
| `avg_cost` | Budget planning input |

**Why this matters**: Facilities teams need to forecast staffing and budget based on historical ticket patterns and seasonal trends.

---

## Part 2: Agentic Acceleration

### Agent Architecture

All agents inherit from `BaseAgent`, which provides:
- OpenAI client with structured JSON output
- Token/cost tracking per call and cumulative
- Metrics logged to `lineage.agent_metrics`
- Proposals saved to `lineage.agent_proposals` (human-in-the-loop gate)

### Agent 1: Data Quality Agent

**What it does**: Profiles bronze data and proposes cleaning rules with natural-language explanations + generated Python/SQL code.

**Input**: Bronze table sample (200 rows) + computed column statistics (null rates, unique values, distributions)

**Output** (sample):
```json
{
  "rules": [
    {
      "rule_name": "normalize_date_formats",
      "column": "created_at",
      "issue_description": "6+ date formats found: ISO 8601, MM/DD/YYYY, DD-Mon-YYYY, Unix epoch",
      "why_it_matters": "Inconsistent dates prevent time-series analysis and SLA calculations",
      "severity": "critical",
      "implementation_python": "df['created_at'] = df['created_at'].apply(parse_multiformat_date)"
    }
  ],
  "data_quality_score": 35
}
```

**Honest assessment**: **This saved me ~30 minutes** of initial data profiling. The agent correctly identified all major issues (date formats, category chaos, sentinel values, encoding problems). However, I still had to manually verify each rule and refine the implementation. The agent's generated Python code was ~80% correct but needed edge case handling.

### Agent 2: Semantic Classification Agent

**What it does**: Normalizes ~60 raw category values into 10 canonical groups using LLM understanding of synonyms and abbreviations.

**Input**: List of unique raw category values + target taxonomy

**Output** (sample):
```json
{
  "mappings": {
    "hvac": "HVAC",
    "A/C": "HVAC",
    "air conditioning": "HVAC",
    "heating cooling": "HVAC",
    "elevator/escalator": "Elevator",
    "Badge/Access": "Security"
  }
}
```

**Honest assessment**: **This was the most valuable agent**. Building the 60-value category mapping manually would have taken 15-20 minutes and been error-prone. The LLM classified everything correctly in one batch, including tricky cases like `"Vertical Transport" → "Elevator"` and detecting that `"need more outlets in conf room 387"` is a description, not a category. **Cost: ~$0.001** (a few hundred tokens).

At scale (10M rows), this is even more valuable — only unique values need classification, so cost stays flat while manual effort would scale with new variant discovery.

### Agent 3: Gold Layer Design Agent

**What it does**: Given the silver schema + business domain description, proposes gold-layer aggregation models with complete SQL.

**Input**: Silver table DDL + sample data + domain context ("facility management support tickets")

**Output** (sample):
```json
{
  "models": [
    {
      "model_name": "gold.sla_performance",
      "description": "SLA breach rates aggregated by category, building, and priority",
      "business_value": "Enables ops managers to identify systemic SLA failures",
      "sql_create": "CREATE TABLE gold.sla_performance (...)",
      "sql_populate": "INSERT INTO gold.sla_performance SELECT ..."
    }
  ]
}
```

**Honest assessment**: **Mixed value**. The agent proposed reasonable aggregations — similar to what I built manually. The SQL was syntactically correct but needed tweaks for edge cases (null handling, grouping behavior). The main value was speed: it generated 3-4 model proposals in 10 seconds that took me ~20 minutes to design manually. **However**, I wouldn't trust the agent's business logic choices without domain expertise review — it tends to propose "obvious" aggregations rather than insightful ones.

---

## What Changes at 100x Scale (1M+ rows, daily incremental loads)

### Pipeline Changes

| Component | Current (10k) | At Scale (1M+) |
|-----------|---------------|-----------------|
| **Bronze ingestion** | Full CSV load via pandas | Streaming ingestion (chunked reads), partitioned by `ingested_at` date |
| **Silver transform** | Truncate & reload | **Incremental**: process only new bronze rows since last run via `_ingested_at` watermark |
| **Gold aggregation** | Truncate & reload | **Incremental merge**: update aggregates with deltas, not full recompute |
| **Deduplication** | In-memory hash set | Bloom filter for approximate dedup, or database-side `ON CONFLICT` |
| **Date parsing** | Python `dateutil` per row | Pre-compiled regex patterns, vectorized operations with `pd.to_datetime` |
| **Storage** | Single PostgreSQL table | Partitioned tables by date range, columnar storage (e.g., TimescaleDB or Parquet) |

### Agent Changes

| Agent | Current | At Scale |
|-------|---------|----------|
| **Data Quality** | Profiles all rows | Statistical sampling (10k sample from 1M+), cached profiles |
| **Semantic Classifier** | Classifies all unique values | Same cost — only unique values matter (~200 regardless of row count) |
| **Gold Design** | One-shot design | Schema evolution detection: re-propose when new columns appear |

### Infrastructure Changes

- **Orchestration**: Replace scripts with Airflow/Dagster for scheduling, retries, and dependency management
- **Database**: PostgreSQL → TimescaleDB (time-series optimized) or move gold to a warehouse (BigQuery/Snowflake)
- **Incremental strategy**: CDC (Change Data Capture) from source systems rather than file-based ingestion
- **Late-arriving records**: Replay window of 7 days — re-process any records with `created_at` in the last week
- **Monitoring**: Pipeline SLAs (must complete within 30 min), row count anomaly detection (±20% alerts)

---

## Good-to-Have Features Implemented

### ✅ Data Lineage
Every pipeline run records to `lineage.pipeline_runs`:
- Stage, start/end times, row counts in/out/dropped/deduped, status
- Individual transformations logged to `lineage.transformations`
- Bronze rows linked to silver via `_bronze_row_hash`

### ✅ Agent Evaluation Harness
- Token usage and cost tracked per LLM call in `lineage.agent_metrics`
- Agent proposals stored in `lineage.agent_proposals` with PENDING/APPROVED/REJECTED status
- Cost summary printed after each agent run

### ✅ Human-in-the-Loop Approval Gates
- Agent proposals saved with `status = 'PENDING'`
- Proposals include full context (rule description, generated code, confidence scores)
- Pipeline can be configured to wait for approval before applying agent suggestions

### ✅ Observability
- Structured logging at every stage with timestamps and row counts
- Pipeline run history queryable from `lineage.pipeline_runs`
- Agent cost tracking across all models

---

## Project Structure

```
turgon/
├── docker-compose.yml          # PostgreSQL + pipeline service
├── Dockerfile                  # Python 3.11 container
├── requirements.txt            # pandas, sqlalchemy, openai, etc.
├── .env.example                # Environment template
├── data/
│   └── raw_tickets.csv         # Original dataset (never modified)
├── config/
│   └── settings.py             # Configuration from env vars
├── pipeline/
│   ├── bronze.py               # Raw ingestion + lineage
│   ├── silver.py               # 15-rule cleaning pipeline
│   ├── gold.py                 # 3 business aggregations
│   ├── lineage.py              # Run/transformation tracking
│   └── utils.py                # Date parsing, normalization, DB helpers
├── agents/
│   ├── base_agent.py           # LLM client, cost tracking, proposals
│   ├── data_quality_agent.py   # Profiles data, proposes rules
│   ├── semantic_classifier.py  # Normalizes categories via LLM
│   └── gold_design_agent.py    # Proposes gold aggregations
├── sql/
│   ├── 00_init_schemas.sql     # Schema + lineage tables
│   ├── 01_bronze_ddl.sql       # Bronze table (all TEXT)
│   ├── 02_silver_ddl.sql       # Silver table (typed)
│   └── 03_gold_ddl.sql         # Gold tables
├── scripts/
│   ├── run_pipeline.py         # Main: bronze → silver → gold
│   └── run_agents.py           # Run agents independently
└── tests/
    └── test_pipeline.py        # Unit tests for cleaning functions
```

---

## Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Language | Python | 3.11 |
| Database | PostgreSQL | 16 |
| AI Provider | OpenAI | GPT-4o-mini |
| Data Processing | pandas | 2.0+ |
| DB Access | SQLAlchemy + psycopg2 | 2.0+ |
| Container | Docker + Docker Compose | - |
| Testing | pytest | 8.0+ |
