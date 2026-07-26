# Implementation Plan: Turgon Medallion Pipeline + Agentic Acceleration

## Goal

Build a production-quality medallion pipeline (bronze → silver → gold) for ~10k messy operational support tickets, with AI agents that accelerate pipeline construction. Deliverable: GitHub repo with `docker-compose up` + single-command execution.

---

## Data Analysis Summary

> [!IMPORTANT]
> The dataset has **extensive, realistic messiness** across every column. Understanding these patterns drives all cleaning rules and agent design.

| Issue | Details | Count/Impact |
|---|---|---|
| **Date formats** | 6+ formats: ISO 8601, `MM/DD/YYYY`, `DD-Mon-YYYY HH:MM`, `MM/DD/YYYY HH:MM AM/PM`, **Unix epoch timestamps** | All 10,280 rows affected |
| **Category chaos** | ~60 raw values → ~8 canonical groups. Mixed case (`HVAC`, `hvac`, `A/C`, `AC`, `air conditioning`, `heating cooling`, `Climate Control`). 33 rows have **descriptions in the category field** (column swap) | 33 swapped, all need normalization |
| **Priority inconsistency** | `LOW`, `Low`, `lo`, `med`, `MED`, `Medium`, `hi`, `high`, `HIGH`, `crit`, `critical`, `CRITICAL`, `urgent!!!`, `???`, `Normal` | 20 distinct values → 4 canonical |
| **Cost messiness** | `$`-prefixed (993), `TBD` (484), `error` (12), `-1` sentinel (192), nulls (3,137) | ~4,818 need cleaning |
| **SLA sentinel values** | `-1` (9), `0` (484), `999` (557), `N/A` (stored as NaN alongside 3,086 real nulls) | ~1,050 invalid |
| **Status values** | `unknown` and `???` as invalid statuses | Small count but needs handling |
| **Duplicate tickets** | 21 duplicate `ticket_id` values | Dedup required |
| **Null ticket_ids** | 22 rows with no ticket ID | Need synthetic IDs |
| **Submitter name variants** | Same person appears as `John Smith`, `john smith`, `J. Smith`, `J. Doe`, `Jane Doe`, `jane doe` — ~36 variants for ~12 actual people | Normalization needed |
| **Test/junk rows** | `submitted_by` = `test` (11), `admin` (7), `system` (5). `category` = `DELETE ME` (5), `test` (4), `asdf` (4) | 36 junk rows |
| **Temporal anomalies** | `resolved_at` before `created_at` in some rows | Validation needed |
| **Building values** | `unknown` and `???` as invalid buildings | Small count |

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     docker-compose                            │
│                                                               │
│  ┌─────────────┐    ┌──────────────────────────────────────┐ │
│  │  PostgreSQL  │    │         Python Pipeline              │ │
│  │             │    │                                      │ │
│  │  bronze.*   │◄───│  1. bronze_ingest.py                 │ │
│  │  silver.*   │◄───│  2. silver_transform.py              │ │
│  │  gold.*     │◄───│  3. gold_aggregate.py                │ │
│  │             │    │                                      │ │
│  │  lineage.*  │◄───│  4. Agents (OpenAI API)              │ │
│  │             │    │     ├── data_quality_agent.py         │ │
│  │             │    │     ├── semantic_classification.py    │ │
│  │             │    │     └── gold_design_agent.py          │ │
│  └─────────────┘    └──────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
```

### Technology Choices

| Component | Choice | Rationale |
|---|---|---|
| **Database** | PostgreSQL (in Docker) | Production-realistic, supports schemas for layer separation, mature ecosystem |
| **Language** | Python | Preferred per instructions, pandas for transforms, psycopg2/SQLAlchemy for DB |
| **AI Provider** | OpenAI (GPT-4o-mini for cost efficiency, GPT-4o for complex reasoning) | Best structured output support, widely available |
| **Schema separation** | PostgreSQL schemas: `bronze`, `silver`, `gold` | Clean namespace isolation, single DB, clear layer boundaries |
| **Orchestration** | Simple Python scripts (no framework) | Clean, readable, minimal — assessment values no over-engineering |

---

## Project Structure

```
turgon/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
├── README.md
├── data/
│   └── raw_tickets.csv              # Original file (renamed, never modified)
├── config/
│   └── settings.py                  # Configuration, env vars
├── pipeline/
│   ├── __init__.py
│   ├── bronze.py                    # Raw ingestion + lineage metadata
│   ├── silver.py                    # Cleaning, dedup, typing, validation
│   ├── gold.py                      # Business aggregations
│   ├── lineage.py                   # Lineage tracking utilities
│   └── utils.py                     # Shared helpers (date parsing, logging)
├── agents/
│   ├── __init__.py
│   ├── base_agent.py                # Shared agent interface + LLM client
│   ├── data_quality_agent.py        # Profiles bronze, proposes cleaning rules
│   ├── semantic_classifier.py       # Normalizes categories via LLM
│   └── gold_design_agent.py         # Proposes gold aggregations
├── sql/
│   ├── 00_init_schemas.sql          # Create bronze/silver/gold schemas
│   ├── 01_bronze_ddl.sql            # Bronze table DDL
│   ├── 02_silver_ddl.sql            # Silver table DDL
│   └── 03_gold_ddl.sql              # Gold table DDL
├── scripts/
│   ├── run_pipeline.py              # Main entry point: runs bronze → silver → gold
│   └── run_agents.py                # Runs agents independently
├── tests/
│   └── test_pipeline.py             # Basic tests for idempotency, dedup
└── docs/
    └── architecture.md              # Architecture diagram (also in README)
```

---

## Part 1 — Medallion Pipeline (50%)

### Bronze Layer

**Goal**: Raw ingestion, schema-on-read, zero data loss, lineage metadata.

#### [NEW] `sql/01_bronze_ddl.sql`
- All columns as `TEXT` (schema-on-read)
- Lineage columns: `_source_file`, `_ingested_at`, `_row_hash` (SHA-256 of entire row for dedup), `_raw_row_number`

#### [NEW] `pipeline/bronze.py`
- Read CSV with pandas (handle encoding issues with the `â€"` characters)
- Compute SHA-256 row hash for each row
- Add lineage metadata: source filename, ingestion timestamp, row number
- UPSERT into `bronze.raw_tickets` (idempotent via `_row_hash`)
- Log: rows ingested, rows skipped (already exists), duration

### Silver Layer

**Goal**: Cleansed, deduplicated, typed, validated. Document every cleaning rule.

#### [NEW] `sql/02_silver_ddl.sql`
- Properly typed columns: `TIMESTAMP`, `VARCHAR`, `NUMERIC`, `INTEGER`
- Constraints where appropriate
- Cleaning metadata: `_cleaning_flags` (JSON array of rules applied per row)

#### [NEW] `pipeline/silver.py`
Cleaning rules (documented with rationale):

| # | Rule | Implementation | Why |
|---|---|---|---|
| 1 | **Parse dates** | Handle 6+ formats including Unix epoch. Use `dateutil.parser` with fallback chain | Dates are in ISO, US, European, epoch, and AM/PM formats |
| 2 | **Normalize categories** | Map ~60 raw values → 8 canonical: `HVAC`, `Electrical`, `Plumbing`, `Fire Safety`, `Elevator`, `Security`, `Cleaning/Janitorial`, `IT/Network`, `Pest Control`, `General/Other` | Inconsistent casing, abbreviations, and synonyms |
| 3 | **Fix category/description swaps** | If `len(category) > 50` or category looks like a sentence → move to description, infer category from text | 33 rows have descriptions in category field |
| 4 | **Normalize priorities** | Map to 4 canonical: `Critical`, `High`, `Medium`, `Low`. Handle `lo`→`Low`, `hi`→`High`, `crit`→`Critical`, `med`→`Medium`, `urgent!!!`→`Critical`, `Normal`→`Medium`, `???`→`NULL` | 20 inconsistent values |
| 5 | **Clean costs** | Strip `$` prefix, convert to numeric. `TBD`→`NULL`, `error`→`NULL`, `-1`→`NULL`, `N/A`→`NULL` | Sentinel values aren't real costs |
| 6 | **Clean SLA hours** | `-1`→`NULL`, `0`→`NULL` (no 0-hour SLA makes sense), `999`→`NULL` (obvious sentinel), `N/A`→`NULL` | Sentinel/placeholder values |
| 7 | **Validate temporal logic** | Flag rows where `resolved_at < created_at` (don't drop, flag) | Data entry errors |
| 8 | **Deduplicate** | Drop exact duplicates by `_row_hash`. For duplicate `ticket_id`, keep latest `_ingested_at` | 21 duplicate ticket IDs |
| 9 | **Remove test/junk rows** | Filter `submitted_by` in (`test`, `admin`, `system`) AND `category` in (`DELETE ME`, `test`, `asdf`) | Clearly not real tickets |
| 10 | **Normalize submitter names** | Standardize to canonical form: `John Smith`, `Jane Doe`, etc. Build a mapping from initials/lowercase variants | Same person appears 3 different ways |
| 11 | **Clean status values** | `unknown`→`NULL`, `???`→`NULL` | Not real statuses |
| 12 | **Clean building values** | `unknown`→`NULL`, `???`→`NULL` | Not real building names |
| 13 | **Handle null ticket_ids** | Generate synthetic IDs: `TKT-SYNTH-{n}` | 22 rows with missing IDs |
| 14 | **Fix encoding artifacts** | Replace `â€"` with `—` (em-dash), clean Unicode garbage | Encoding issues in descriptions |
| 15 | **Validate assigned_to** | Keep as-is (7 canonical vendors), null is valid (unassigned) | Clean data, no changes needed |

### Gold Layer

**Goal**: Business-ready aggregations. 3 models, justified.

#### [NEW] `sql/03_gold_ddl.sql` + `pipeline/gold.py`

| Gold Model | What | Business Value |
|---|---|---|
| **`gold.sla_performance`** | SLA breach rate by category, building, priority. Calculated as `(resolved_at - created_at) > sla_hours` | Ops managers need to know which areas/categories are consistently missing SLAs |
| **`gold.vendor_performance`** | Per-vendor: ticket count, avg resolution time, avg cost, SLA breach rate | Procurement needs vendor scorecards for contract negotiations |
| **`gold.ticket_volume_trends`** | Weekly/monthly ticket volume by category, building. Includes resolution rate and avg cost | Facilities planning: where to invest, seasonal patterns, resource allocation |

---

## Part 2 — Agentic Acceleration (50%)

### Agent Selection

I'm building **3 of the 4 offered agents** — (b) Data Quality Agent, (c) Semantic Classification Agent, and (d) Gold Layer Design Agent. These three provide the most real-world value for this dataset:

| Agent | Why Selected |
|---|---|
| **Data Quality Agent** | The dataset has ~15 distinct data quality issues. An agent that profiles and proposes rules saves significant manual analysis time |
| **Semantic Classification Agent** | The ~60 category variants → 8 canonical groups is a perfect LLM task. Manual mapping is tedious and brittle |
| **Gold Layer Design Agent** | Given the silver schema + "operational support tickets" domain, the agent can propose useful aggregations. Good for testing LLM reasoning about business value |

### Agent Architecture

```
┌─────────────────────────────────────────────────┐
│               Base Agent Interface               │
│  - LLM client (OpenAI)                          │
│  - Structured output parsing                     │
│  - Cost tracking (tokens in/out)                │
│  - Retry logic + error handling                  │
│  - Logging                                       │
└──────────────┬──────────────────────────────────┘
               │
    ┌──────────┼──────────────┐
    ▼          ▼              ▼
┌────────┐ ┌────────────┐ ┌────────────┐
│ Data   │ │  Semantic   │ │ Gold Layer │
│Quality │ │Classif.     │ │  Design    │
│ Agent  │ │  Agent      │ │  Agent     │
└────────┘ └────────────┘ └────────────┘
```

---

### (b) Data Quality Agent

**What it does**: Profiles bronze data, proposes cleaning/validation rules in natural language, generates Python/SQL to implement them.

**Flow**:
1. Agent receives a sample of bronze data (100-200 rows, stratified)
2. Agent also receives column statistics (null rates, unique counts, value distributions)
3. Agent proposes cleaning rules as structured JSON: `{ rule_name, column, description, why_it_matters, implementation_sql }`
4. Human reviews proposals before they're applied (approval gate)

**Prompt design**:
- System prompt establishes agent as a "senior data engineer reviewing raw data quality"
- Context includes: column stats, sample values, known domain ("operational support tickets for facility management")
- Output format: JSON array of rule objects with `explanation` and `implementation` fields
- Guardrails: "Do not propose dropping rows unless clearly junk. Prefer nulling invalid values over imputation."

**Scale thinking**:
- At 10M rows: Sample-based profiling (statistically significant sample), not full scan
- Cache profiling results between runs
- Batch rule generation (all rules in one LLM call, not per-column)

---

### (c) Semantic Classification Agent

**What it does**: Normalizes the ~60 messy category values into canonical groups using LLM understanding of synonyms/abbreviations.

**Flow**:
1. Extract unique category values from bronze
2. Send to LLM in batches (20-30 per call) with target canonical categories
3. LLM returns mapping: `{ raw_value → canonical_category }`
4. Cache the mapping — same raw values don't need re-classification
5. Apply mapping during silver transform

**Prompt design**:
- Provide the target taxonomy: `[HVAC, Electrical, Plumbing, Fire Safety, Elevator, Security, Cleaning/Janitorial, IT/Network, Pest Control, General/Other]`
- Include examples of tricky mappings: "`A/C` → `HVAC`", "`Badge/Access` → `Security`"
- For swapped columns (description in category field): "If the value looks like a full sentence, classify based on content"
- Output: JSON mapping with confidence scores

**Scale thinking**:
- At 10M rows: Only unique values need classification (likely still <200 unique categories)
- Cache mapping table persistently — new values only classified if unseen
- Cost: ~1-2 API calls total regardless of row count (batch unique values)

---

### (d) Gold Layer Design Agent

**What it does**: Given the silver schema + a plain-English business domain description, proposes gold-layer aggregations and generates the SQL.

**Flow**:
1. Agent receives: silver schema DDL, column descriptions, sample data, domain description
2. Agent proposes 3-5 gold models with: name, business justification, SQL, expected output schema
3. Human reviews and selects which to implement

**Prompt design**:
- System: "You are a senior analytics engineer designing a gold layer for an operational data warehouse"
- Context: Full silver schema, domain = "facility management support tickets", stakeholders = "ops managers, procurement, facilities planning"
- Output: JSON array of `{ model_name, description, business_value, sql, output_columns }`
- Guardrails: "Only propose aggregations supported by the available columns. No assumptions about data not in the schema."

**Trust boundary**: 
- Trust the agent's SQL syntax (it's good at this)
- Verify business logic manually (the agent may over-engineer or miss domain nuance)
- Always run proposed SQL in a transaction and validate output shape before committing

---

## Good-to-Have Items

| Feature | Implementation |
|---|---|
| **Data lineage** | `lineage.transformations` table tracking: source_table → target_table, transformation_type, row_counts_in/out, timestamp, rules_applied |
| **Metadata auto-tagging** | During bronze ingestion, auto-detect: column types (PII hints like names/emails), sensitivity level, domain tags |
| **Human-in-the-loop gates** | Agent proposals written to `agent_proposals` table with status `PENDING`. Pipeline pauses until status = `APPROVED`. CLI command to review + approve |
| **Agent evaluation harness** | 20-row manually-labeled test set for category classification. Score agent accuracy, format compliance, latency, cost per run |
| **Observability** | Pipeline metrics table: stage, rows_in, rows_out, rows_dropped, duration_seconds, errors. Agent metrics: tokens_used, cost_usd, latency_ms |

---

## Proposed Changes

### Infrastructure
#### [NEW] `docker-compose.yml` — PostgreSQL + Python pipeline container
#### [NEW] `Dockerfile` — Python 3.11, pip install requirements
#### [NEW] `requirements.txt` — pandas, psycopg2-binary, openai, python-dateutil, sqlalchemy
#### [NEW] `.env.example` — `OPENAI_API_KEY`, `DATABASE_URL`, `LOG_LEVEL`

### Database
#### [NEW] `sql/00_init_schemas.sql` — Create `bronze`, `silver`, `gold`, `lineage` schemas
#### [NEW] `sql/01_bronze_ddl.sql` — `bronze.raw_tickets` (all TEXT + lineage columns)
#### [NEW] `sql/02_silver_ddl.sql` — `silver.tickets` (typed columns + cleaning metadata)
#### [NEW] `sql/03_gold_ddl.sql` — 3 gold tables + lineage table

### Pipeline
#### [NEW] `config/settings.py` — Configuration management
#### [NEW] `pipeline/bronze.py` — Raw ingestion with lineage
#### [NEW] `pipeline/silver.py` — 15-rule cleaning pipeline
#### [NEW] `pipeline/gold.py` — 3 business aggregations
#### [NEW] `pipeline/lineage.py` — Lineage tracking
#### [NEW] `pipeline/utils.py` — Date parsing, logging, hashing

### Agents
#### [NEW] `agents/base_agent.py` — Shared LLM client, cost tracking, structured output
#### [NEW] `agents/data_quality_agent.py` — Profile + propose rules
#### [NEW] `agents/semantic_classifier.py` — Category normalization via LLM
#### [NEW] `agents/gold_design_agent.py` — Propose gold aggregations

### Entrypoints
#### [NEW] `scripts/run_pipeline.py` — Main: `bronze → silver → gold`
#### [NEW] `scripts/run_agents.py` — Run agents independently

### Documentation
#### [NEW] `README.md` — Full documentation per assessment requirements

---

## Verification Plan

### Automated Tests
```bash
# Run the full pipeline end-to-end
docker-compose up -d db
python scripts/run_pipeline.py

# Verify idempotency
python scripts/run_pipeline.py  # Run again — row counts should not change

# Run basic tests
pytest tests/
```

### Manual Verification
- Verify bronze row count = 10,280 (no data loss)
- Verify silver dedup removed 21 duplicate ticket_ids + junk rows
- Verify gold tables have meaningful aggregations
- Verify agent outputs are sensible (review proposals)
- Verify `docker-compose up` + single command works end-to-end

---

## Open Questions

> [!IMPORTANT]
> **AI Provider**: The plan assumes OpenAI (GPT-4o-mini). Do you have an `OPENAI_API_KEY` available, or should I use Anthropic/a local model instead?

> [!IMPORTANT]
> **Data file rename**: The raw file is `raw_tickets (4).csv` but the assessment says `data/raw_tickets.csv`. I'll copy it to `data/raw_tickets.csv` without modifying the original. OK?

> [!NOTE]
> **Scope**: The plan covers all required items + 5 of 7 good-to-have items. I'll focus on Part 1 + Part 2 core requirements first, then add good-to-haves if time permits. Sound right?
