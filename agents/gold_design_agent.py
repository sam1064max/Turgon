"""
Gold Layer Design Agent: Proposes business aggregations from silver schema.

What it does:
  - Receives the silver schema DDL + column descriptions + sample data
  - Given a business domain description, proposes gold-layer models
  - Generates the SQL for each proposed model
  - Explains the business value and target audience for each

Trust boundary:
  - Trust: SQL syntax (LLMs are good at this)
  - Verify: Business logic (may over-engineer or miss domain nuance)
  - Always: Run proposed SQL in a transaction first, validate output shape
"""

import json

import pandas as pd

from agents.base_agent import BaseAgent
from pipeline.utils import read_sql, get_logger

log = get_logger("agent.gold_design")

SYSTEM_PROMPT = """You are a senior analytics engineer designing a gold (business-ready) data layer for an operational data warehouse.

Context: This is a facility management system tracking support tickets for multiple buildings. The stakeholders are:
- Operations managers: need to monitor SLA compliance and identify problem areas
- Procurement team: need vendor performance data for contract negotiations
- Facilities planning: need trend data for resource allocation and budgeting
- Executive leadership: need high-level KPIs and dashboards

You will receive the silver schema and sample data. Propose 3-5 gold-layer aggregation tables/views.

For each proposed model, provide:
1. model_name: snake_case table name (prefix with gold.)
2. description: What this model shows (1-2 sentences)
3. business_value: Why stakeholders need this, who would use it
4. target_audience: Which stakeholder group(s)
5. sql_create: CREATE TABLE statement
6. sql_populate: INSERT ... SELECT statement that populates it from silver.tickets
7. key_metrics: List of the key metrics this model provides
8. refresh_frequency: How often this should be refreshed (real-time, hourly, daily, weekly)

Guidelines:
- Only use columns that exist in the silver schema
- Include _refreshed_at timestamp in every gold table
- Make the SQL correct and executable against PostgreSQL
- Focus on actionable metrics, not vanity metrics
- Consider time-series aggregations for trend analysis

Respond with JSON: {"models": [...], "summary": "..."}
"""


class GoldDesignAgent(BaseAgent):
    """Proposes gold-layer aggregations from silver schema + domain context."""

    def __init__(self):
        super().__init__("gold_design")

    def run(self) -> dict:
        """
        Analyze silver schema and propose gold layer models.

        Returns:
            Dict with proposed models and their SQL
        """
        self.log.info("Starting gold layer design analysis")

        # ---- Step 1: Get silver schema info ----
        schema_info = self._get_schema_info()

        # ---- Step 2: Get sample data ----
        sample = read_sql("SELECT * FROM silver.tickets LIMIT 20")
        sample_str = sample.to_csv(index=False)

        # ---- Step 3: Get basic statistics ----
        stats = read_sql("""
            SELECT
                count(*) as total_tickets,
                count(DISTINCT category) as unique_categories,
                count(DISTINCT building) as unique_buildings,
                count(DISTINCT assigned_to) as unique_vendors,
                count(DISTINCT submitted_by) as unique_submitters,
                avg(cost) as avg_cost,
                avg(resolution_hours) as avg_resolution_hours,
                min(created_at) as earliest_ticket,
                max(created_at) as latest_ticket
            FROM silver.tickets
        """)

        # ---- Step 4: Build prompt ----
        user_prompt = f"""
## Silver Schema
{schema_info}

## Sample Data (20 rows)
```csv
{sample_str}
```

## Dataset Statistics
{stats.to_string(index=False)}

## Business Domain
This is a facility management support ticket system for a multi-building corporate campus.
Buildings include headquarters floors, towers, annexes, warehouses, data centers, and remote sites.
Tickets cover HVAC, electrical, plumbing, fire safety, elevators, security, cleaning, IT/network,
pest control, and general maintenance.

Vendors include: maintenance team, vendor tech, ABC Mechanical, overnight crew,
PestPro Services, Joe (in-house), CityWide Electric.

Design the optimal gold layer for this domain.
"""

        # ---- Step 5: Call LLM ----
        self.log.info("Calling LLM for gold layer design")
        result = self.call_llm(SYSTEM_PROMPT, user_prompt, max_tokens=4096)

        # ---- Step 6: Save proposals ----
        if "models" in result:
            self.log.info(f"Agent proposed {len(result['models'])} gold models")
            for model in result["models"]:
                self.save_proposal("gold_model", model)
                self.log.info(
                    f"  📊 {model.get('model_name', '?')}: "
                    f"{model.get('description', '')[:80]}"
                )
        else:
            self.log.warning("No models returned from LLM")

        self.print_cost_summary()
        return result

    def _get_schema_info(self) -> str:
        """Get the silver table schema as DDL-like description."""
        cols = read_sql("""
            SELECT column_name, data_type, is_nullable,
                   character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = 'silver' AND table_name = 'tickets'
            ORDER BY ordinal_position
        """)

        lines = ["CREATE TABLE silver.tickets ("]
        for _, row in cols.iterrows():
            dtype = row["data_type"].upper()
            if row["character_maximum_length"]:
                dtype += f"({int(row['character_maximum_length'])})"
            nullable = "" if row["is_nullable"] == "YES" else " NOT NULL"
            lines.append(f"    {row['column_name']} {dtype}{nullable},")
        lines.append(");")
        return "\n".join(lines)


if __name__ == "__main__":
    agent = GoldDesignAgent()
    result = agent.run()
    print(json.dumps(result, indent=2, default=str))
