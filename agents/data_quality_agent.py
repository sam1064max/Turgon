"""
Data Quality Agent: Profiles bronze data and proposes cleaning rules.

What it does:
  - Receives a sample of bronze data + column statistics
  - Identifies data quality issues (nulls, outliers, inconsistencies, sentinels)
  - Proposes cleaning rules in natural language WITH generated Python/SQL
  - Explains WHY each rule matters (not just flags anomalies)
  - Saves proposals for human review

Scale thinking:
  - At 10M rows: uses statistically significant sample (not full scan)
  - Profiles unique values only for categorical columns
  - Single LLM call per profiling run (batches all columns)
"""

import pandas as pd

from agents.base_agent import BaseAgent
from pipeline.utils import read_sql, get_logger

log = get_logger("agent.data_quality")

SYSTEM_PROMPT = """You are a senior data engineer reviewing raw data quality for an operational support ticket system (facility management).

Your task: analyze the provided data profile and propose specific cleaning rules.

For each rule, provide:
1. rule_name: Short snake_case identifier
2. column: Which column(s) it applies to
3. issue_description: What's wrong with the data (be specific, cite examples)
4. why_it_matters: Business impact of NOT fixing this issue
5. implementation_python: Python code snippet using pandas to fix it
6. implementation_sql: SQL expression or UPDATE statement to fix it
7. severity: critical / high / medium / low
8. estimated_rows_affected: Approximate number of rows affected

Guidelines:
- Do NOT propose dropping rows unless they are clearly junk (test data, spam)
- Prefer setting invalid values to NULL over imputation
- For categorical columns with many variants, propose normalization mappings
- For dates, handle multiple formats gracefully
- For numeric columns with sentinel values (like -1 or 999), explain the pattern
- Be specific: cite actual values from the profile, not generic advice

Respond with a JSON object: {"rules": [...], "summary": "...", "data_quality_score": 0-100}
"""


class DataQualityAgent(BaseAgent):
    """Profiles bronze data and proposes cleaning/validation rules."""

    def __init__(self):
        super().__init__("data_quality")

    def run(self, sample_size: int = 200) -> dict:
        """
        Profile bronze data and generate cleaning rule proposals.

        Args:
            sample_size: Number of rows to sample for LLM analysis

        Returns:
            Dict with proposed rules and quality assessment
        """
        self.log.info("Starting data quality profiling")

        # ---- Step 1: Read bronze data ----
        df = read_sql("SELECT * FROM bronze.raw_tickets")
        total_rows = len(df)
        self.log.info(f"Read {total_rows} bronze rows")

        # ---- Step 2: Compute column statistics ----
        profile = self._compute_profile(df)

        # ---- Step 3: Sample rows for LLM ----
        sample = df.sample(n=min(sample_size, len(df)), random_state=42)
        sample_str = sample.drop(
            columns=[c for c in sample.columns if c.startswith("_")]
        ).head(30).to_csv(index=False)

        # ---- Step 4: Build prompt ----
        user_prompt = f"""
## Dataset: Operational Support Tickets (Facility Management)
Total rows: {total_rows}
Columns: {list(df.columns)}

## Column Profile
{profile}

## Sample Data (first 30 rows of random sample)
```csv
{sample_str}
```

Analyze this data and propose cleaning rules. Be thorough — this data has
multiple issues including inconsistent date formats, mixed-case categories,
sentinel values, encoding problems, and potential column swaps.
"""

        # ---- Step 5: Call LLM ----
        self.log.info("Calling LLM for data quality analysis")
        result = self.call_llm(SYSTEM_PROMPT, user_prompt, max_tokens=4096)

        # ---- Step 6: Save proposals ----
        if "rules" in result:
            self.log.info(f"Agent proposed {len(result['rules'])} cleaning rules")
            for rule in result["rules"]:
                self.save_proposal("cleaning_rule", rule)
                self.log.info(
                    f"  [{rule.get('severity', '?').upper()}] "
                    f"{rule.get('rule_name', '?')}: {rule.get('issue_description', '')[:80]}"
                )
        else:
            self.log.warning("No rules returned from LLM")

        quality_score = result.get("data_quality_score", "N/A")
        self.log.info(f"Data quality score: {quality_score}/100")

        self.print_cost_summary()
        return result

    def _compute_profile(self, df: pd.DataFrame) -> str:
        """Compute column-level statistics for the LLM."""
        lines = []
        # Exclude metadata columns from profiling
        cols = [c for c in df.columns if not c.startswith("_")]

        for col in cols:
            series = df[col]
            null_count = series.isna().sum()
            null_pct = (null_count / len(df) * 100)
            unique_count = series.nunique()

            line = f"### {col}\n"
            line += f"- Null: {null_count} ({null_pct:.1f}%)\n"
            line += f"- Unique values: {unique_count}\n"

            # Show value distribution for low-cardinality columns
            if unique_count <= 30:
                vc = series.value_counts().head(15)
                line += f"- Top values: {dict(vc)}\n"
            else:
                # Show a sample of unique values
                sample_vals = series.dropna().unique()[:10].tolist()
                line += f"- Sample values: {sample_vals}\n"

            lines.append(line)

        return "\n".join(lines)


if __name__ == "__main__":
    agent = DataQualityAgent()
    result = agent.run()
    import json
    print(json.dumps(result, indent=2, default=str))
