"""
Semantic Classification Agent: Normalizes free-text categories via LLM.

What it does:
  - Extracts unique raw category values from bronze
  - Sends them to the LLM in batches with target taxonomy
  - Returns a mapping: raw_value → canonical_category
  - Caches mappings — unseen values only need classification once
  - Can also handle description-in-category-field rows via content analysis

Scale thinking:
  - At 10M rows: only ~200 unique category values need classification
  - Cached mapping eliminates redundant LLM calls on re-runs
  - Batch size of 25 values per call = ~3-4 API calls total
  - Total cost: ~$0.001-0.005 regardless of row count
"""

import json

import pandas as pd

from agents.base_agent import BaseAgent
from config.settings import settings
from pipeline.utils import read_sql, execute_sql, get_logger

log = get_logger("agent.semantic_classifier")

SYSTEM_PROMPT = """You are a data classification specialist for a facility management system.

Your task: map raw, messy category values to a canonical taxonomy.

Target taxonomy (use ONLY these categories):
- HVAC (heating, ventilation, air conditioning, climate control, temperature)
- Electrical (power, wiring, breakers, outlets, lighting)
- Plumbing (water, pipes, faucets, toilets, drains, leaks)
- Fire Safety (alarms, sprinklers, extinguishers, exit signs, smoke detectors)
- Elevator (lifts, escalators, vertical transport)
- Security (badges, access control, cameras, locks, doors)
- Cleaning (janitorial, housekeeping, trash, restroom servicing)
- IT/Network (WiFi, network, printers, VPN, connectivity)
- Pest Control (exterminator, insects, rodents)
- General (other, miscellaneous, general maintenance, furniture, parking, paint)

Rules:
- If a value is clearly a description/sentence (not a category label), classify based on the text content
- If a value is ambiguous, pick the BEST match and set confidence lower
- If a value is junk (like "asdf", "test", "DELETE ME", "???"), set canonical to null
- Every mapping must include a confidence score (0.0 to 1.0)

Respond with JSON: {"mappings": [{"raw": "...", "canonical": "...", "confidence": 0.95, "reasoning": "..."}]}
"""


class SemanticClassifierAgent(BaseAgent):
    """Normalizes messy category values using LLM classification."""

    def __init__(self):
        super().__init__("semantic_classifier")
        self._cache = {}  # In-memory cache for this run

    def run(self, batch_size: int = None) -> dict:
        """
        Classify all unique raw category values from bronze.

        Returns:
            Dict with mappings and statistics
        """
        batch_size = batch_size or settings.AGENT_BATCH_SIZE
        self.log.info("Starting semantic classification of categories")

        # ---- Step 1: Get unique raw categories ----
        df = read_sql(
            "SELECT DISTINCT category FROM bronze.raw_tickets WHERE category IS NOT NULL"
        )
        raw_values = df["category"].dropna().unique().tolist()
        self.log.info(f"Found {len(raw_values)} unique raw category values")

        # ---- Step 2: Check cache (DB-based for persistence across runs) ----
        cached = self._load_cached_mappings()
        uncached = [v for v in raw_values if v not in cached]
        self.log.info(f"Cached: {len(cached)}, Need classification: {len(uncached)}")

        # ---- Step 3: Classify in batches ----
        all_mappings = dict(cached)  # Start with cached

        if uncached:
            for i in range(0, len(uncached), batch_size):
                batch = uncached[i:i + batch_size]
                self.log.info(
                    f"Classifying batch {i // batch_size + 1} "
                    f"({len(batch)} values)"
                )
                mappings = self._classify_batch(batch)
                all_mappings.update(mappings)

                # Cache new mappings
                for raw, canonical in mappings.items():
                    self._cache[raw] = canonical

        # ---- Step 4: Save full mapping as proposal ----
        mapping_list = [
            {"raw": k, "canonical": v} for k, v in all_mappings.items()
        ]
        self.save_proposal("category_mapping", {
            "total_unique_values": len(raw_values),
            "newly_classified": len(uncached),
            "from_cache": len(cached),
            "mappings": mapping_list,
        })

        self.print_cost_summary()

        return {
            "mappings": all_mappings,
            "total_unique": len(raw_values),
            "newly_classified": len(uncached),
            "from_cache": len(cached),
        }

    def classify_single(self, text: str) -> str | None:
        """Classify a single text value. Uses cache first."""
        if text in self._cache:
            return self._cache[text]
        result = self._classify_batch([text])
        return result.get(text)

    def _classify_batch(self, values: list[str]) -> dict[str, str | None]:
        """Classify a batch of raw category values via LLM."""
        user_prompt = f"""Classify these raw category values into the canonical taxonomy.

Raw values to classify:
{json.dumps(values, indent=2)}

Respond with the mappings JSON.
"""
        result = self.call_llm(SYSTEM_PROMPT, user_prompt)
        mappings = {}

        if "mappings" in result:
            for m in result["mappings"]:
                raw = m.get("raw", "")
                canonical = m.get("canonical")
                confidence = m.get("confidence", 0)
                # Only accept high-confidence mappings automatically
                if canonical and confidence >= 0.5:
                    mappings[raw] = canonical
                else:
                    mappings[raw] = None
                    self.log.warning(
                        f"Low confidence for '{raw}' → '{canonical}' "
                        f"(conf={confidence}): {m.get('reasoning', '')}"
                    )
        else:
            self.log.warning(f"No mappings in LLM response: {result}")

        return mappings

    def _load_cached_mappings(self) -> dict[str, str]:
        """Load previously saved category mappings from agent_proposals."""
        try:
            df = read_sql("""
                SELECT proposal->'mappings' as mappings
                FROM lineage.agent_proposals
                WHERE agent_name = 'semantic_classifier'
                  AND proposal_type = 'category_mapping'
                  AND status IN ('APPROVED', 'PENDING')
                ORDER BY created_at DESC
                LIMIT 1
            """)
            if not df.empty and df.iloc[0]["mappings"]:
                mappings_list = json.loads(df.iloc[0]["mappings"])
                return {m["raw"]: m["canonical"] for m in mappings_list}
        except Exception as e:
            self.log.debug(f"No cached mappings found: {e}")
        return {}


if __name__ == "__main__":
    agent = SemanticClassifierAgent()
    result = agent.run()
    print(json.dumps(result, indent=2, default=str))
