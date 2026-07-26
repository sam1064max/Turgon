"""
Agent runner: executes AI agents independently.

Usage:
    python scripts/run_agents.py                    # Run all agents
    python scripts/run_agents.py --agent quality    # Data Quality Agent only
    python scripts/run_agents.py --agent classify   # Semantic Classifier only
    python scripts/run_agents.py --agent gold       # Gold Design Agent only

Requires:
    - Bronze data already loaded (run pipeline first: --stage bronze)
    - OPENAI_API_KEY set in environment or .env file
"""

import argparse
import json
import sys
import os
import time

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import settings
from pipeline.utils import get_logger

log = get_logger("agents")


def run_agents(agent_name: str = None):
    """Run one or all agents."""
    start = time.time()
    log.info("=" * 60)
    log.info("TURGON AI AGENTS")
    log.info(f"Model: {settings.OPENAI_MODEL}")
    log.info("=" * 60)

    if not settings.OPENAI_API_KEY:
        log.error(
            "OPENAI_API_KEY not set. Please set it in .env or environment.\n"
            "Agents will run in dry-run mode (no LLM calls)."
        )

    results = {}

    # ---- Data Quality Agent ----
    if agent_name is None or agent_name == "quality":
        log.info("-" * 40)
        log.info("AGENT: Data Quality Profiler")
        log.info("-" * 40)
        from agents.data_quality_agent import DataQualityAgent
        agent = DataQualityAgent()
        result = agent.run()
        results["data_quality"] = result
        _print_quality_summary(result)

    # ---- Semantic Classifier Agent ----
    if agent_name is None or agent_name == "classify":
        log.info("-" * 40)
        log.info("AGENT: Semantic Classifier")
        log.info("-" * 40)
        from agents.semantic_classifier import SemanticClassifierAgent
        agent = SemanticClassifierAgent()
        result = agent.run()
        results["semantic_classifier"] = result
        _print_classifier_summary(result)

    # ---- Gold Design Agent ----
    if agent_name is None or agent_name == "gold":
        log.info("-" * 40)
        log.info("AGENT: Gold Layer Designer")
        log.info("-" * 40)
        from agents.gold_design_agent import GoldDesignAgent
        agent = GoldDesignAgent()
        result = agent.run()
        results["gold_design"] = result
        _print_gold_summary(result)

    elapsed = time.time() - start
    log.info("=" * 60)
    log.info(f"ALL AGENTS COMPLETE in {elapsed:.1f}s")
    log.info("=" * 60)

    # Print cost summary across all agents
    _print_total_cost()

    return results


def _print_quality_summary(result: dict):
    """Pretty-print data quality agent results."""
    if not result or "rules" not in result:
        log.info("No rules generated (LLM unavailable or error)")
        return

    log.info(f"\n📋 Data Quality Score: {result.get('data_quality_score', 'N/A')}/100")
    log.info(f"📋 Proposed {len(result['rules'])} cleaning rules:\n")
    for i, rule in enumerate(result["rules"], 1):
        log.info(
            f"  {i}. [{rule.get('severity', '?').upper():>8}] "
            f"{rule.get('rule_name', '?')}\n"
            f"     Column: {rule.get('column', '?')}\n"
            f"     Issue:  {rule.get('issue_description', '?')[:100]}\n"
            f"     Why:    {rule.get('why_it_matters', '?')[:100]}\n"
        )


def _print_classifier_summary(result: dict):
    """Pretty-print semantic classifier results."""
    if not result or "mappings" not in result:
        log.info("No mappings generated (LLM unavailable or error)")
        return

    mappings = result["mappings"]
    log.info(f"\n🏷️  Classified {len(mappings)} unique category values:")
    log.info(f"   From cache: {result.get('from_cache', 0)}")
    log.info(f"   Newly classified: {result.get('newly_classified', 0)}")
    log.info("\n   Sample mappings:")
    for raw, canonical in list(mappings.items())[:10]:
        log.info(f"     '{raw}' → '{canonical}'")


def _print_gold_summary(result: dict):
    """Pretty-print gold design agent results."""
    if not result or "models" not in result:
        log.info("No models generated (LLM unavailable or error)")
        return

    log.info(f"\n📊 Proposed {len(result['models'])} gold models:\n")
    for model in result["models"]:
        log.info(
            f"  📈 {model.get('model_name', '?')}\n"
            f"     {model.get('description', '?')}\n"
            f"     Audience: {model.get('target_audience', '?')}\n"
            f"     Refresh:  {model.get('refresh_frequency', '?')}\n"
        )


def _print_total_cost():
    """Print total cost across all agent runs."""
    try:
        from pipeline.utils import read_sql
        metrics = read_sql("""
            SELECT agent_name,
                   SUM(tokens_in) as total_tokens_in,
                   SUM(tokens_out) as total_tokens_out,
                   SUM(cost_usd) as total_cost,
                   AVG(latency_ms) as avg_latency_ms,
                   COUNT(*) as calls
            FROM lineage.agent_metrics
            GROUP BY agent_name
        """)
        if not metrics.empty:
            log.info("\n💰 Agent Cost Summary:")
            log.info(metrics.to_string(index=False))
            total = metrics["total_cost"].sum()
            log.info(f"\n   Total cost: ${total:.6f}")
    except Exception:
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Turgon AI Agents")
    parser.add_argument(
        "--agent",
        choices=["quality", "classify", "gold"],
        default=None,
        help="Run a specific agent (default: all agents)",
    )
    args = parser.parse_args()
    run_agents(args.agent)
