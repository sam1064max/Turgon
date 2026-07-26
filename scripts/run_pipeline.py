"""
Main pipeline entry point: runs bronze → silver → gold sequentially.

Usage:
    python scripts/run_pipeline.py              # Full pipeline
    python scripts/run_pipeline.py --stage bronze   # Single stage
    python scripts/run_pipeline.py --stage silver
    python scripts/run_pipeline.py --stage gold
"""

import argparse
import sys
import time
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.utils import get_logger, execute_sql
from pipeline.bronze import ingest_bronze
from pipeline.silver import transform_silver
from pipeline.gold import build_gold

log = get_logger("pipeline")


def ensure_schemas():
    """Ensure all database schemas and tables exist."""
    sql_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "sql")
    sql_files = sorted(f for f in os.listdir(sql_dir) if f.endswith(".sql"))

    for sql_file in sql_files:
        path = os.path.join(sql_dir, sql_file)
        log.info(f"Executing {sql_file}")
        with open(path, "r") as f:
            sql = f.read()
        # Split on semicolons and execute each statement
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                try:
                    execute_sql(stmt)
                except Exception as e:
                    log.warning(f"SQL statement warning: {e}")


def run_pipeline(stage: str = None):
    """Run the full pipeline or a specific stage."""
    start = time.time()
    log.info("=" * 60)
    log.info("TURGON MEDALLION PIPELINE")
    log.info("=" * 60)

    # Ensure schemas exist
    log.info("Initializing database schemas...")
    ensure_schemas()

    if stage is None or stage == "bronze":
        log.info("-" * 40)
        log.info("STAGE 1: BRONZE (Raw Ingestion)")
        log.info("-" * 40)
        rows = ingest_bronze()
        log.info(f"Bronze complete: {rows} rows ingested")

    if stage is None or stage == "silver":
        log.info("-" * 40)
        log.info("STAGE 2: SILVER (Cleanse & Transform)")
        log.info("-" * 40)
        rows = transform_silver()
        log.info(f"Silver complete: {rows} rows transformed")

    if stage is None or stage == "gold":
        log.info("-" * 40)
        log.info("STAGE 3: GOLD (Business Aggregations)")
        log.info("-" * 40)
        result = build_gold()
        log.info(f"Gold complete: {result}")

    elapsed = time.time() - start
    log.info("=" * 60)
    log.info(f"PIPELINE COMPLETE in {elapsed:.1f}s")
    log.info("=" * 60)

    # Print summary from lineage
    try:
        from pipeline.utils import read_sql
        runs = read_sql("""
            SELECT stage, status, rows_in, rows_out, rows_dropped, rows_deduped,
                   finished_at - started_at as duration
            FROM lineage.pipeline_runs
            ORDER BY run_id DESC
            LIMIT 10
        """)
        log.info(f"\nRecent pipeline runs:\n{runs.to_string(index=False)}")
    except Exception:
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Turgon Medallion Pipeline")
    parser.add_argument(
        "--stage",
        choices=["bronze", "silver", "gold"],
        default=None,
        help="Run a specific stage (default: all stages)",
    )
    args = parser.parse_args()
    run_pipeline(args.stage)
