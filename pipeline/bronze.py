"""
Bronze Layer: Raw ingestion with lineage metadata.

Principles:
  - Schema-on-read: all columns stored as TEXT
  - Zero data loss: every row from the CSV is preserved
  - Lineage: source file, ingestion timestamp, row hash, row number
  - Idempotent: row hash uniqueness prevents duplicate inserts
"""

import os
from datetime import datetime, timezone

import pandas as pd

from config.settings import settings
from pipeline.lineage import PipelineRun
from pipeline.utils import compute_row_hash, get_engine, get_logger, execute_sql

log = get_logger("bronze")


def ingest_bronze(csv_path: str = None) -> int:
    """
    Ingest raw CSV into bronze.raw_tickets.
    Returns the number of new rows inserted.
    """
    csv_path = csv_path or settings.RAW_DATA_PATH
    source_file = os.path.basename(csv_path)

    with PipelineRun("bronze") as run:
        # ---- Read raw CSV ----
        log.info(f"Reading {csv_path}")
        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
        # Replace empty strings with None for consistent null handling
        df = df.replace({"": None})
        run.rows_in = len(df)
        log.info(f"Read {len(df)} rows, {len(df.columns)} columns")

        # ---- Add lineage metadata ----
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        df["_source_file"] = source_file
        df["_ingested_at"] = now
        df["_raw_row_number"] = range(1, len(df) + 1)

        # Compute row hash (on original columns only, not metadata)
        original_cols = [c for c in df.columns if not c.startswith("_")]
        df["_row_hash"] = df[original_cols].apply(compute_row_hash, axis=1)

        # ---- Idempotent insert: skip rows that already exist (by hash) ----
        engine = get_engine()

        # Get existing hashes
        try:
            existing = pd.read_sql(
                "SELECT _row_hash FROM bronze.raw_tickets", engine
            )
            existing_hashes = set(existing["_row_hash"])
        except Exception:
            existing_hashes = set()

        new_rows = df[~df["_row_hash"].isin(existing_hashes)]
        skipped = len(df) - len(new_rows)

        if skipped > 0:
            log.info(f"Skipping {skipped} rows (already ingested, matching hash)")

        # ---- Insert new rows ----
        if len(new_rows) > 0:
            # Ensure column order matches the table
            cols = [
                "ticket_id", "created_at", "resolved_at", "category",
                "priority", "status", "building", "description",
                "submitted_by", "assigned_to", "resolution_notes",
                "cost", "sla_hours",
                "_source_file", "_ingested_at", "_row_hash", "_raw_row_number",
            ]
            new_rows = new_rows[cols]
            new_rows.to_sql(
                "raw_tickets", engine, schema="bronze",
                if_exists="append", index=False, method="multi",
                chunksize=1000,
            )
            log.info(f"Inserted {len(new_rows)} new rows into bronze.raw_tickets")
        else:
            log.info("No new rows to insert (fully idempotent)")

        run.rows_out = len(new_rows)
        run.rows_deduped = skipped
        run.details = {
            "source_file": source_file,
            "total_csv_rows": len(df),
            "new_rows_inserted": len(new_rows),
            "skipped_existing": skipped,
        }

        run.log_transformation(
            source=csv_path,
            target="bronze.raw_tickets",
            rule="raw_ingestion",
            description="Ingested raw CSV with lineage metadata and row hashing",
            rows_affected=len(new_rows),
        )

    return len(new_rows)


if __name__ == "__main__":
    ingest_bronze()
