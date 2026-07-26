"""
Silver Layer: Cleansed, deduplicated, typed, validated.

Cleaning Rules (each documented with rationale):
  1.  Parse dates — 6+ formats including Unix epoch
  2.  Normalize categories — ~60 raw values → 10 canonical groups
  3.  Fix category/description swaps — 33 rows with descriptions in category
  4.  Normalize priorities — 20 variants → 4 canonical levels
  5.  Clean costs — strip $, handle TBD/error/-1 sentinels
  6.  Clean SLA hours — remove -1/0/999 sentinels
  7.  Validate temporal logic — flag resolved_at < created_at
  8.  Deduplicate by ticket_id — keep first occurrence per row hash
  9.  Remove junk/test rows — test/admin/system submitters + DELETE ME categories
  10. Normalize submitter names — consolidate variants (J. Smith → John Smith)
  11. Clean status values — unknown/??? → NULL
  12. Clean building values — unknown/??? → NULL
  13. Handle null ticket_ids — generate synthetic TKT-SYNTH-{n}
  14. Fix encoding artifacts — â€\" → em-dash
  15. Compute derived fields — resolution_hours, is_sla_breached, is_resolved
"""

import json

import pandas as pd

from pipeline.lineage import PipelineRun
from pipeline.utils import (
    get_engine,
    get_logger,
    read_sql,
    execute_sql,
    parse_date,
    normalize_category,
    normalize_priority,
    clean_cost,
    clean_sla,
    normalize_submitter,
    clean_status,
    clean_building,
    fix_encoding,
)

log = get_logger("silver")


def transform_silver() -> int:
    """
    Read from bronze, apply all cleaning rules, write to silver.
    Idempotent: truncates and reloads silver on each run.
    Returns the number of rows written.
    """
    with PipelineRun("silver") as run:
        # ---- Read bronze ----
        log.info("Reading bronze.raw_tickets")
        df = read_sql("SELECT * FROM bronze.raw_tickets ORDER BY _raw_row_number")
        run.rows_in = len(df)
        log.info(f"Read {len(df)} bronze rows")

        cleaning_log = []  # Track rules applied per row

        # ---- Rule 9: Remove junk/test rows FIRST ----
        junk_submitters = df["submitted_by"].str.lower().isin(
            ["test", "admin", "system"]
        )
        junk_categories = df["category"].str.lower().isin(
            ["delete me", "test", "asdf"]
        )
        junk_mask = junk_submitters | junk_categories
        junk_count = junk_mask.sum()
        df = df[~junk_mask].copy()
        log.info(f"Rule 9: Removed {junk_count} junk/test rows")

        # ---- Rule 8: Deduplicate by ticket_id ----
        # For rows with the same ticket_id, keep the first occurrence
        pre_dedup = len(df)
        # First, handle true row-level duplicates (same hash)
        df = df.drop_duplicates(subset=["_row_hash"], keep="first")
        hash_dupes = pre_dedup - len(df)

        # Then handle same ticket_id with different data (keep first by row number)
        pre_tid_dedup = len(df)
        df = df.sort_values("_raw_row_number").drop_duplicates(
            subset=["ticket_id"], keep="first"
        )
        tid_dupes = pre_tid_dedup - len(df)
        log.info(f"Rule 8: Deduped {hash_dupes} hash dupes + {tid_dupes} ticket_id dupes")

        # ---- Rule 13: Handle null ticket_ids ----
        null_tid_mask = df["ticket_id"].isna() | (df["ticket_id"].str.strip() == "")
        null_tid_count = null_tid_mask.sum()
        if null_tid_count > 0:
            synth_ids = [f"TKT-SYNTH-{i+1:04d}" for i in range(null_tid_count)]
            df.loc[null_tid_mask, "ticket_id"] = synth_ids
            log.info(f"Rule 13: Generated {null_tid_count} synthetic ticket IDs")

        # ---- Rule 14: Fix encoding artifacts ----
        for col in ["description", "resolution_notes", "category"]:
            df[col] = df[col].apply(fix_encoding)
        log.info("Rule 14: Fixed encoding artifacts")

        # ---- Rule 3: Fix category/description swaps ----
        # If category is longer than 50 chars, it's probably a description
        swap_mask = df["category"].str.len() > 50
        swap_count = swap_mask.sum()
        if swap_count > 0:
            # Move category content to description (if description is short/null)
            short_desc = df["description"].isna() | (df["description"].str.len() < 20)
            swap_and_short = swap_mask & short_desc
            df.loc[swap_and_short, "description"] = df.loc[swap_and_short, "category"]
            df.loc[swap_mask, "category"] = None  # Will be classified later
            log.info(f"Rule 3: Fixed {swap_count} category/description swaps")

        # ---- Rule 2: Normalize categories ----
        df["category"] = df["category"].apply(normalize_category)
        log.info("Rule 2: Normalized categories to canonical groups")

        # ---- Rule 4: Normalize priorities ----
        df["priority"] = df["priority"].apply(normalize_priority)
        log.info("Rule 4: Normalized priorities")

        # ---- Rule 1: Parse dates ----
        df["created_at"] = df["created_at"].apply(parse_date)
        df["resolved_at"] = df["resolved_at"].apply(parse_date)
        log.info("Rule 1: Parsed dates (6+ formats)")

        # ---- Rule 5: Clean costs ----
        df["cost"] = df["cost"].apply(clean_cost)
        log.info("Rule 5: Cleaned cost values")

        # ---- Rule 6: Clean SLA hours ----
        df["sla_hours"] = df["sla_hours"].apply(clean_sla)
        log.info("Rule 6: Cleaned SLA hours")

        # ---- Rule 10: Normalize submitter names ----
        df["submitted_by"] = df["submitted_by"].apply(normalize_submitter)
        log.info("Rule 10: Normalized submitter names")

        # ---- Rule 11: Clean status ----
        df["status"] = df["status"].apply(clean_status)
        log.info("Rule 11: Cleaned status values")

        # ---- Rule 12: Clean building ----
        df["building"] = df["building"].apply(clean_building)
        log.info("Rule 12: Cleaned building values")

        # ---- Rule 7: Validate temporal logic ----
        temporal_issues = (
            df["created_at"].notna() & df["resolved_at"].notna() &
            (df["resolved_at"] < df["created_at"])
        )
        temporal_count = temporal_issues.sum()
        log.info(f"Rule 7: Found {temporal_count} rows with resolved_at < created_at (flagged, not dropped)")

        # ---- Rule 15: Compute derived fields ----
        # Resolution hours
        mask_both_dates = df["created_at"].notna() & df["resolved_at"].notna()
        df["resolution_hours"] = None
        if mask_both_dates.any():
            df.loc[mask_both_dates, "resolution_hours"] = (
                (df.loc[mask_both_dates, "resolved_at"] -
                 df.loc[mask_both_dates, "created_at"])
                .dt.total_seconds() / 3600
            ).round(2)
            # Set negative resolution hours (temporal anomalies) to None
            neg_mask = df["resolution_hours"].notna() & (df["resolution_hours"] < 0)
            df.loc[neg_mask, "resolution_hours"] = None

        # SLA breached
        df["is_sla_breached"] = None
        sla_check = df["resolution_hours"].notna() & df["sla_hours"].notna()
        if sla_check.any():
            df.loc[sla_check, "is_sla_breached"] = (
                df.loc[sla_check, "resolution_hours"] >
                df.loc[sla_check, "sla_hours"]
            )

        # Is resolved
        df["is_resolved"] = df["status"].str.lower().isin(
            ["resolved", "closed"]
        ).where(df["status"].notna(), None)

        log.info("Rule 15: Computed derived fields (resolution_hours, is_sla_breached, is_resolved)")

        # ---- Build cleaning flags per row ----
        df["_cleaning_flags"] = df.apply(
            lambda row: json.dumps(_build_cleaning_flags(row, temporal_issues)),
            axis=1,
        )

        # ---- Write to silver (idempotent: truncate + reload) ----
        log.info("Writing to silver.tickets (truncate + reload)")
        execute_sql("TRUNCATE TABLE silver.tickets")

        silver_cols = [
            "ticket_id", "created_at", "resolved_at", "category",
            "priority", "status", "building", "description",
            "submitted_by", "assigned_to", "resolution_notes",
            "cost", "sla_hours",
            "resolution_hours", "is_sla_breached", "is_resolved",
            "_row_hash", "_cleaning_flags",
        ]

        out = df[silver_cols].copy()
        out = out.rename(columns={"_row_hash": "_bronze_row_hash"})

        engine = get_engine()
        out.to_sql(
            "tickets", engine, schema="silver",
            if_exists="append", index=False, method="multi",
            chunksize=1000,
        )

        run.rows_out = len(out)
        run.rows_dropped = junk_count
        run.rows_deduped = hash_dupes + tid_dupes
        run.details = {
            "junk_removed": junk_count,
            "hash_duplicates": hash_dupes,
            "ticket_id_duplicates": tid_dupes,
            "null_ticket_ids_fixed": int(null_tid_count),
            "category_swaps_fixed": int(swap_count),
            "temporal_anomalies": int(temporal_count),
        }

        # Log transformations
        rules = [
            ("parse_dates", "Parse 6+ date formats including Unix epoch", len(df)),
            ("normalize_categories", "Map ~60 raw categories to 10 canonical groups", len(df)),
            ("fix_category_swaps", "Move descriptions from category field", swap_count),
            ("normalize_priorities", "Map 20 priority variants to 4 levels", len(df)),
            ("clean_costs", "Strip $, handle TBD/error/-1 sentinels", len(df)),
            ("clean_sla", "Remove -1/0/999 SLA sentinels", len(df)),
            ("validate_temporal", "Flag resolved_at < created_at", temporal_count),
            ("dedup_rows", "Remove duplicate rows by hash and ticket_id", hash_dupes + tid_dupes),
            ("remove_junk", "Remove test/admin/system rows", junk_count),
            ("normalize_submitters", "Consolidate name variants", len(df)),
            ("clean_status", "Remove unknown/??? statuses", len(df)),
            ("clean_building", "Remove unknown/??? buildings", len(df)),
            ("fix_null_ids", "Generate synthetic ticket IDs", null_tid_count),
            ("fix_encoding", "Fix UTF-8 mojibake artifacts", len(df)),
            ("compute_derived", "Calculate resolution_hours, SLA breach, is_resolved", len(df)),
        ]
        for rule_name, desc, affected in rules:
            run.log_transformation(
                "bronze.raw_tickets", "silver.tickets",
                rule_name, desc, int(affected)
            )

        log.info(f"Silver transform complete: {len(out)} clean rows written")

    return len(out)


def _build_cleaning_flags(row, temporal_issues: pd.Series) -> list[str]:
    """Build a list of cleaning rules that were applied to this row."""
    flags = []
    if pd.isna(row.get("category")):
        flags.append("category_null_after_cleaning")
    if pd.isna(row.get("priority")):
        flags.append("priority_null_after_cleaning")
    if pd.isna(row.get("cost")):
        flags.append("cost_cleaned_to_null")
    if pd.isna(row.get("sla_hours")):
        flags.append("sla_cleaned_to_null")
    try:
        if temporal_issues.loc[row.name]:
            flags.append("temporal_anomaly")
    except (KeyError, TypeError):
        pass
    return flags


if __name__ == "__main__":
    transform_silver()
