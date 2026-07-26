"""
Gold Layer: Business-ready aggregations.

Three models, each justified by stakeholder needs:

1. SLA Performance — Ops managers need breach rates by category/building/priority
   to identify systemic issues and prioritize improvements.

2. Vendor Performance — Procurement needs scorecards for contract negotiations:
   resolution speed, cost, and SLA compliance per vendor.

3. Ticket Volume Trends — Facilities planning needs weekly trends to forecast
   resource allocation, spot seasonal patterns, and track resolution rates.
"""

import pandas as pd

from pipeline.lineage import PipelineRun
from pipeline.utils import get_engine, get_logger, execute_sql, read_sql

log = get_logger("gold")


class GoldAggregator:
    """Class wrapper for gold layer aggregations."""

    def aggregate_sla_performance(self, df_silver: pd.DataFrame) -> pd.DataFrame:
        """Aggregate SLA performance by category, building, priority."""
        if df_silver is None or df_silver.empty:
            return pd.DataFrame()
        sla_df = df_silver[df_silver["sla_hours"].notna()].copy()
        if sla_df.empty:
            return pd.DataFrame()
        for col in ["category", "building", "priority"]:
            if col in sla_df.columns:
                sla_df[col] = sla_df[col].fillna("Unknown")
        grouped = sla_df.groupby(["category", "building", "priority"]).agg(
            total_tickets=("ticket_id", "count"),
            resolved_tickets=("is_resolved", lambda x: x.sum() if x.notna().any() else 0),
            sla_breached=("is_sla_breached", lambda x: x.sum() if x.notna().any() else 0),
        ).reset_index()
        grouped["sla_met"] = grouped["total_tickets"] - grouped["sla_breached"]
        grouped["breach_rate_pct"] = ((grouped["sla_breached"] / grouped["total_tickets"]) * 100).round(2)
        return grouped


def build_gold() -> dict[str, int]:
    """
    Build all gold layer tables from silver.
    Idempotent: truncates and reloads each gold table.
    Returns a dict of {table_name: row_count}.
    """
    with PipelineRun("gold") as run:
        log.info("Reading silver.tickets")
        df = read_sql("SELECT * FROM silver.tickets")
        run.rows_in = len(df)
        log.info(f"Read {len(df)} silver rows")

        results = {}

        # ---- Gold 1: SLA Performance ----
        results["sla_performance"] = _build_sla_performance(df, run)

        # ---- Gold 2: Vendor Performance ----
        results["vendor_performance"] = _build_vendor_performance(df, run)

        # ---- Gold 3: Ticket Volume Trends ----
        results["ticket_volume_trends"] = _build_ticket_volume_trends(df, run)

        run.rows_out = sum(results.values())
        run.details = results
        log.info(f"Gold layer complete: {results}")

    return results


def _build_sla_performance(df: pd.DataFrame, run: PipelineRun) -> int:
    """
    SLA Performance: breach rates by category × building × priority.
    Helps ops managers identify which areas/categories consistently miss SLAs.
    """
    log.info("Building gold.sla_performance")

    # Filter to rows with SLA data
    sla_df = df[df["sla_hours"].notna()].copy()

    if sla_df.empty:
        log.warning("No SLA data available, skipping sla_performance")
        return 0

    # Fill nulls for grouping
    for col in ["category", "building", "priority"]:
        sla_df[col] = sla_df[col].fillna("Unknown")

    grouped = sla_df.groupby(["category", "building", "priority"]).agg(
        total_tickets=("ticket_id", "count"),
        resolved_tickets=("is_resolved", lambda x: x.sum() if x.notna().any() else 0),
        sla_breached=("is_sla_breached", lambda x: x.sum() if x.notna().any() else 0),
    ).reset_index()

    grouped["sla_met"] = grouped["total_tickets"] - grouped["sla_breached"]
    grouped["breach_rate_pct"] = (
        (grouped["sla_breached"] / grouped["total_tickets"]) * 100
    ).round(2)

    # Compute avg and median resolution hours per group
    res_stats = sla_df[sla_df["resolution_hours"].notna()].groupby(
        ["category", "building", "priority"]
    )["resolution_hours"].agg(
        avg_resolution_hrs="mean",
        median_resolution_hrs="median",
    ).round(2).reset_index()

    result = grouped.merge(res_stats, on=["category", "building", "priority"], how="left")

    # Write
    execute_sql("TRUNCATE TABLE gold.sla_performance")
    engine = get_engine()
    result.to_sql(
        "sla_performance", engine, schema="gold",
        if_exists="append", index=False, method="multi",
    )

    run.log_transformation(
        "silver.tickets", "gold.sla_performance",
        "sla_aggregation",
        "SLA breach rates by category/building/priority",
        len(result),
    )

    log.info(f"gold.sla_performance: {len(result)} rows")
    return len(result)


def _build_vendor_performance(df: pd.DataFrame, run: PipelineRun) -> int:
    """
    Vendor Performance: scorecards per assigned vendor/team.
    Helps procurement evaluate vendor contracts.
    """
    log.info("Building gold.vendor_performance")

    # Only rows with an assigned_to value
    vendor_df = df[df["assigned_to"].notna()].copy()

    if vendor_df.empty:
        log.warning("No vendor data available, skipping vendor_performance")
        return 0

    grouped = vendor_df.groupby("assigned_to").agg(
        total_tickets=("ticket_id", "count"),
        resolved_tickets=("is_resolved", lambda x: x.sum() if x.notna().any() else 0),
        avg_resolution_hrs=("resolution_hours", lambda x: x.mean() if x.notna().any() else None),
        avg_cost=("cost", lambda x: x.mean() if x.notna().any() else None),
        total_cost=("cost", lambda x: x.sum() if x.notna().any() else None),
    ).reset_index()

    # Compute SLA breach rate per vendor
    sla_data = vendor_df[vendor_df["is_sla_breached"].notna()].groupby("assigned_to").agg(
        breached=("is_sla_breached", "sum"),
        sla_total=("is_sla_breached", "count"),
    ).reset_index()
    sla_data["sla_breach_rate_pct"] = (
        (sla_data["breached"] / sla_data["sla_total"]) * 100
    ).round(2)

    grouped = grouped.merge(
        sla_data[["assigned_to", "sla_breach_rate_pct"]],
        on="assigned_to", how="left",
    )

    # Top category per vendor
    top_cats = (
        vendor_df.groupby(["assigned_to", "category"])
        .size()
        .reset_index(name="cnt")
        .sort_values(["assigned_to", "cnt"], ascending=[True, False])
        .drop_duplicates("assigned_to")
        [["assigned_to", "category"]]
        .rename(columns={"category": "top_category"})
    )
    grouped = grouped.merge(top_cats, on="assigned_to", how="left")

    # Round numeric columns
    for col in ["avg_resolution_hrs", "avg_cost", "total_cost"]:
        grouped[col] = grouped[col].round(2)

    # Write
    execute_sql("TRUNCATE TABLE gold.vendor_performance")
    engine = get_engine()
    grouped.to_sql(
        "vendor_performance", engine, schema="gold",
        if_exists="append", index=False, method="multi",
    )

    run.log_transformation(
        "silver.tickets", "gold.vendor_performance",
        "vendor_aggregation",
        "Vendor scorecards: resolution time, cost, SLA breach rate",
        len(grouped),
    )

    log.info(f"gold.vendor_performance: {len(grouped)} rows")
    return len(grouped)


def _build_ticket_volume_trends(df: pd.DataFrame, run: PipelineRun) -> int:
    """
    Ticket Volume Trends: weekly aggregations by category and building.
    Helps facilities planning forecast resource needs and spot patterns.
    """
    log.info("Building gold.ticket_volume_trends")

    # Only rows with created_at
    trend_df = df[df["created_at"].notna()].copy()

    if trend_df.empty:
        log.warning("No timestamp data available, skipping ticket_volume_trends")
        return 0

    # Ensure datetime type
    trend_df["created_at"] = pd.to_datetime(trend_df["created_at"])

    # Week start (Monday)
    trend_df["week_start"] = trend_df["created_at"].dt.to_period("W-MON").dt.start_time

    # Fill nulls for grouping
    for col in ["category", "building"]:
        trend_df[col] = trend_df[col].fillna("Unknown")

    grouped = trend_df.groupby(["week_start", "category", "building"]).agg(
        tickets_opened=("ticket_id", "count"),
        tickets_resolved=("is_resolved", lambda x: x.sum() if x.notna().any() else 0),
        avg_cost=("cost", lambda x: x.mean() if x.notna().any() else None),
        avg_resolution_hrs=("resolution_hours", lambda x: x.mean() if x.notna().any() else None),
    ).reset_index()

    grouped["resolution_rate_pct"] = (
        (grouped["tickets_resolved"] / grouped["tickets_opened"]) * 100
    ).round(2)

    # Round
    for col in ["avg_cost", "avg_resolution_hrs"]:
        grouped[col] = grouped[col].round(2)

    grouped["week_start"] = grouped["week_start"].dt.date

    # Write
    execute_sql("TRUNCATE TABLE gold.ticket_volume_trends")
    engine = get_engine()
    grouped.to_sql(
        "ticket_volume_trends", engine, schema="gold",
        if_exists="append", index=False, method="multi",
    )

    run.log_transformation(
        "silver.tickets", "gold.ticket_volume_trends",
        "volume_trend_aggregation",
        "Weekly ticket volumes by category and building",
        len(grouped),
    )

    log.info(f"gold.ticket_volume_trends: {len(grouped)} rows")
    return len(grouped)


if __name__ == "__main__":
    build_gold()
