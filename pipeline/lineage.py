"""
Lineage tracking: records pipeline stage executions and transformations.
"""

import json
from datetime import datetime

from pipeline.utils import execute_sql, get_logger

log = get_logger("lineage")


class PipelineRun:
    """
    Context manager that tracks a pipeline stage execution.
    Records start/end times, row counts, and status in lineage.pipeline_runs.

    Usage:
        with PipelineRun("bronze") as run:
            run.rows_in = 10280
            # ... do work ...
            run.rows_out = 10280
    """

    def __init__(self, stage: str):
        self.stage = stage
        self.run_id = None
        self.rows_in = 0
        self.rows_out = 0
        self.rows_dropped = 0
        self.rows_deduped = 0
        self.details = {}

    def __enter__(self):
        try:
            result = execute_sql(
                """
                INSERT INTO lineage.pipeline_runs (stage, started_at, status)
                VALUES (:stage, NOW(), 'RUNNING')
                RETURNING run_id
                """,
                {"stage": self.stage},
            )
            if result and hasattr(result, "fetchone"):
                row = result.fetchone()
                self.run_id = row[0] if row else 1
            else:
                self.run_id = 1
        except Exception as e:
            log.warning(f"PipelineRun tracking skipped (no database connection): {e}")
            self.run_id = 1
        log.info(f"[{self.stage.upper()}] Pipeline run #{self.run_id} started")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        status = "FAILED" if exc_type else "SUCCESS"
        error_msg = str(exc_val) if exc_val else None

        execute_sql(
            """
            UPDATE lineage.pipeline_runs
            SET finished_at = NOW(),
                rows_in = :rows_in,
                rows_out = :rows_out,
                rows_dropped = :rows_dropped,
                rows_deduped = :rows_deduped,
                status = :status,
                error_message = :error_message,
                details = :details
            WHERE run_id = :run_id
            """,
            {
                "run_id": self.run_id,
                "rows_in": self.rows_in,
                "rows_out": self.rows_out,
                "rows_dropped": self.rows_dropped,
                "rows_deduped": self.rows_deduped,
                "status": status,
                "error_message": error_msg,
                "details": json.dumps(self.details),
            },
        )
        log.info(
            f"[{self.stage.upper()}] Run #{self.run_id} {status} | "
            f"in={self.rows_in} out={self.rows_out} "
            f"dropped={self.rows_dropped} deduped={self.rows_deduped}"
        )
        return False  # Don't suppress exceptions

    def log_transformation(self, source: str, target: str, rule: str,
                           description: str, rows_affected: int = 0):
        """Record a specific transformation applied during this run."""
        execute_sql(
            """
            INSERT INTO lineage.transformations
                (run_id, source_table, target_table, rule_name,
                 rule_description, rows_affected)
            VALUES (:run_id, :source, :target, :rule, :desc, :rows)
            """,
            {
                "run_id": self.run_id,
                "source": source,
                "target": target,
                "rule": rule,
                "desc": description,
                "rows": rows_affected,
            },
        )
