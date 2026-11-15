"""Daily reconciliation DAG foundation with explicit, finite task semantics."""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    schedule="0 2 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["finstream", "reconciliation", "scaffolded"],
)
def daily_reconciliation():
    @task
    def record_run_start() -> str:
        return "A future implementation will create core.reconciliation_runs metadata."

    @task
    def compare_source_and_sink(run_note: str) -> str:
        return f"Not executed until source manifests exist. {run_note}"

    compare_source_and_sink(record_run_start())


daily_reconciliation()
