"""Finite historical-ingestion DAG; it never manages the continuous Spark service."""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["finstream", "backfill", "scaffolded"],
)
def historical_ingestion():
    @task
    def validate_manifest() -> str:
        """Validate a future parameterized input manifest before a bounded replay."""
        return "Manifest validation is scaffolded; no dataset is downloaded or replayed by this DAG."

    @task
    def submit_bounded_job(validation_note: str) -> str:
        """Reserve a task boundary for an explicit Spark submit operator."""
        return f"Not submitted: {validation_note}"

    submit_bounded_job(validate_manifest())


historical_ingestion()
