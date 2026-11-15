"""Warehouse dbt execution DAG; commands are configured only after profile setup."""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    schedule="30 3 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["finstream", "dbt", "scaffolded"],
)
def dbt_warehouse():
    @task
    def require_profile() -> str:
        return "dbt profiles.yml must be created from profiles.yml.example before execution."

    @task
    def execute_dbt(profile_note: str) -> str:
        return f"Not executed by scaffold: {profile_note}"

    execute_dbt(require_profile())


dbt_warehouse()
