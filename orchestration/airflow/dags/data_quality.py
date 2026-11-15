"""Data-quality DAG boundary for finite validation checks."""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    schedule="0 4 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["finstream", "quality", "scaffolded"],
)
def data_quality():
    @task
    def validate_contract_and_dbt_tests() -> str:
        return "Contract and dbt checks are declared; run commands are intentionally configured later."

    validate_contract_and_dbt_tests()


data_quality()
