"""PostgreSQL integration tests for manifest-driven reconciliation."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from uuid import UUID

import psycopg
import pytest

from ingestion.consumers.reconciliation import (
    ReconciliationManifest,
    ReconciliationReport,
    reconcile,
)


@pytest.fixture
def staged_transactions(
    test_database_url: str,
    test_data: dict[str, Any],
) -> Iterator[dict[str, Any]]:
    """Copy fixture operational transactions into staging and clean them up."""
    prefix = test_data["prefix"]

    with psycopg.connect(test_database_url) as connection:
        connection.execute(
            """
            INSERT INTO staging.transactions (
                event_id,
                event_version,
                source_system,
                source_record_id,
                simulation_id,
                event_time,
                ingested_at,
                amount,
                currency,
                transaction_type,
                origin_account_id,
                destination_account_id,
                merchant_id,
                historical_fraud_label,
                source_attributes
            )
            SELECT
                event_id,
                event_version,
                source_system,
                source_record_id,
                simulation_id,
                event_time,
                ingested_at,
                amount,
                currency,
                transaction_type,
                origin_account_id,
                destination_account_id,
                merchant_id,
                historical_fraud_label,
                source_attributes
            FROM core.transactions
            WHERE source_system = 'synthetic'
              AND simulation_id = %s
            """,
            (prefix,),
        )

    yield test_data

    with psycopg.connect(test_database_url) as connection:
        connection.execute(
            """
            DELETE FROM staging.transactions
            WHERE simulation_id = %s
            """,
            (prefix,),
        )


def load_run(
    database_url: str,
    run_id: UUID,
) -> tuple[dict[str, Any], dict[str, tuple[int, int]]]:
    """Read one persisted reconciliation run and its metric results."""
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT status, source_system, input_manifest_uri,
                       started_at, completed_at
                FROM core.reconciliation_runs
                WHERE run_id = %s
                """,
                (run_id,),
            )
            row = cursor.fetchone()
            assert row is not None

            run = {
                "status": row[0],
                "source_system": row[1],
                "input_manifest_uri": row[2],
                "started_at": row[3],
                "completed_at": row[4],
            }

            cursor.execute(
                """
                SELECT metric_name, expected_value, actual_value
                FROM core.reconciliation_results
                WHERE run_id = %s
                ORDER BY metric_name
                """,
                (run_id,),
            )
            metrics = {
                metric_name: (expected, actual)
                for metric_name, expected, actual in cursor.fetchall()
            }

    return run, metrics


def cleanup_run(database_url: str, run_id: UUID) -> None:
    """Delete only the reconciliation run created by one test."""
    with psycopg.connect(database_url) as connection:
        connection.execute(
            "DELETE FROM core.reconciliation_results WHERE run_id = %s",
            (run_id,),
        )
        connection.execute(
            "DELETE FROM core.reconciliation_runs WHERE run_id = %s",
            (run_id,),
        )


def execute_reconciliation(
    database_url: str,
    test_data: dict[str, Any],
    source_count: int,
    duplicate_count: int,
) -> ReconciliationReport:
    manifest = ReconciliationManifest(
        source_system="synthetic",
        simulation_id=test_data["prefix"],
        source_count=source_count,
        duplicate_count=duplicate_count,
    )

    return reconcile(
        database_url=database_url,
        manifest=manifest,
        manifest_uri=f"test://{test_data['prefix']}/manifest.json",
        run_type="replay",
        notes="PostgreSQL reconciliation integration test",
    )


def test_reconciliation_persists_successful_run(
    test_database_url: str,
    staged_transactions: dict[str, Any],
) -> None:
    """All three staging/core records reconcile against the source manifest."""
    report = execute_reconciliation(
        test_database_url,
        staged_transactions,
        source_count=3,
        duplicate_count=0,
    )

    try:
        assert report.status == "passed"
        assert report.is_reconciled
        assert report.staging_count == 3
        assert report.operational_count == 3

        run, metrics = load_run(test_database_url, report.run_id)

        assert run["status"] == "passed"
        assert run["source_system"] == "synthetic"
        assert run["input_manifest_uri"].endswith("/manifest.json")
        assert run["started_at"] is not None
        assert run["completed_at"] is not None

        assert metrics == {
            "operational_unique_records": (3, 3),
            "source_records_accounted_for": (3, 3),
            "staging_unique_records": (3, 3),
            "staging_vs_operational_records": (3, 3),
        }
    finally:
        cleanup_run(test_database_url, report.run_id)


def test_reconciliation_accounts_for_known_duplicates(
    test_database_url: str,
    staged_transactions: dict[str, Any],
) -> None:
    """Four source records with one known duplicate reconcile to three unique rows."""
    report = execute_reconciliation(
        test_database_url,
        staged_transactions,
        source_count=4,
        duplicate_count=1,
    )

    try:
        assert report.status == "passed"
        assert report.summary.unreconciled_count == 0
        assert report.staging_count == 3
        assert report.operational_count == 3

        _, metrics = load_run(test_database_url, report.run_id)

        assert metrics["source_records_accounted_for"] == (4, 4)
        assert metrics["staging_unique_records"] == (3, 3)
        assert metrics["operational_unique_records"] == (3, 3)
    finally:
        cleanup_run(test_database_url, report.run_id)


def test_reconciliation_fails_when_staging_is_missing_a_record(
    test_database_url: str,
    test_data: dict[str, Any],
) -> None:
    """A staging/core count mismatch must persist a failed run."""
    prefix = test_data["prefix"]
    missing_event_id = test_data["transactions"][2]["event_id"]

    with psycopg.connect(test_database_url) as connection:
        connection.execute(
            """
            INSERT INTO staging.transactions (
                event_id,
                event_version,
                source_system,
                source_record_id,
                simulation_id,
                event_time,
                ingested_at,
                amount,
                currency,
                transaction_type,
                origin_account_id,
                destination_account_id,
                merchant_id,
                historical_fraud_label,
                source_attributes
            )
            SELECT
                event_id,
                event_version,
                source_system,
                source_record_id,
                simulation_id,
                event_time,
                ingested_at,
                amount,
                currency,
                transaction_type,
                origin_account_id,
                destination_account_id,
                merchant_id,
                historical_fraud_label,
                source_attributes
            FROM core.transactions
            WHERE source_system = 'synthetic'
              AND simulation_id = %s
              AND event_id <> %s
            """,
            (prefix, missing_event_id),
        )

    try:
        report = execute_reconciliation(
            test_database_url,
            test_data,
            source_count=3,
            duplicate_count=0,
        )

        try:
            assert report.status == "failed"
            assert not report.is_reconciled
            assert report.staging_count == 2
            assert report.operational_count == 3

            run, metrics = load_run(test_database_url, report.run_id)

            assert run["status"] == "failed"
            assert run["completed_at"] is not None
            assert metrics["staging_unique_records"] == (3, 2)
            assert metrics["operational_unique_records"] == (3, 3)
            assert metrics["staging_vs_operational_records"] == (2, 3)
        finally:
            cleanup_run(test_database_url, report.run_id)
    finally:
        with psycopg.connect(test_database_url) as connection:
            connection.execute(
                """
                DELETE FROM staging.transactions
                WHERE simulation_id = %s
                """,
                (prefix,),
            )


def test_reconciliation_scopes_counts_to_simulation(
    test_database_url: str,
    staged_transactions: dict[str, Any],
) -> None:
    """Records from another simulation must not affect this run's counts."""
    report = execute_reconciliation(
        test_database_url,
        staged_transactions,
        source_count=3,
        duplicate_count=0,
    )

    try:
        assert report.status == "passed"
        assert report.staging_count == 3
        assert report.operational_count == 3
    finally:
        cleanup_run(test_database_url, report.run_id)


def test_reconciliation_rejects_unsupported_run_type(
    test_database_url: str,
    staged_transactions: dict[str, Any],
) -> None:
    manifest = ReconciliationManifest(
        source_system="synthetic",
        simulation_id=staged_transactions["prefix"],
        source_count=3,
        duplicate_count=0,
    )

    with pytest.raises(ValueError, match="Unsupported run_type"):
        reconcile(
            database_url=test_database_url,
            manifest=manifest,
            manifest_uri="test://invalid-run-type",
            run_type="invalid",  # type: ignore[arg-type]
        )
