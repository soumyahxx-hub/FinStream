"""Manifest-driven reconciliation across staging and operational storage."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import psycopg

LOGGER = logging.getLogger("finstream.reconciliation")

RunType = Literal["replay", "backfill", "daily"]
VALID_SOURCES = {"paysim", "ieee_cis", "synthetic"}


@dataclass(frozen=True)
class ReconciliationSummary:
    """Compare source records with unique sink records and known duplicates."""

    source_count: int
    sink_count: int
    duplicate_count: int

    def __post_init__(self) -> None:
        if min(self.source_count, self.sink_count, self.duplicate_count) < 0:
            raise ValueError("Reconciliation counts cannot be negative")
        if self.duplicate_count > self.source_count:
            raise ValueError("duplicate_count cannot exceed source_count")

    @property
    def unreconciled_count(self) -> int:
        return self.source_count - self.sink_count - self.duplicate_count

    @property
    def is_reconciled(self) -> bool:
        return self.unreconciled_count == 0


@dataclass(frozen=True)
class ReconciliationManifest:
    """Expected source counts for one source system and optional simulation."""

    source_system: str
    source_count: int
    duplicate_count: int
    simulation_id: str | None = None

    @classmethod
    def from_file(cls, path: Path) -> ReconciliationManifest:
        try:
            payload: Any = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"Cannot read reconciliation manifest: {exc}") from exc

        if not isinstance(payload, dict):
            raise ValueError("Manifest must contain a JSON object")

        required = {"source_system", "source_count", "duplicate_count"}
        missing = required - payload.keys()
        if missing:
            raise ValueError(f"Manifest is missing fields: {sorted(missing)}")

        source_system = payload["source_system"]
        source_count = payload["source_count"]
        duplicate_count = payload["duplicate_count"]
        simulation_id = payload.get("simulation_id")

        if source_system not in VALID_SOURCES:
            raise ValueError(f"Unsupported source_system: {source_system!r}")

        for name, value in (
            ("source_count", source_count),
            ("duplicate_count", duplicate_count),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")

        if duplicate_count > source_count:
            raise ValueError("duplicate_count cannot exceed source_count")

        if simulation_id is not None and (
            not isinstance(simulation_id, str) or not simulation_id.strip()
        ):
            raise ValueError("simulation_id must be a non-empty string or null")

        return cls(
            source_system=source_system,
            source_count=source_count,
            duplicate_count=duplicate_count,
            simulation_id=simulation_id,
        )


@dataclass(frozen=True)
class ReconciliationReport:
    run_id: uuid.UUID
    status: Literal["passed", "failed"]
    summary: ReconciliationSummary
    staging_count: int
    operational_count: int
    metrics: dict[str, tuple[int, int]]

    @property
    def is_reconciled(self) -> bool:
        return self.status == "passed"


def _count_records(
    connection: psycopg.Connection[Any],
    schema: str,
    manifest: ReconciliationManifest,
) -> int:
    """Count records for the manifest's source and optional simulation."""
    if schema not in {"staging", "core"}:
        raise ValueError("Unsupported reconciliation schema")

    query = f"SELECT COUNT(*) FROM {schema}.transactions WHERE source_system = %s"
    params: list[Any] = [manifest.source_system]

    if manifest.simulation_id is not None:
        query += " AND simulation_id = %s"
        params.append(manifest.simulation_id)

    with connection.cursor() as cursor:
        cursor.execute(query, params)
        row = cursor.fetchone()

    if row is None:
        raise RuntimeError(f"Could not count records in {schema}.transactions")
    return int(row[0])


def reconcile(
    database_url: str,
    manifest: ReconciliationManifest,
    manifest_uri: str,
    run_type: RunType = "replay",
    notes: str | None = None,
) -> ReconciliationReport:
    """Persist a reconciliation run and its metric-level results."""
    if run_type not in {"replay", "backfill", "daily"}:
        raise ValueError(f"Unsupported run_type: {run_type}")

    run_id = uuid.uuid4()
    started_at = datetime.now(UTC)
    expected_unique = manifest.source_count - manifest.duplicate_count

    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO core.reconciliation_runs (
                    run_id, run_type, source_system, started_at, status,
                    input_manifest_uri, notes
                )
                VALUES (%s, %s, %s, %s, 'running', %s, %s)
                """,
                (
                    run_id,
                    run_type,
                    manifest.source_system,
                    started_at,
                    manifest_uri,
                    notes,
                ),
            )
        connection.commit()

        try:
            staging_count = _count_records(connection, "staging", manifest)
            operational_count = _count_records(connection, "core", manifest)

            summary = ReconciliationSummary(
                source_count=manifest.source_count,
                sink_count=operational_count,
                duplicate_count=manifest.duplicate_count,
            )

            # Each metric stores its expected value and observed value.
            metrics = {
                "source_records_accounted_for": (
                    manifest.source_count,
                    operational_count + manifest.duplicate_count,
                ),
                "staging_unique_records": (expected_unique, staging_count),
                "operational_unique_records": (expected_unique, operational_count),
                "staging_vs_operational_records": (staging_count, operational_count),
            }

            passed = summary.is_reconciled and all(
                expected == actual for expected, actual in metrics.values()
            )
            status: Literal["passed", "failed"] = "passed" if passed else "failed"

            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO core.reconciliation_results (
                        run_id, metric_name, expected_value, actual_value
                    )
                    VALUES (%s, %s, %s, %s)
                    """,
                    [
                        (run_id, metric_name, expected, actual)
                        for metric_name, (expected, actual) in metrics.items()
                    ],
                )
                cursor.execute(
                    """
                    UPDATE core.reconciliation_runs
                    SET completed_at = %s, status = %s
                    WHERE run_id = %s
                    """,
                    (datetime.now(UTC), status, run_id),
                )
            connection.commit()

            return ReconciliationReport(
                run_id=run_id,
                status=status,
                summary=summary,
                staging_count=staging_count,
                operational_count=operational_count,
                metrics=metrics,
            )

        except Exception as exc:
            connection.rollback()
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE core.reconciliation_runs
                    SET completed_at = %s, status = 'failed', notes = %s
                    WHERE run_id = %s
                    """,
                    (datetime.now(UTC), f"Reconciliation error: {exc}"[:2000], run_id),
                )
            connection.commit()
            raise


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reconcile source manifest counts against FinStream PostgreSQL."
    )
    parser.add_argument(
        "--manifest",
        required=True,
        type=Path,
        help="JSON manifest containing source_system, source_count and duplicate_count",
    )
    parser.add_argument(
        "--run-type",
        choices=("replay", "backfill", "daily"),
        default="replay",
    )
    parser.add_argument("--notes", default=None)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        parser.error("DATABASE_URL environment variable is required")

    try:
        manifest = ReconciliationManifest.from_file(args.manifest)
        report = reconcile(
            database_url=database_url,
            manifest=manifest,
            manifest_uri=args.manifest.resolve().as_uri(),
            run_type=args.run_type,
            notes=args.notes,
        )
    except (ValueError, psycopg.Error, RuntimeError) as exc:
        LOGGER.error("Reconciliation failed: %s", exc)
        sys.exit(1)

    output = {
        "run_id": str(report.run_id),
        "status": report.status,
        "source_count": report.summary.source_count,
        "duplicate_count": report.summary.duplicate_count,
        "staging_count": report.staging_count,
        "operational_count": report.operational_count,
        "unreconciled_count": report.summary.unreconciled_count,
        "metrics": {
            name: {"expected": values[0], "actual": values[1]}
            for name, values in report.metrics.items()
        },
    }
    print(json.dumps(output, indent=2))
    if not report.is_reconciled:
        sys.exit(2)


if __name__ == "__main__":
    main()
