"""Idempotently load the Parquet transaction lake into PostgreSQL staging."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any

import pyarrow.dataset as ds
import psycopg
from psycopg.types.json import Jsonb


LOGGER = logging.getLogger("finstream.lake_loader")

INSERT_SQL = """
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
    source_attributes,
    kafka_key,
    kafka_timestamp
)
VALUES (
    %(event_id)s,
    %(event_version)s,
    %(source_system)s,
    %(source_record_id)s,
    %(simulation_id)s,
    %(event_time)s,
    %(ingested_at)s,
    %(amount)s,
    %(currency)s,
    %(transaction_type)s,
    %(origin_account_id)s,
    %(destination_account_id)s,
    %(merchant_id)s,
    %(historical_fraud_label)s,
    %(source_attributes)s,
    %(kafka_key)s,
    %(kafka_timestamp)s
)
ON CONFLICT DO NOTHING
"""

REQUIRED_COLUMNS = {
    "event_id",
    "event_version",
    "source_system",
    "source_record_id",
    "simulation_id",
    "event_time",
    "ingested_at",
    "amount",
    "currency",
    "transaction_type",
    "origin_account_id",
    "destination_account_id",
    "merchant_id",
    "historical_fraud_label",
    "source_attributes_json",
    "kafka_key",
    "kafka_timestamp",
}

EVENT_ID_PATTERN = re.compile(r"^[0-9a-f]{40}$")


def parse_record(row: dict[str, Any]) -> dict[str, Any]:
    """Validate and convert one Parquet record to the PostgreSQL shape."""
    event_id = row["event_id"]

    if not isinstance(event_id, str) or not EVENT_ID_PATTERN.fullmatch(event_id):
        raise ValueError(f"Invalid event_id: {event_id!r}")

    if row["event_version"] is None or int(row["event_version"]) <= 0:
        raise ValueError(f"Invalid event_version for {event_id}")

    if row["source_system"] not in {"paysim", "ieee_cis", "synthetic"}:
        raise ValueError(f"Unsupported source_system for {event_id}")

    if not row["source_record_id"]:
        raise ValueError(f"Missing source_record_id for {event_id}")

    if row["event_time"] is None or row["ingested_at"] is None:
        raise ValueError(f"Missing required timestamp for {event_id}")

    if row["amount"] is None or row["amount"] < 0:
        raise ValueError(f"Invalid amount for {event_id}")

    if not row["currency"] or len(row["currency"]) != 3:
        raise ValueError(f"Invalid currency for {event_id}")

    raw_attributes = row["source_attributes_json"]
    if raw_attributes is None:
        attributes = {}
    else:
        attributes = json.loads(raw_attributes)

    if not isinstance(attributes, dict):
        raise ValueError(f"source_attributes_json must be a JSON object for {event_id}")

    return {
        "event_id": event_id,
        "event_version": int(row["event_version"]),
        "source_system": row["source_system"],
        "source_record_id": row["source_record_id"],
        "simulation_id": row["simulation_id"],
        "event_time": row["event_time"],
        "ingested_at": row["ingested_at"],
        "amount": row["amount"],
        "currency": row["currency"],
        "transaction_type": row["transaction_type"],
        "origin_account_id": row["origin_account_id"],
        "destination_account_id": row["destination_account_id"],
        "merchant_id": row["merchant_id"],
        "historical_fraud_label": row["historical_fraud_label"],
        "source_attributes": Jsonb(attributes),
        "kafka_key": row["kafka_key"],
        "kafka_timestamp": row["kafka_timestamp"],
    }


def load_parquet(
    parquet_path: Path,
    database_url: str,
    batch_size: int = 500,
    expected_source_system: str | None = None,
) -> dict[str, int]:
    """Load Parquet records in committed, idempotent batches."""
    if batch_size < 1:
        raise ValueError("batch_size must be positive")

    dataset = ds.dataset(
    str(parquet_path),
    format="parquet",
    partitioning="hive",
)
    available_columns = set(dataset.schema.names)
    missing = REQUIRED_COLUMNS - available_columns

    if missing:
        raise ValueError(f"Parquet dataset is missing columns: {sorted(missing)}")

    inserted = 0
    skipped = 0
    processed = 0

    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT to_regclass('staging.transactions')")
            if cursor.fetchone()[0] is None:
                raise RuntimeError(
                    "staging.transactions does not exist. Apply the staging DDL first."
                )

        scanner = dataset.scanner(batch_size=batch_size)

        for record_batch in scanner.to_batches():
            payloads = []

            for row in record_batch.to_pylist():
                payload = parse_record(row)

                if (
                    expected_source_system is not None
                    and payload["source_system"] != expected_source_system
                ):
                    raise ValueError(
                        f"Expected source {expected_source_system!r}, "
                        f"found {payload['source_system']!r}"
                    )

                payloads.append(payload)

            if not payloads:
                continue

            with connection.cursor() as cursor:
                cursor.executemany(INSERT_SQL, payloads)
                batch_inserted = cursor.rowcount

            # Each batch is independently committed. A later failure does not
            # roll back earlier batches; rerunning safely skips existing rows.
            connection.commit()

            inserted += batch_inserted
            processed += len(payloads)
            skipped += len(payloads) - batch_inserted

            LOGGER.info(
                "Batch committed: processed=%d inserted=%d skipped=%d",
                len(payloads),
                batch_inserted,
                len(payloads) - batch_inserted,
            )

        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*), COUNT(DISTINCT event_id)
                FROM staging.transactions
                """
            )
            total_rows, unique_event_ids = cursor.fetchone()

    if total_rows != unique_event_ids:
        raise RuntimeError(
            f"Staging reconciliation failed: rows={total_rows}, "
            f"unique_event_ids={unique_event_ids}"
        )

    return {
        "processed": processed,
        "inserted": inserted,
        "skipped": skipped,
        "staging_rows": total_rows,
        "unique_event_ids": unique_event_ids,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load the FinStream Parquet lake into PostgreSQL staging."
    )
    parser.add_argument(
        "parquet_path",
        nargs="?",
        default=".local/lake-export/transactions",
        help="Path to the exported Parquet dataset",
    )
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument(
        "--expected-source-system",
        default=None,
        help="Optionally enforce one source system, e.g. paysim",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        LOGGER.error("DATABASE_URL is not set")
        sys.exit(2)

    result = load_parquet(
        Path(args.parquet_path),
        database_url,
        batch_size=args.batch_size,
        expected_source_system=args.expected_source_system,
    )

    print("\n===== LOAD RECONCILIATION =====")
    for key, value in result.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
