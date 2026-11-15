
from __future__ import annotations

import hashlib
import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

import psycopg
import pytest

from api.repositories.operational import OperationalRepository


@pytest.fixture(scope="session")
def test_database_url() -> str:
    url = os.getenv("FINSTREAM_TEST_DATABASE_URL")

    if not url:
        pytest.fail(
            "FINSTREAM_TEST_DATABASE_URL is required. "
            "Use the dedicated finstream_test database."
        )

    return url


@pytest.fixture(scope="session")
def repository(
    test_database_url: str,
) -> Iterator[OperationalRepository]:
    repo = OperationalRepository(
        database_url=test_database_url,
        min_pool_size=1,
        max_pool_size=3,
    )

    repo.open()

    yield repo

    repo.close()


@pytest.fixture
def test_data(
    test_database_url: str,
) -> Iterator[dict[str, Any]]:
    token = uuid4().hex
    prefix = f"it-{token}"
    base_time = datetime(2099, 1, 1, 1, 0, tzinfo=UTC)

    def make_id(value: str) -> str:
        return hashlib.sha1(value.encode()).hexdigest()

    transactions = [
        {
            "event_id": make_id(f"{prefix}-tx-1"),
            "source_record_id": f"{prefix}-record-1",
            "event_time": base_time + timedelta(minutes=10),
            "amount": Decimal("100.00"),
            "transaction_type": "PAYMENT",
            "origin_account_id": f"{prefix}-customer",
            "destination_account_id": f"{prefix}-merchant-1",
        },
        {
            "event_id": make_id(f"{prefix}-tx-2"),
            "source_record_id": f"{prefix}-record-2",
            "event_time": base_time + timedelta(minutes=20),
            "amount": Decimal("200.00"),
            "transaction_type": "CASH_OUT",
            "origin_account_id": f"{prefix}-customer",
            "destination_account_id": f"{prefix}-merchant-2",
        },
        {
            "event_id": make_id(f"{prefix}-tx-3"),
            "source_record_id": f"{prefix}-record-3",
            "event_time": base_time + timedelta(days=1, minutes=10),
            "amount": Decimal("300.00"),
            "transaction_type": "TRANSFER",
            "origin_account_id": f"{prefix}-other",
            "destination_account_id": f"{prefix}-customer",
        },
    ]

    alerts = [
        {
            "alert_id": make_id(f"{prefix}-alert-1"),
            "event_id": transactions[1]["event_id"],
            "created_at": base_time + timedelta(minutes=21),
            "rule_name": "integration_test_rule",
            "risk_score": Decimal("0.7500"),
            "severity": "high",
            "explanation": "Integration test alert",
        },
        {
            "alert_id": make_id(f"{prefix}-alert-2"),
            "event_id": transactions[2]["event_id"],
            "created_at": base_time + timedelta(days=1, minutes=11),
            "rule_name": "integration_test_rule",
            "risk_score": Decimal("0.5500"),
            "severity": "medium",
            "explanation": "Integration test alert",
        },
    ]

    processor_name = f"{prefix}-processor"
    checkpoint_ids = [
        f"{prefix}-checkpoint-{index}"
        for index in range(3)
    ]
    reconciliation_id = uuid4()

    with psycopg.connect(test_database_url) as conn:
        for tx in transactions:
            conn.execute(
                """
                INSERT INTO core.transactions (
                    event_id,
                    event_version,
                    source_system,
                    source_record_id,
                    simulation_id,
                    event_time,
                    amount,
                    currency,
                    transaction_type,
                    origin_account_id,
                    destination_account_id,
                    historical_fraud_label
                )
                VALUES (
                    %(event_id)s,
                    1,
                    'synthetic',
                    %(source_record_id)s,
                    %(simulation_id)s,
                    %(event_time)s,
                    %(amount)s,
                    'INR',
                    %(transaction_type)s,
                    %(origin_account_id)s,
                    %(destination_account_id)s,
                    NULL
                )
                """,
                {
                    **tx,
                    "simulation_id": prefix,
                },
            )

        for alert in alerts:
            conn.execute(
                """
                INSERT INTO core.fraud_alerts (
                    alert_id,
                    event_id,
                    created_at,
                    rule_name,
                    risk_score,
                    severity,
                    explanation
                )
                VALUES (
                    %(alert_id)s,
                    %(event_id)s,
                    %(created_at)s,
                    %(rule_name)s,
                    %(risk_score)s,
                    %(severity)s,
                    %(explanation)s
                )
                """,
                alert,
            )

        # Explicit timestamps guarantee deterministic latest-status ordering.
        processor_rows = [
            (
                checkpoint_ids[0],
                "transactions.raw",
                0,
                0,
                10,
                "started",
                base_time,
            ),
            (
                checkpoint_ids[1],
                "transactions.raw",
                0,
                0,
                20,
                "completed",
                base_time + timedelta(minutes=1),
            ),
            (
                checkpoint_ids[2],
                "transactions.raw",
                1,
                0,
                15,
                "failed",
                base_time + timedelta(minutes=2),
            ),
        ]

        for (
            checkpoint,
            topic,
            partition,
            offset_start,
            offset_end,
            status,
            processed_at,
        ) in processor_rows:
            conn.execute(
                """
                INSERT INTO core.processing_metadata (
                    processor_name,
                    checkpoint_id,
                    source_topic,
                    partition_id,
                    offset_start,
                    offset_end,
                    status,
                    processed_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    processor_name,
                    checkpoint,
                    topic,
                    partition,
                    offset_start,
                    offset_end,
                    status,
                    processed_at,
                ),
            )

        conn.execute(
            """
            INSERT INTO core.reconciliation_runs (
                run_id,
                run_type,
                source_system,
                started_at,
                completed_at,
                status,
                input_manifest_uri,
                notes
            )
            VALUES (
                %s,
                'replay',
                'synthetic',
                %s,
                %s,
                'passed',
                %s,
                %s
            )
            """,
            (
                reconciliation_id,
                base_time,
                base_time + timedelta(minutes=5),
                f"test://{prefix}/manifest",
                "Integration test run",
            ),
        )

        conn.execute(
            """
            INSERT INTO core.reconciliation_results (
                run_id,
                metric_name,
                expected_value,
                actual_value
            )
            VALUES
                (%s, 'input_count', 3, 3),
                (%s, 'fraud_count', 2, 1)
            """,
            (
                reconciliation_id,
                reconciliation_id,
            ),
        )

    data = {
        "prefix": prefix,
        "base_time": base_time,
        "transactions": transactions,
        "alerts": alerts,
        "customer_id": f"{prefix}-customer",
        "processor_name": processor_name,
        "checkpoint_ids": checkpoint_ids,
        "reconciliation_id": reconciliation_id,
    }

    yield data

    # Delete only records belonging to this fixture.
    with psycopg.connect(test_database_url) as conn:
        conn.execute(
            """
            DELETE FROM core.reconciliation_results
            WHERE run_id = %s
            """,
            (reconciliation_id,),
        )

        conn.execute(
            """
            DELETE FROM core.reconciliation_runs
            WHERE run_id = %s
            """,
            (reconciliation_id,),
        )

        conn.execute(
            """
            DELETE FROM core.processing_metadata
            WHERE processor_name = %s
            """,
            (processor_name,),
        )

        conn.execute(
            """
            DELETE FROM core.fraud_alerts
            WHERE alert_id = ANY(%s)
            """,
            ([alert["alert_id"] for alert in alerts],),
        )

        conn.execute(
            """
            DELETE FROM core.transactions
            WHERE event_id = ANY(%s)
            """,
            ([tx["event_id"] for tx in transactions],),
        )
