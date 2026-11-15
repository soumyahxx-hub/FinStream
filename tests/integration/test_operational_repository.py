
from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from api.repositories.operational import OperationalRepository


def time_window(
    data: dict[str, Any],
) -> tuple[Any, Any]:
    start = data["base_time"] - timedelta(hours=1)
    end = data["base_time"] + timedelta(days=2)

    return start, end


def test_connection_pool_healthcheck(
    repository: OperationalRepository,
) -> None:
    repository.healthcheck()
    repository.healthcheck()


def test_transaction_retrieval_and_filters(
    repository: OperationalRepository,
    test_data: dict[str, Any],
) -> None:
    tx = test_data["transactions"][0]

    result = repository.get_transaction(tx["event_id"])

    assert result is not None
    assert result["event_id"] == tx["event_id"]
    assert result["source_system"] == "synthetic"
    assert result["amount"] == Decimal("100.00")

    start, end = time_window(test_data)

    rows, has_more = repository.list_transactions(
        limit=10,
        source_system="synthetic",
        transaction_type="CASH_OUT",
        min_amount=Decimal("150"),
        max_amount=Decimal("250"),
        start_time=start,
        end_time=end,
    )

    assert not has_more
    assert len(rows) == 1
    assert rows[0]["event_id"] == test_data["transactions"][1]["event_id"]


def test_transaction_pagination_and_cursor(
    repository: OperationalRepository,
    test_data: dict[str, Any],
) -> None:
    start, end = time_window(test_data)

    first_page, has_more = repository.list_transactions(
        limit=2,
        start_time=start,
        end_time=end,
    )

    assert has_more
    assert len(first_page) == 2

    assert (
        first_page[0]["event_id"]
        == test_data["transactions"][2]["event_id"]
    )

    last_row = first_page[-1]
    cursor = (
        last_row["event_time"],
        last_row["event_id"],
    )

    second_page, has_more = repository.list_transactions(
        limit=2,
        start_time=start,
        end_time=end,
        cursor_position=cursor,
    )

    assert not has_more
    assert len(second_page) == 1

    assert (
        second_page[0]["event_id"]
        == test_data["transactions"][0]["event_id"]
    )


def test_fraud_alert_retrieval_and_filters(
    repository: OperationalRepository,
    test_data: dict[str, Any],
) -> None:
    alert = test_data["alerts"][0]

    result = repository.get_alert(alert["alert_id"])

    assert result is not None
    assert result["alert_id"] == alert["alert_id"]
    assert result["severity"] == "high"
    assert result["risk_score"] == Decimal("0.7500")

    start, end = time_window(test_data)

    rows, has_more = repository.list_alerts(
        limit=10,
        severity="medium",
        rule_name="integration_test_rule",
        start_time=start,
        end_time=end,
    )

    assert not has_more
    assert len(rows) == 1

    assert (
        rows[0]["alert_id"]
        == test_data["alerts"][1]["alert_id"]
    )


def test_alert_pagination(
    repository: OperationalRepository,
    test_data: dict[str, Any],
) -> None:
    start, end = time_window(test_data)

    rows, has_more = repository.list_alerts(
        limit=1,
        start_time=start,
        end_time=end,
    )

    assert has_more
    assert len(rows) == 1

    assert (
        rows[0]["alert_id"]
        == test_data["alerts"][1]["alert_id"]
    )

    last_row = rows[-1]

    next_rows, next_has_more = repository.list_alerts(
        limit=1,
        start_time=start,
        end_time=end,
        cursor_position=(
            last_row["created_at"],
            last_row["alert_id"],
        ),
    )

    assert not next_has_more
    assert len(next_rows) == 1

    assert (
        next_rows[0]["alert_id"]
        == test_data["alerts"][0]["alert_id"]
    )


def test_customer_risk_aggregation(
    repository: OperationalRepository,
    test_data: dict[str, Any],
) -> None:
    result = repository.customer_risk(
        test_data["customer_id"],
    )

    assert result["transaction_count"] == 3
    assert result["alert_count"] == 2


def test_analytics_overview(
    repository: OperationalRepository,
    test_data: dict[str, Any],
) -> None:
    start, end = time_window(test_data)

    result = repository.analytics_overview(start, end)

    assert result["transaction_count"] == 3
    assert result["total_transaction_value"] == Decimal("600.00")
    assert result["average_transaction_value"] == Decimal("200")
    assert result["alert_count"] == 2
    assert result["alerted_transaction_count"] == 2
    assert result["alert_rate_pct"] == Decimal("66.6667")


def test_transaction_trends_hourly_and_daily(
    repository: OperationalRepository,
    test_data: dict[str, Any],
) -> None:
    start, end = time_window(test_data)

    hourly = repository.transaction_trends(
        start,
        end,
        "hour",
    )

    assert len(hourly) == 2
    assert sum(row["transaction_count"] for row in hourly) == 3
    assert sum(row["alert_count"] for row in hourly) == 2

    daily = repository.transaction_trends(
        start,
        end,
        "day",
    )

    assert len(daily) == 2
    assert sum(row["transaction_count"] for row in daily) == 3
    assert sum(row["alert_count"] for row in daily) == 2


def test_transaction_trends_reject_invalid_granularity(
    repository: OperationalRepository,
    test_data: dict[str, Any],
) -> None:
    start, end = time_window(test_data)

    with pytest.raises(ValueError, match="Granularity"):
        repository.transaction_trends(
            start,
            end,
            "month",
        )


def test_analytics_distributions(
    repository: OperationalRepository,
    test_data: dict[str, Any],
) -> None:
    start, end = time_window(test_data)

    result = repository.analytics_distributions(start, end)

    assert sum(
        row["count"] for row in result["source_systems"]
    ) == 3

    assert sum(
        row["count"] for row in result["transaction_types"]
    ) == 3

    assert sum(
        row["count"] for row in result["alert_severities"]
    ) == 2

    severity_counts = {
        row["category"]: row["count"]
        for row in result["alert_severities"]
    }

    assert severity_counts == {
        "high": 1,
        "medium": 1,
    }


def test_pipeline_operations(
    repository: OperationalRepository,
    test_data: dict[str, Any],
) -> None:
    result = repository.pipeline_operations()

    processor = next(
        row
        for row in result["processors"]
        if row["processor_name"] == test_data["processor_name"]
    )

    assert processor["source_topic"] == "transactions.raw"
    assert processor["partition_count"] == 2
    assert processor["completed_partitions"] == 1
    assert processor["failed_partitions"] == 1
    assert processor["latest_offset"] == 20

    reconciliation = next(
        row
        for row in result["recent_reconciliations"]
        if row["run_id"] == str(test_data["reconciliation_id"])
    )

    assert reconciliation["status"] == "passed"
    assert reconciliation["metric_count"] == 2
    assert reconciliation["mismatch_count"] == 1


def test_summary_returns_database_counts(
    repository: OperationalRepository,
    test_data: dict[str, Any],
) -> None:
    result = repository.summary()

    assert result["transaction_count"] >= 3
    assert result["fraud_alert_count"] >= 2
    assert result["latest_event_time"] is not None


def test_database_enforces_transaction_source_uniqueness(
    test_database_url: str,
    test_data: dict[str, Any],
) -> None:
    tx = test_data["transactions"][0]

    with pytest.raises(psycopg.IntegrityError):
        with psycopg.connect(test_database_url) as conn:
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
                    currency
                )
                VALUES (
                    %s, 1, 'synthetic', %s, %s, %s, 10, 'INR'
                )
                """,
                (
                    "f" * 40,
                    tx["source_record_id"],
                    test_data["prefix"],
                    tx["event_time"],
                ),
            )


def test_database_enforces_alert_foreign_key(
    test_database_url: str,
    test_data: dict[str, Any],
) -> None:
    with pytest.raises(psycopg.IntegrityError):
        with psycopg.connect(test_database_url) as conn:
            conn.execute(
                """
                INSERT INTO core.fraud_alerts (
                    alert_id,
                    event_id,
                    rule_name,
                    risk_score,
                    severity,
                    explanation
                )
                VALUES (
                    %s, %s, 'integration_test', 0.5, 'medium', 'Invalid FK'
                )
                """,
                (
                    "e" * 40,
                    "d" * 40,
                ),
            )
