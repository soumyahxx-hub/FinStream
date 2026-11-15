
"""HTTP integration tests for the FinStream operational API."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import timedelta
from decimal import Decimal
from typing import Any

import pytest
from fastapi.testclient import TestClient

from api.main import app


@pytest.fixture
def api_client(
    monkeypatch: pytest.MonkeyPatch,
    test_database_url: str,
) -> Iterator[TestClient]:
    """Start FastAPI using the isolated PostgreSQL test database."""
    import api.services.operational as operational

    previous_repository = operational._repository

    monkeypatch.setattr(
        operational.settings,
        "database_url",
        test_database_url,
    )
    operational._repository = None

    try:
        with TestClient(app) as client:
            yield client
    finally:
        operational.close_operational_repository()
        operational._repository = previous_repository


def analytics_params(data: dict[str, Any]) -> dict[str, str]:
    """Build a bounded analytics time window for fixture records."""
    return {
        "start_time": (
            data["base_time"] - timedelta(hours=1)
        ).isoformat(),
        "end_time": (
            data["base_time"] + timedelta(days=2)
        ).isoformat(),
    }


def assert_request_id(response: Any) -> None:
    """Verify that the response contains a request ID."""
    assert "x-request-id" in response.headers
    assert response.headers["x-request-id"]


def assert_error(
    response: Any,
    expected_status: int,
    expected_code: str,
) -> dict[str, Any]:
    """Verify the standardized API error response."""
    assert response.status_code == expected_status
    assert_request_id(response)

    body = response.json()
    assert "error" in body

    error = body["error"]

    assert error["code"] == expected_code
    assert error["request_id"]
    assert error["request_id"] == response.headers["x-request-id"]

    return error


# ---------------------------------------------------------------------------
# Application lifecycle and health
# ---------------------------------------------------------------------------


def test_application_starts_with_test_database(
    api_client: TestClient,
) -> None:
    response = api_client.get("/ready")

    assert response.status_code == 200
    assert response.json()["status"] == "ready"


def test_liveness_endpoint(
    api_client: TestClient,
) -> None:
    response = api_client.get("/live")

    assert response.status_code == 200
    assert response.json()["status"] == "alive"


def test_legacy_health_endpoint(
    api_client: TestClient,
) -> None:
    response = api_client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ready"


# ---------------------------------------------------------------------------
# Transaction endpoints
# ---------------------------------------------------------------------------


def test_transaction_list_returns_database_records(
    api_client: TestClient,
    test_data: dict[str, Any],
) -> None:
    response = api_client.get(
        "/transactions",
        params={
            "source_system": "synthetic",
            "transaction_type": "CASH_OUT",
        },
    )

    assert response.status_code == 200

    body = response.json()

    assert len(body["data"]) == 1
    assert body["meta"]["returned"] == 1
    assert body["meta"]["has_more"] is False

    assert (
        body["data"][0]["event_id"]
        == test_data["transactions"][1]["event_id"]
    )

    assert_request_id(response)


def test_transaction_detail_returns_database_record(
    api_client: TestClient,
    test_data: dict[str, Any],
) -> None:
    event_id = test_data["transactions"][0]["event_id"]

    response = api_client.get(f"/transactions/{event_id}")

    assert response.status_code == 200

    body = response.json()

    assert body["event_id"] == event_id
    assert body["source_system"] == "synthetic"
    assert Decimal(body["amount"]) == Decimal("100.00")


def test_transaction_cursor_pagination(
    api_client: TestClient,
    test_data: dict[str, Any],
) -> None:
    first_response = api_client.get(
        "/transactions",
        params={"limit": 2},
    )

    assert first_response.status_code == 200

    first_body = first_response.json()

    assert len(first_body["data"]) == 2
    assert first_body["meta"]["has_more"] is True

    next_cursor = first_body["meta"]["next_cursor"]
    assert next_cursor is not None

    second_response = api_client.get(
        "/transactions",
        params={
            "limit": 2,
            "cursor": next_cursor,
        },
    )

    assert second_response.status_code == 200

    second_body = second_response.json()

    assert len(second_body["data"]) == 1
    assert second_body["meta"]["has_more"] is False

    returned_ids = [
        item["event_id"]
        for item in first_body["data"] + second_body["data"]
    ]

    expected_ids = [
        tx["event_id"]
        for tx in reversed(test_data["transactions"])
    ]

    assert returned_ids == expected_ids


def test_transaction_not_found(
    api_client: TestClient,
) -> None:
    missing_id = "f" * 40

    response = api_client.get(f"/transactions/{missing_id}")

    error = assert_error(
        response,
        404,
        "RESOURCE_NOT_FOUND",
    )

    assert error["message"] == "Transaction not found"


# ---------------------------------------------------------------------------
# Fraud alert endpoints
# ---------------------------------------------------------------------------


def test_fraud_alert_list_returns_database_records(
    api_client: TestClient,
    test_data: dict[str, Any],
) -> None:
    response = api_client.get(
        "/fraud-alerts",
        params={"severity": "high"},
    )

    assert response.status_code == 200

    body = response.json()

    assert len(body["data"]) == 1
    assert (
        body["data"][0]["alert_id"]
        == test_data["alerts"][0]["alert_id"]
    )
    assert body["data"][0]["severity"] == "high"


def test_fraud_alert_detail_returns_database_record(
    api_client: TestClient,
    test_data: dict[str, Any],
) -> None:
    alert_id = test_data["alerts"][0]["alert_id"]

    response = api_client.get(f"/fraud-alerts/{alert_id}")

    assert response.status_code == 200

    body = response.json()

    assert body["alert_id"] == alert_id
    assert (
        body["event_id"]
        == test_data["transactions"][1]["event_id"]
    )
    assert Decimal(body["risk_score"]) == Decimal("0.7500")
    assert body["severity"] == "high"


def test_fraud_alert_not_found(
    api_client: TestClient,
) -> None:
    missing_id = "e" * 40

    response = api_client.get(f"/fraud-alerts/{missing_id}")

    error = assert_error(
        response,
        404,
        "RESOURCE_NOT_FOUND",
    )

    assert error["message"] == "Fraud alert not found"


# ---------------------------------------------------------------------------
# Analytics endpoints
# ---------------------------------------------------------------------------


def test_analytics_summary(
    api_client: TestClient,
    test_data: dict[str, Any],
) -> None:
    response = api_client.get("/analytics/summary")

    assert response.status_code == 200

    body = response.json()

    assert body["transaction_count"] == 3
    assert body["fraud_alert_count"] == 2
    assert body["latest_event_time"] is not None


def test_analytics_overview(
    api_client: TestClient,
    test_data: dict[str, Any],
) -> None:
    response = api_client.get(
        "/analytics/overview",
        params=analytics_params(test_data),
    )

    assert response.status_code == 200

    body = response.json()

    assert body["transaction_count"] == 3
    assert Decimal(body["total_transaction_value"]) == Decimal("600.00")
    assert Decimal(body["average_transaction_value"]) == Decimal("200")
    assert body["alert_count"] == 2
    assert body["alerted_transaction_count"] == 2
    assert Decimal(body["alert_rate_pct"]) == Decimal("66.6667")


def test_analytics_hourly_trends(
    api_client: TestClient,
    test_data: dict[str, Any],
) -> None:
    response = api_client.get(
        "/analytics/trends",
        params={
            **analytics_params(test_data),
            "granularity": "hour",
        },
    )

    assert response.status_code == 200

    body = response.json()

    assert body["granularity"] == "hour"
    assert len(body["data"]) == 2
    assert sum(
        row["transaction_count"] for row in body["data"]
    ) == 3
    assert sum(
        row["alert_count"] for row in body["data"]
    ) == 2


def test_analytics_daily_trends(
    api_client: TestClient,
    test_data: dict[str, Any],
) -> None:
    response = api_client.get(
        "/analytics/trends",
        params={
            **analytics_params(test_data),
            "granularity": "day",
        },
    )

    assert response.status_code == 200

    body = response.json()

    assert body["granularity"] == "day"
    assert len(body["data"]) == 2
    assert sum(
        row["transaction_count"] for row in body["data"]
    ) == 3
    assert sum(
        row["alert_count"] for row in body["data"]
    ) == 2


def test_analytics_distributions(
    api_client: TestClient,
    test_data: dict[str, Any],
) -> None:
    response = api_client.get(
        "/analytics/distributions",
        params=analytics_params(test_data),
    )

    assert response.status_code == 200

    body = response.json()

    assert sum(
        row["count"] for row in body["source_systems"]
    ) == 3

    assert sum(
        row["count"] for row in body["transaction_types"]
    ) == 3

    assert sum(
        row["count"] for row in body["alert_severities"]
    ) == 2


def test_analytics_rejects_ranges_over_90_days(
    api_client: TestClient,
) -> None:
    response = api_client.get(
        "/analytics/overview",
        params={
            "start_time": "2026-01-01T00:00:00Z",
            "end_time": "2026-04-02T00:00:00Z",
        },
    )

    error = assert_error(
        response,
        400,
        "INVALID_REQUEST",
    )

    assert "90 days" in error["message"]


# ---------------------------------------------------------------------------
# Customer risk and pipeline operations
# ---------------------------------------------------------------------------


def test_customer_risk(
    api_client: TestClient,
    test_data: dict[str, Any],
) -> None:
    customer_id = test_data["customer_id"]

    response = api_client.get(
        f"/customers/{customer_id}/risk",
    )

    assert response.status_code == 200

    body = response.json()

    assert body["customer_id"] == customer_id
    assert body["transaction_count"] == 3
    assert body["alert_count"] == 2
    assert (
        body["note"]
        == "Operational count summary; not a fraud probability."
    )


def test_pipeline_operations(
    api_client: TestClient,
    test_data: dict[str, Any],
) -> None:
    response = api_client.get("/operations/pipeline")

    assert response.status_code == 200

    body = response.json()

    processor = next(
        item
        for item in body["processors"]
        if item["processor_name"] == test_data["processor_name"]
    )

    assert processor["source_topic"] == "transactions.raw"
    assert processor["partition_count"] == 2
    assert processor["completed_partitions"] == 1
    assert processor["failed_partitions"] == 1
    assert processor["latest_offset"] == 20

    reconciliation = next(
        item
        for item in body["recent_reconciliations"]
        if item["run_id"] == str(test_data["reconciliation_id"])
    )

    assert reconciliation["status"] == "passed"
    assert reconciliation["metric_count"] == 2
    assert reconciliation["mismatch_count"] == 1


# ---------------------------------------------------------------------------
# Request context and error handling
# ---------------------------------------------------------------------------


def test_request_id_is_preserved(
    api_client: TestClient,
) -> None:
    request_id = "finstream-integration-test-001"

    response = api_client.get(
        "/live",
        headers={"X-Request-ID": request_id},
    )

    assert response.status_code == 200
    assert response.headers["x-request-id"] == request_id


def test_invalid_request_returns_standard_error(
    api_client: TestClient,
) -> None:
    response = api_client.get(
        "/transactions",
        params={"limit": 0},
    )

    assert_error(
        response,
        422,
        "VALIDATION_ERROR",
    )
