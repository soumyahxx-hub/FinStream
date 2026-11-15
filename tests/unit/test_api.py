"""Unit tests for the FinStream operational API."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import Mock

from fastapi.testclient import TestClient

from api.main import app
from api.services.cursors import decode_cursor

client = TestClient(app)

EVENT_TIME = datetime(2026, 11, 14, 10, 0, tzinfo=UTC)

TRANSACTION = {
    "event_id": "a" * 40,
    "source_system": "synthetic",
    "source_record_id": "api-test-001",
    "event_time": EVENT_TIME,
    "amount": Decimal("250000.00"),
    "currency": "INR",
    "transaction_type": "TRANSFER",
}

ALERT = {
    "alert_id": "b" * 40,
    "event_id": "a" * 40,
    "created_at": EVENT_TIME,
    "rule_name": "composite_v1",
    "risk_score": Decimal("0.8500"),
    "severity": "critical",
    "explanation": "High-value transaction triggered a risk rule.",
}


def install_repository(monkeypatch, repository: Mock) -> None:
    """Replace repository access in each route module."""
    monkeypatch.setattr(
        "api.routes.transactions.get_operational_repository",
        lambda: repository,
    )
    monkeypatch.setattr(
        "api.routes.fraud_alerts.get_operational_repository",
        lambda: repository,
    )
    monkeypatch.setattr(
        "api.routes.analytics.get_operational_repository",
        lambda: repository,
    )
    monkeypatch.setattr(
        "api.routes.operations.get_operational_repository",
        lambda: repository,
    )


def assert_standard_error(response, expected_code: str) -> dict:
    """Verify the standardized API error envelope."""
    assert "x-request-id" in response.headers

    body = response.json()
    assert "error" in body

    error = body["error"]
    assert error["code"] == expected_code
    assert error["request_id"]
    assert error["request_id"] == response.headers["x-request-id"]

    return error


# ---------------------------------------------------------------------------
# Transaction endpoints
# ---------------------------------------------------------------------------


def test_transaction_pagination_returns_next_cursor(monkeypatch) -> None:
    repository = Mock()
    repository.list_transactions.return_value = ([TRANSACTION], True)
    install_repository(monkeypatch, repository)

    response = client.get("/transactions", params={"limit": 1})

    assert response.status_code == 200
    body = response.json()
    assert len(body["data"]) == 1
    assert body["meta"]["returned"] == 1
    assert body["meta"]["has_more"] is True
    assert body["meta"]["next_cursor"] is not None

    cursor_position = decode_cursor(body["meta"]["next_cursor"])
    assert cursor_position[1] == TRANSACTION["event_id"]


def test_transaction_filters_are_forwarded(monkeypatch) -> None:
    repository = Mock()
    repository.list_transactions.return_value = ([TRANSACTION], False)
    install_repository(monkeypatch, repository)

    response = client.get(
        "/transactions",
        params={
            "source_system": "synthetic",
            "transaction_type": "TRANSFER",
            "currency": "INR",
            "min_amount": "1000",
            "max_amount": "300000",
        },
    )

    assert response.status_code == 200
    kwargs = repository.list_transactions.call_args.kwargs
    assert kwargs["source_system"] == "synthetic"
    assert kwargs["transaction_type"] == "TRANSFER"
    assert kwargs["currency"] == "INR"
    assert kwargs["min_amount"] == Decimal("1000")
    assert kwargs["max_amount"] == Decimal("300000")


def test_transaction_rejects_invalid_amount_range(monkeypatch) -> None:
    repository = Mock()
    install_repository(monkeypatch, repository)

    response = client.get(
        "/transactions",
        params={"min_amount": "500", "max_amount": "100"},
    )

    assert response.status_code == 400
    error = assert_standard_error(response, "INVALID_REQUEST")
    assert error["message"] == "min_amount cannot exceed max_amount."
    repository.list_transactions.assert_not_called()


def test_transaction_rejects_cursor_with_offset(monkeypatch) -> None:
    repository = Mock()
    install_repository(monkeypatch, repository)

    response = client.get(
        "/transactions",
        params={"cursor": "invalid", "offset": 1},
    )

    assert response.status_code == 400
    assert_standard_error(response, "INVALID_REQUEST")
    repository.list_transactions.assert_not_called()


def test_transaction_not_found(monkeypatch) -> None:
    repository = Mock()
    repository.get_transaction.return_value = None
    install_repository(monkeypatch, repository)

    response = client.get("/transactions/missing")

    assert response.status_code == 404
    error = assert_standard_error(response, "RESOURCE_NOT_FOUND")
    assert error["message"] == "Transaction not found"


def test_database_unavailable_returns_503(monkeypatch) -> None:
    import api.services.operational as operational

    monkeypatch.setattr(operational.settings, "database_url", None)
    monkeypatch.setattr(operational, "_repository", None)

    response = client.get("/transactions")

    assert response.status_code == 503
    error = assert_standard_error(response, "SERVICE_UNAVAILABLE")
    assert error["message"] == "Operational database is not configured."


# ---------------------------------------------------------------------------
# Fraud alert endpoints
# ---------------------------------------------------------------------------


def test_alert_pagination_returns_next_cursor(monkeypatch) -> None:
    repository = Mock()
    repository.list_alerts.return_value = ([ALERT], True)
    install_repository(monkeypatch, repository)

    response = client.get("/fraud-alerts", params={"limit": 1})

    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["has_more"] is True
    assert body["meta"]["next_cursor"] is not None

    cursor_position = decode_cursor(body["meta"]["next_cursor"])
    assert cursor_position[1] == ALERT["alert_id"]


def test_alert_filters_are_forwarded(monkeypatch) -> None:
    repository = Mock()
    repository.list_alerts.return_value = ([ALERT], False)
    install_repository(monkeypatch, repository)

    response = client.get(
        "/fraud-alerts",
        params={
            "severity": "critical",
            "rule_name": "composite_v1",
            "event_id": "a" * 40,
        },
    )

    assert response.status_code == 200
    kwargs = repository.list_alerts.call_args.kwargs
    assert kwargs["severity"] == "critical"
    assert kwargs["rule_name"] == "composite_v1"
    assert kwargs["event_id"] == "a" * 40


def test_alert_rejects_invalid_severity(monkeypatch) -> None:
    repository = Mock()
    install_repository(monkeypatch, repository)

    response = client.get("/fraud-alerts", params={"severity": "extreme"})

    assert response.status_code == 400
    error = assert_standard_error(response, "INVALID_REQUEST")
    assert error["message"] == "Invalid severity."
    repository.list_alerts.assert_not_called()


def test_alert_not_found(monkeypatch) -> None:
    repository = Mock()
    repository.get_alert.return_value = None
    install_repository(monkeypatch, repository)

    response = client.get("/fraud-alerts/missing")

    assert response.status_code == 404
    error = assert_standard_error(response, "RESOURCE_NOT_FOUND")
    assert error["message"] == "Fraud alert not found"


# ---------------------------------------------------------------------------
# Analytics endpoints
# ---------------------------------------------------------------------------


def test_analytics_overview(monkeypatch) -> None:
    repository = Mock()
    repository.analytics_overview.return_value = {
        "transaction_count": 10,
        "total_transaction_value": Decimal("500000.00"),
        "average_transaction_value": Decimal("50000.00"),
        "alert_count": 2,
        "alerted_transaction_count": 2,
        "alert_rate_pct": Decimal("20.0000"),
    }
    install_repository(monkeypatch, repository)

    response = client.get(
        "/analytics/overview",
        params={
            "start_time": "2026-11-14T00:00:00Z",
            "end_time": "2026-11-15T00:00:00Z",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["transaction_count"] == 10
    assert Decimal(body["alert_rate_pct"]) == Decimal("20.0000")


def test_analytics_rejects_timezone_naive_range(monkeypatch) -> None:
    repository = Mock()
    install_repository(monkeypatch, repository)

    response = client.get(
        "/analytics/overview",
        params={
            "start_time": "2026-11-14T00:00:00",
            "end_time": "2026-11-15T00:00:00",
        },
    )

    assert response.status_code == 400
    assert_standard_error(response, "INVALID_REQUEST")
    repository.analytics_overview.assert_not_called()


def test_analytics_rejects_invalid_time_range(monkeypatch) -> None:
    repository = Mock()
    install_repository(monkeypatch, repository)

    response = client.get(
        "/analytics/overview",
        params={
            "start_time": "2026-11-15T00:00:00Z",
            "end_time": "2026-11-14T00:00:00Z",
        },
    )

    assert response.status_code == 400
    assert_standard_error(response, "INVALID_REQUEST")
    repository.analytics_overview.assert_not_called()


def test_analytics_rejects_ranges_over_90_days(monkeypatch) -> None:
    repository = Mock()
    install_repository(monkeypatch, repository)

    response = client.get(
        "/analytics/overview",
        params={
            "start_time": "2026-01-01T00:00:00Z",
            "end_time": "2026-04-02T00:00:00Z",
        },
    )

    assert response.status_code == 400
    error = assert_standard_error(response, "INVALID_REQUEST")
    assert error["message"] == ("Analytics time range cannot exceed 90 days.")
    repository.analytics_overview.assert_not_called()


def test_analytics_trends(monkeypatch) -> None:
    repository = Mock()
    repository.transaction_trends.return_value = [
        {
            "bucket": EVENT_TIME,
            "transaction_count": 10,
            "total_transaction_value": Decimal("500000.00"),
            "alert_count": 2,
        }
    ]
    install_repository(monkeypatch, repository)

    response = client.get(
        "/analytics/trends",
        params={
            "start_time": "2026-11-14T00:00:00Z",
            "end_time": "2026-11-15T00:00:00Z",
            "granularity": "hour",
        },
    )

    assert response.status_code == 200
    assert len(response.json()["data"]) == 1
    assert response.json()["granularity"] == "hour"


def test_analytics_rejects_invalid_granularity(monkeypatch) -> None:
    repository = Mock()
    install_repository(monkeypatch, repository)

    response = client.get(
        "/analytics/trends",
        params={
            "start_time": "2026-11-14T00:00:00Z",
            "end_time": "2026-11-15T00:00:00Z",
            "granularity": "minute",
        },
    )

    assert response.status_code == 422
    error = assert_standard_error(response, "VALIDATION_ERROR")
    assert error["issues"]
    repository.transaction_trends.assert_not_called()


def test_analytics_distributions(monkeypatch) -> None:
    repository = Mock()
    repository.analytics_distributions.return_value = {
        "source_systems": [
            {
                "category": "synthetic",
                "count": 10,
                "total_value": Decimal("500000"),
            }
        ],
        "transaction_types": [
            {
                "category": "TRANSFER",
                "count": 10,
                "total_value": Decimal("500000"),
            }
        ],
        "alert_severities": [
            {
                "category": "critical",
                "count": 2,
                "total_value": None,
            }
        ],
    }
    install_repository(monkeypatch, repository)

    response = client.get(
        "/analytics/distributions",
        params={
            "start_time": "2026-11-14T00:00:00Z",
            "end_time": "2026-11-15T00:00:00Z",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["source_systems"][0]["category"] == "synthetic"
    assert body["alert_severities"][0]["category"] == "critical"


# ---------------------------------------------------------------------------
# Pipeline operations
# ---------------------------------------------------------------------------


def test_pipeline_operations(monkeypatch) -> None:
    repository = Mock()
    repository.pipeline_operations.return_value = {
        "processors": [
            {
                "processor_name": "fraud-detection",
                "source_topic": "transactions.validated",
                "partition_count": 3,
                "completed_partitions": 3,
                "failed_partitions": 0,
                "latest_processed_at": EVENT_TIME,
                "latest_offset": 340,
            }
        ],
        "recent_reconciliations": [],
    }
    install_repository(monkeypatch, repository)

    response = client.get("/operations/pipeline")

    assert response.status_code == 200
    body = response.json()
    assert body["processors"][0]["partition_count"] == 3
    assert body["processors"][0]["latest_offset"] == 340
    assert body["recent_reconciliations"] == []


# ---------------------------------------------------------------------------
# Health and readiness
# ---------------------------------------------------------------------------


def test_liveness_does_not_require_database(monkeypatch) -> None:
    import api.services.operational as operational

    monkeypatch.setattr(operational.settings, "database_url", None)
    monkeypatch.setattr(operational, "_repository", None)

    response = client.get("/live")

    assert response.status_code == 200
    assert response.json()["status"] == "alive"


def test_readiness_returns_503_when_database_is_unavailable(
    monkeypatch,
) -> None:
    import api.services.operational as operational

    monkeypatch.setattr(operational.settings, "database_url", None)
    monkeypatch.setattr(operational, "_repository", None)

    response = client.get("/ready")

    assert response.status_code == 503
    error = assert_standard_error(response, "SERVICE_UNAVAILABLE")
    assert error["message"] == "Operational database is unavailable."


def test_metrics_endpoint_returns_prometheus_payload() -> None:
    client.get("/live")

    response = client.get("/metrics")

    assert response.status_code == 200
    assert "finstream_api_requests_total" in response.text
    assert "finstream_api_request_duration_seconds" in response.text


def test_openapi_contains_financial_api_metadata() -> None:
    response = client.get("/openapi.json")

    assert response.status_code == 200
    schema = response.json()

    assert schema["info"]["title"] == "FinStream Operational API"
    assert "transactions" in schema["tags"][1]["name"].lower()
    assert "/operations/pipeline" in schema["paths"]


# ---------------------------------------------------------------------------
# Request correlation IDs
# ---------------------------------------------------------------------------


def test_request_id_is_generated_when_missing() -> None:
    response = client.get("/live")

    request_id = response.headers.get("x-request-id")

    assert response.status_code == 200
    assert request_id is not None
    assert len(request_id) == 32


def test_valid_request_id_is_preserved() -> None:
    request_id = "finstream-test-001"

    response = client.get(
        "/live",
        headers={"X-Request-ID": request_id},
    )

    assert response.status_code == 200
    assert response.headers["x-request-id"] == request_id


def test_invalid_request_id_is_replaced() -> None:
    response = client.get(
        "/live",
        headers={"X-Request-ID": "invalid request id"},
    )

    request_id = response.headers.get("x-request-id")

    assert response.status_code == 200
    assert request_id is not None
    assert request_id != "invalid request id"
    assert len(request_id) == 32


# ---------------------------------------------------------------------------
# Standardized error responses
# ---------------------------------------------------------------------------


def test_validation_error_uses_standard_envelope() -> None:
    response = client.get("/transactions?limit=0")

    assert response.status_code == 422

    error = assert_standard_error(response, "VALIDATION_ERROR")

    assert error["message"] == "Request validation failed."
    assert isinstance(error["issues"], list)
    assert error["issues"]


def test_bad_cursor_uses_standard_envelope() -> None:
    response = client.get("/transactions?cursor=invalid-cursor")

    assert response.status_code == 400

    error = assert_standard_error(response, "INVALID_REQUEST")
    assert error["message"] == "Invalid pagination cursor."
