from datetime import datetime, timezone
from decimal import Decimal

import pytest

from fraud_detection.engine import DetectionConfig, evaluate_transaction
from fraud_detection.features.extractor import extract_features
from fraud_detection.rules.high_value import evaluate_high_value
from fraud_detection.scoring.risk_scorer import (
    build_alert,
    classify_severity,
    combine_signals,
)
from fraud_detection.rules.base import RuleSignal
from ingestion.schemas.contracts import CanonicalTransaction, stable_event_id


def make_event(
    *,
    amount: str = "100.00",
    source: str = "synthetic",
    transaction_type: str = "TRANSFER",
    attributes: dict | None = None,
    historical_label: bool | None = None,
) -> CanonicalTransaction:
    return CanonicalTransaction(
        event_id=stable_event_id(source, "fraud-test-1", "test-run"),
        source_system=source,
        source_record_id="fraud-test-1",
        simulation_id="test-run",
        event_time=datetime.now(timezone.utc),
        amount=Decimal(amount),
        currency="INR",
        transaction_type=transaction_type,
        origin_account_id="account-1",
        destination_account_id="account-2",
        historical_fraud_label=historical_label,
        source_attributes=attributes or {},
    )


def test_normal_transaction_produces_no_alert() -> None:
    event = make_event(amount="500.00")
    assert evaluate_transaction(event) is None


def test_high_value_transaction_produces_alert() -> None:
    event = make_event(amount="250000.00")
    alert = evaluate_transaction(
        event,
        DetectionConfig(high_value_threshold=Decimal("100000.00")),
    )

    assert alert is not None
    assert alert.rule_name == "composite_v1"
    assert alert.risk_score == Decimal("0.5500")
    assert alert.severity == "medium"
    assert "high_value_v1" in alert.explanation


def test_threshold_boundary_triggers_high_value_rule() -> None:
    event = make_event(amount="100000.00")
    features = extract_features(event)

    signal = evaluate_high_value(features, Decimal("100000.00"))

    assert signal is not None
    assert signal.risk_score == Decimal("0.4500")


def test_amount_below_threshold_does_not_trigger() -> None:
    event = make_event(amount="99999.99")
    signal = evaluate_high_value(
        extract_features(event),
        Decimal("100000.00"),
    )
    assert signal is None


def test_paysim_balance_anomaly_produces_alert() -> None:
    event = make_event(
        source="paysim",
        amount="100.00",
        transaction_type="TRANSFER",
        attributes={
            "oldbalanceOrg": "1000.00",
            "newbalanceOrig": "950.00",
            "isFlaggedFraud": 0,
        },
    )

    alert = evaluate_transaction(event)

    assert alert is not None
    assert "paysim_balance_anomaly_v1" in alert.explanation
    assert alert.risk_score == Decimal("0.6500")


def test_consistent_paysim_balance_does_not_trigger() -> None:
    event = make_event(
        source="paysim",
        amount="100.00",
        transaction_type="TRANSFER",
        attributes={
            "oldbalanceOrg": "1000.00",
            "newbalanceOrig": "900.00",
        },
    )

    assert evaluate_transaction(event) is None


def test_cash_in_uses_incoming_balance_direction() -> None:
    event = make_event(
        source="paysim",
        amount="100.00",
        transaction_type="CASH_IN",
        attributes={
            "oldbalanceOrg": "1000.00",
            "newbalanceOrig": "1100.00",
        },
    )

    assert evaluate_transaction(event) is None


def test_historical_label_does_not_change_detection() -> None:
    clean_label = make_event(amount="500.00", historical_label=False)
    fraud_label = make_event(amount="500.00", historical_label=True)

    assert evaluate_transaction(clean_label) is None
    assert evaluate_transaction(fraud_label) is None


def test_paysim_flagged_fraud_field_is_not_used() -> None:
    event = make_event(
        source="paysim",
        amount="500.00",
        attributes={"isFlaggedFraud": 1},
    )

    assert evaluate_transaction(event) is None


def test_invalid_balance_values_are_ignored_safely() -> None:
    event = make_event(
        source="paysim",
        attributes={
            "oldbalanceOrg": "not-a-number",
            "newbalanceOrig": "900.00",
        },
    )

    assert evaluate_transaction(event) is None


def test_composite_score_combines_signals() -> None:
    signals = [
        RuleSignal("rule_a", Decimal("0.5000"), "reason A"),
        RuleSignal("rule_b", Decimal("0.5000"), "reason B"),
    ]

    assert combine_signals(signals) == Decimal("0.7500")


def test_alert_id_is_deterministic() -> None:
    signals = [RuleSignal("rule_a", Decimal("0.5000"), "reason A")]

    first = build_alert("event-1", signals)
    second = build_alert("event-1", signals)

    assert first.alert_id == second.alert_id


def test_severity_boundaries() -> None:
    assert classify_severity(Decimal("0.2999")) == "low"
    assert classify_severity(Decimal("0.3000")) == "medium"
    assert classify_severity(Decimal("0.6000")) == "high"
    assert classify_severity(Decimal("0.8000")) == "critical"


def test_invalid_detection_configuration_is_rejected() -> None:
    with pytest.raises(ValueError):
        DetectionConfig(high_value_threshold=Decimal("0"))

    with pytest.raises(ValueError):
        DetectionConfig(balance_tolerance=Decimal("-1"))