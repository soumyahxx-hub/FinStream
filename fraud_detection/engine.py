"""Main transaction fraud-detection engine."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from fraud_detection.features.extractor import extract_features
from fraud_detection.rules.balance_anomaly import evaluate_balance_anomaly
from fraud_detection.rules.high_value import evaluate_high_value
from fraud_detection.rules.base import RuleSignal
from fraud_detection.scoring.risk_scorer import build_alert
from ingestion.schemas.contracts import CanonicalTransaction, FraudAlert


@dataclass(frozen=True)
class DetectionConfig:
    high_value_threshold: Decimal = Decimal("100000.00")
    balance_tolerance: Decimal = Decimal("1.00")

    def __post_init__(self) -> None:
        if (
            not self.high_value_threshold.is_finite()
            or self.high_value_threshold <= 0
        ):
            raise ValueError("high_value_threshold must be positive and finite")
        if (
            not self.balance_tolerance.is_finite()
            or self.balance_tolerance < 0
        ):
            raise ValueError("balance_tolerance must be non-negative and finite")


def evaluate_transaction(
    event: CanonicalTransaction,
    config: DetectionConfig | None = None,
) -> FraudAlert | None:
    """Evaluate enabled rules and return one composite alert when triggered."""
    settings = config or DetectionConfig()
    features = extract_features(event)

    signals: list[RuleSignal] = []

    high_value = evaluate_high_value(features, settings.high_value_threshold)
    if high_value is not None:
        signals.append(high_value)

    balance_anomaly = evaluate_balance_anomaly(
        features,
        settings.balance_tolerance,
    )
    if balance_anomaly is not None:
        signals.append(balance_anomaly)

    if not signals:
        return None

    return build_alert(event.event_id, signals)