"""Configurable high-value transaction detection."""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP

from fraud_detection.features.extractor import TransactionFeatures
from fraud_detection.rules.base import RuleSignal
from ingestion.schemas.contracts import CanonicalTransaction, FraudAlert


def evaluate_high_value(
    features: TransactionFeatures,
    threshold: Decimal,
) -> RuleSignal | None:
    """Return a risk signal when the transaction reaches the configured threshold."""
    if not threshold.is_finite() or threshold <= 0:
        raise ValueError("high-value threshold must be a positive finite amount")

    if features.amount < threshold:
        return None

    ratio = features.amount / threshold

    # Transparent, deterministic tiers. These are initial heuristic weights,
    # not statistically calibrated fraud probabilities.
    if ratio >= Decimal("100"):
        score = Decimal("0.8500")
    elif ratio >= Decimal("10"):
        score = Decimal("0.7500")
    elif ratio >= Decimal("5"):
        score = Decimal("0.6500")
    elif ratio >= Decimal("2"):
        score = Decimal("0.5500")
    else:
        score = Decimal("0.4500")

    score = score.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    return RuleSignal(
        rule_name="high_value_v1",
        risk_score=score,
        explanation=(
            f"Transaction amount {features.amount} {features.currency} "
            f"reached the configured threshold {threshold} "
            f"({ratio:.2f}x threshold)"
        ),
    )


def high_value_alert(
    event: CanonicalTransaction,
    threshold: Decimal,
) -> FraudAlert | None:
    """Backward-compatible high-value rule interface."""
    from fraud_detection.scoring.risk_scorer import build_alert

    from fraud_detection.features.extractor import extract_features

    signal = evaluate_high_value(extract_features(event), threshold)
    if signal is None:
        return None
    return build_alert(event.event_id, [signal])