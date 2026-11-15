"""Deterministic composite scoring and alert construction."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import ROUND_HALF_UP, Decimal
from hashlib import blake2b
from typing import Literal

from fraud_detection.rules.base import RuleSignal
from ingestion.schemas.contracts import FraudAlert

FOUR_PLACES = Decimal("0.0001")


def classify_severity(score: Decimal) -> Literal["low", "medium", "high", "critical"]:
    """Map a normalized risk score to an operational severity."""
    if not Decimal("0") <= score <= Decimal("1"):
        raise ValueError("risk score must be between 0 and 1")

    if score < Decimal("0.3000"):
        return "low"
    if score < Decimal("0.6000"):
        return "medium"
    if score < Decimal("0.8000"):
        return "high"
    return "critical"


def combine_signals(signals: Sequence[RuleSignal]) -> Decimal:
    """Combine independent heuristic signals using a capped noisy-OR score."""
    if not signals:
        return Decimal("0.0000")

    remaining_risk = Decimal("1")
    for signal in signals:
        remaining_risk *= Decimal("1") - signal.risk_score

    score = Decimal("1") - remaining_risk
    return score.quantize(FOUR_PLACES, rounding=ROUND_HALF_UP)


def build_alert(event_id: str, signals: Sequence[RuleSignal]) -> FraudAlert:
    """Create one deterministic, explainable composite alert per transaction."""
    if not signals:
        raise ValueError("cannot create an alert without rule signals")

    ordered = sorted(signals, key=lambda item: item.rule_name)
    score = combine_signals(ordered)

    # Stable across retries and replay of the same event and signal set.
    identity = "|".join(
        [event_id, *(f"{item.rule_name}:{item.risk_score}" for item in ordered)]
    )
    alert_id = blake2b(identity.encode("utf-8"), digest_size=20).hexdigest()

    explanation = " | ".join(
        f"[{item.rule_name}] {item.explanation}" for item in ordered
    )

    return FraudAlert(
        alert_id=alert_id,
        event_id=event_id,
        rule_name="composite_v1",
        risk_score=score,
        severity=classify_severity(score),
        explanation=explanation,
    )
