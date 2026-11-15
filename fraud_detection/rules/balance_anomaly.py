"""PaySim origin-account balance consistency rule."""

from __future__ import annotations

from decimal import Decimal

from fraud_detection.features.extractor import TransactionFeatures
from fraud_detection.rules.base import RuleSignal


def evaluate_balance_anomaly(
    features: TransactionFeatures,
    tolerance: Decimal = Decimal("1.00"),
) -> RuleSignal | None:
    """Flag material differences between expected and observed origin balances."""
    if not tolerance.is_finite() or tolerance < 0:
        raise ValueError("balance tolerance must be a non-negative finite amount")

    if features.source_system != "paysim":
        return None

    deviation = features.origin_balance_deviation
    if deviation is None or deviation <= tolerance:
        return None

    return RuleSignal(
        rule_name="paysim_balance_anomaly_v1",
        risk_score=Decimal("0.6500"),
        explanation=(
            f"PaySim origin balance movement deviated by {deviation}; "
            f"configured tolerance is {tolerance}"
        ),
    )