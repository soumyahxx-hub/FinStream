"""Common types for explainable fraud-detection rules."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class RuleSignal:
    rule_name: str
    risk_score: Decimal
    explanation: str

    def __post_init__(self) -> None:
        if not self.rule_name.strip():
            raise ValueError("rule_name cannot be empty")
        if not Decimal("0") <= self.risk_score <= Decimal("1"):
            raise ValueError("risk_score must be between 0 and 1")
        if not self.explanation.strip():
            raise ValueError("explanation cannot be empty")