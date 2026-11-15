"""Validation produces explicit, routable failures rather than silently dropping rows."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from ingestion.schemas.contracts import CanonicalTransaction


@dataclass(frozen=True)
class ValidationResult:
    accepted: bool
    reasons: tuple[str, ...]


def validate_transaction(event: CanonicalTransaction) -> ValidationResult:
    """Apply inexpensive contract checks before the Kafka producer publishes an event."""
    reasons: list[str] = []
    if event.amount > Decimal("1000000000"):
        reasons.append("amount_exceeds_contract_limit")
    if event.origin_account_id == event.destination_account_id and event.origin_account_id:
        reasons.append("same_origin_and_destination")
    if not event.source_record_id.strip():
        reasons.append("missing_source_record_id")
    return ValidationResult(accepted=not reasons, reasons=tuple(reasons))
