"""Source-aware, leakage-safe transaction feature extraction."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from ingestion.schemas.contracts import CanonicalTransaction


@dataclass(frozen=True)
class TransactionFeatures:
    event_id: str
    source_system: str
    amount: Decimal
    currency: str
    transaction_type: str | None
    origin_account_id: str | None
    destination_account_id: str | None
    origin_balance_before: Decimal | None = None
    origin_balance_after: Decimal | None = None
    origin_balance_expected_after: Decimal | None = None
    origin_balance_deviation: Decimal | None = None


def _decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return result if result.is_finite() else None


def extract_features(event: CanonicalTransaction) -> TransactionFeatures:
    """Extract only transaction-time features; never use historical fraud labels."""
    attributes = event.source_attributes
    transaction_type = (
        event.transaction_type.upper() if event.transaction_type else None
    )

    before = after = expected_after = deviation = None

    # PaySim-specific balance fields. These are not assumed to exist in IEEE-CIS.
    if event.source_system == "paysim":
        before = _decimal(attributes.get("oldbalanceOrg"))
        after = _decimal(attributes.get("newbalanceOrig"))

        if before is not None and after is not None and transaction_type:
            if transaction_type == "CASH_IN":
                expected_after = before + event.amount
            else:
                expected_after = before - event.amount

            deviation = abs(after - expected_after)

    return TransactionFeatures(
        event_id=event.event_id,
        source_system=event.source_system,
        amount=event.amount,
        currency=event.currency,
        transaction_type=transaction_type,
        origin_account_id=event.origin_account_id,
        destination_account_id=event.destination_account_id,
        origin_balance_before=before,
        origin_balance_after=after,
        origin_balance_expected_after=expected_after,
        origin_balance_deviation=deviation,
    )