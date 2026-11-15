"""IEEE-CIS adapter that intentionally preserves its separate source domain."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Mapping

from ingestion.schemas.contracts import CanonicalTransaction, stable_event_id


def adapt_ieee_cis_row(
    row: Mapping[str, str], simulation_id: str, reference_time: datetime
) -> CanonicalTransaction:
    """Map IEEE-CIS `TransactionDT` relative time without inventing a universal identity.

    The supplied reference time and feature mapping must be recorded in dataset
    metadata before this adapter is used for a real replay.
    """
    transaction_id = row.get("TransactionID")
    transaction_dt = row.get("TransactionDT")
    if not transaction_id or transaction_dt is None:
        raise ValueError("IEEE-CIS row requires TransactionID and TransactionDT")
    if reference_time.tzinfo is None:
        raise ValueError("reference_time must be timezone-aware")
    epoch = reference_time.astimezone(timezone.utc).replace(microsecond=0)
    event_time = datetime.fromtimestamp(epoch.timestamp() + int(transaction_dt), tz=timezone.utc)
    retained = {key: value for key, value in row.items() if key in {"ProductCD", "card4", "card6", "P_emaildomain"}}
    return CanonicalTransaction(
        event_id=stable_event_id("ieee_cis", transaction_id, simulation_id),
        source_system="ieee_cis",
        source_record_id=transaction_id,
        simulation_id=simulation_id,
        event_time=event_time,
        amount=Decimal(row["TransactionAmt"]),
        transaction_type=row.get("ProductCD"),
        historical_fraud_label=row.get("isFraud") == "1" if "isFraud" in row else None,
        source_attributes=retained,
    )
