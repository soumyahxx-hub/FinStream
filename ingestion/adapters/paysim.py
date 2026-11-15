"""PaySim row adapter; it does not download or bundle the dataset."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Mapping

from ingestion.schemas.contracts import CanonicalTransaction, stable_event_id


def adapt_paysim_row(row: Mapping[str, str], simulation_id: str) -> CanonicalTransaction:
    """Map a PaySim CSV row while retaining source fields as provenance.

    PaySim's `step` is an hour offset, not a real event timestamp. The caller must
    provide a chosen simulation epoch and record that policy in run metadata.
    """
    record_id = row.get("source_record_id") or row.get("nameOrig", "") + ":" + row.get("step", "")
    if not record_id or record_id == ":":
        raise ValueError("PaySim adapter requires source_record_id or nameOrig and step")
    epoch = datetime(2020, 1, 1, tzinfo=timezone.utc)
    event_time = epoch + timedelta(hours=int(row["step"]))
    source_attributes = {
        key: row[key]
        for key in ("step", "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest", "isFlaggedFraud")
        if key in row
    }
    return CanonicalTransaction(
        event_id=stable_event_id("paysim", record_id, simulation_id),
        source_system="paysim",
        source_record_id=record_id,
        simulation_id=simulation_id,
        event_time=event_time,
        amount=Decimal(row["amount"]),
        transaction_type=row.get("type"),
        origin_account_id=row.get("nameOrig"),
        destination_account_id=row.get("nameDest"),
        historical_fraud_label=row.get("isFraud") == "1",
        source_attributes=source_attributes,
    )
