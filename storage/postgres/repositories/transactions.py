"""Idempotent operational-store write primitives."""

from __future__ import annotations

import json
from collections.abc import Iterable

from psycopg import Connection

from ingestion.schemas.contracts import CanonicalTransaction


UPSERT_TRANSACTION = """
INSERT INTO core.transactions (
  event_id, event_version, source_system, source_record_id, simulation_id,
  event_time, ingested_at, amount, currency, transaction_type,
  origin_account_id, destination_account_id, merchant_id,
  historical_fraud_label, source_attributes
) VALUES (
  %(event_id)s, %(event_version)s, %(source_system)s, %(source_record_id)s, %(simulation_id)s,
  %(event_time)s, %(ingested_at)s, %(amount)s, %(currency)s, %(transaction_type)s,
  %(origin_account_id)s, %(destination_account_id)s, %(merchant_id)s,
  %(historical_fraud_label)s, %(source_attributes)s::jsonb
)
ON CONFLICT (event_id) DO NOTHING
"""


def insert_events(connection: Connection, events: Iterable[CanonicalTransaction]) -> int:
    """Insert only unseen stable event identities and return inserted row count."""
    payloads = []
    for event in events:
        payload = event.model_dump()
        payload["source_attributes"] = json.dumps(payload.pop("source_attributes"))
        payloads.append(payload)
    if not payloads:
        return 0
    with connection.cursor() as cursor:
        cursor.executemany(UPSERT_TRANSACTION, payloads)
        inserted = cursor.rowcount
    connection.commit()
    return inserted
