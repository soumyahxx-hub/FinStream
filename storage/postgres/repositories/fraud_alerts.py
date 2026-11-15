"""Idempotent PostgreSQL persistence for operational fraud alerts."""

from __future__ import annotations

import json

from psycopg import Connection

from ingestion.schemas.contracts import CanonicalTransaction, FraudAlert
from storage.postgres.repositories.transactions import UPSERT_TRANSACTION

INSERT_FRAUD_ALERT = """
INSERT INTO core.fraud_alerts (
    alert_id,
    event_id,
    created_at,
    rule_name,
    risk_score,
    severity,
    explanation
)
VALUES (
    %(alert_id)s,
    %(event_id)s,
    %(created_at)s,
    %(rule_name)s,
    %(risk_score)s,
    %(severity)s,
    %(explanation)s
)
ON CONFLICT DO NOTHING
"""


def insert_alert(connection: Connection, alert: FraudAlert) -> bool:
    """Persist an alert once; return True only when a new row is inserted."""
    with connection.cursor() as cursor:
        cursor.execute(INSERT_FRAUD_ALERT, alert.model_dump())
        inserted = cursor.rowcount == 1

    connection.commit()
    return inserted


def persist_detection_result(
    connection: Connection,
    event: CanonicalTransaction,
    alert: FraudAlert | None,
) -> bool:
    """Persist the validated event and optional alert in one database transaction.

    A Kafka offset must not be committed until this durable write succeeds.  The
    stable event and alert identities make a replay harmless to this store, but
    do not make the Kafka-to-PostgreSQL-to-Kafka flow exactly-once.
    """
    transaction_payload = event.model_dump()
    transaction_payload["source_attributes"] = json.dumps(
        transaction_payload.pop("source_attributes")
    )

    with connection.transaction():
        with connection.cursor() as cursor:
            cursor.execute(UPSERT_TRANSACTION, transaction_payload)
            if alert is None:
                return False

            cursor.execute(INSERT_FRAUD_ALERT, alert.model_dump())
            return cursor.rowcount == 1
