"""Canonical event model shared by source-specific adapters and downstream services.

The stable event identifier deliberately uses source provenance. It is deterministic
across replay attempts, unlike a generated UUID, and is therefore usable for
deduplication and idempotent storage. It is not a guarantee of end-to-end
exactly-once delivery.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from hashlib import blake2b
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

SourceSystem = Literal["paysim", "ieee_cis", "synthetic"]


def stable_event_id(source_system: str, source_record_id: str, simulation_id: str | None) -> str:
    """Create a deterministic opaque ID from the natural identity of an event."""
    material = "\x1f".join((source_system, source_record_id, simulation_id or ""))
    return blake2b(material.encode("utf-8"), digest_size=20).hexdigest()


class CanonicalTransaction(BaseModel):
    """Source-preserving financial transaction contract, version 1.

    `source_attributes` holds noncanonical fields rather than merging incompatible
    PaySim and IEEE-CIS domains. Sensitive attributes must be minimized before
    production use.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    event_id: str = Field(min_length=40, max_length=40)
    event_version: Literal[1] = 1
    source_system: SourceSystem
    source_record_id: str = Field(min_length=1, max_length=256)
    simulation_id: str | None = Field(default=None, max_length=128)
    event_time: datetime
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    amount: Decimal = Field(ge=Decimal("0"), max_digits=18, decimal_places=2)
    currency: str = Field(default="INR", min_length=3, max_length=3)
    transaction_type: str | None = Field(default=None, max_length=64)
    origin_account_id: str | None = Field(default=None, max_length=256)
    destination_account_id: str | None = Field(default=None, max_length=256)
    merchant_id: str | None = Field(default=None, max_length=256)
    historical_fraud_label: bool | None = None
    source_attributes: dict[str, Any] = Field(default_factory=dict)

    @field_validator("currency")
    @classmethod
    def normalize_currency(cls, value: str) -> str:
        return value.upper()

    @field_validator("event_time", "ingested_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("timestamps must be timezone-aware")
        return value.astimezone(timezone.utc)


class FraudAlert(BaseModel):
    """Operational alert, separate from a dataset's historical fraud label."""

    model_config = ConfigDict(extra="forbid")

    alert_id: str
    event_id: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    rule_name: str
    risk_score: Decimal = Field(ge=Decimal("0"), le=Decimal("1"))
    severity: Literal["low", "medium", "high", "critical"]
    explanation: str
