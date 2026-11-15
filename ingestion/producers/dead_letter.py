"""DLQ envelope and a deliberate handoff point for terminal processing failures."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field


class DeadLetterEnvelope(BaseModel):
    """An auditable payload for manual remediation and controlled replay."""

    event_id: str
    failed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    stage: str
    attempt_count: int = Field(ge=1)
    error_type: str
    error_message: str
    original_payload: dict[str, Any]
    replay_authorized: bool = False


def to_dead_letter(event_id: str, stage: str, attempt_count: int, error: Exception, payload: dict[str, Any]) -> DeadLetterEnvelope:
    """Create an envelope; publishing requires a configured DLQ serializer and review."""
    return DeadLetterEnvelope(
        event_id=event_id,
        stage=stage,
        attempt_count=attempt_count,
        error_type=type(error).__name__,
        error_message=str(error),
        original_payload=payload,
    )
