"""Deterministic logical paths for Parquet outputs."""

from __future__ import annotations

from datetime import datetime


def transaction_partition(source_system: str, event_time: datetime) -> str:
    """Return a source/date partition path without embedding raw source records."""
    return f"transactions/source_system={source_system}/event_date={event_time.date().isoformat()}"
