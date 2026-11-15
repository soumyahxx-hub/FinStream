"""Opaque, URL-safe cursors for stable keyset pagination."""

from __future__ import annotations

import base64
import json
from datetime import UTC, datetime


def encode_cursor(timestamp: datetime, record_id: str) -> str:
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("Cursor timestamps must be timezone-aware.")

    payload = json.dumps(
        {"t": timestamp.isoformat(), "i": record_id},
        separators=(",", ":"),
    ).encode("utf-8")

    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def decode_cursor(cursor: str) -> tuple[datetime, str]:
    if not cursor or len(cursor) > 2048:
        raise ValueError("Invalid cursor.")

    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        payload = base64.b64decode(
            padded,
            altchars=b"-_",
            validate=True,
        )
        decoded = json.loads(payload)

        timestamp = datetime.fromisoformat(decoded["t"])
        record_id = decoded["i"]

        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError("Cursor timestamp must include a timezone.")

        if not isinstance(record_id, str) or not record_id:
            raise ValueError("Cursor record ID is invalid.")

        return timestamp.astimezone(UTC), record_id

    except (ValueError, TypeError, KeyError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Invalid pagination cursor.") from exc
