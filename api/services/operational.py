"""Shared operational repository lifecycle."""

from __future__ import annotations

from fastapi import HTTPException, status

from api.config import settings
from api.repositories.operational import OperationalRepository

_repository: OperationalRepository | None = None


def get_operational_repository() -> OperationalRepository:
    global _repository

    if not settings.database_url:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Operational database is not configured.",
        )

    if _repository is None:
        _repository = OperationalRepository(settings.database_url)

    return _repository


def close_operational_repository() -> None:
    global _repository

    if _repository is not None:
        _repository.close()
        _repository = None
