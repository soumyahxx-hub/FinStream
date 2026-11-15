"""Health and metrics endpoints."""

import logging

from fastapi import APIRouter, HTTPException, Response, status
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from api.config import settings
from api.services.operational import get_operational_repository

logger = logging.getLogger(__name__)

router = APIRouter(tags=["system"])


@router.get("/live", summary="Check API process liveness")
def liveness() -> dict[str, str]:
    """Confirm that the API process is running."""
    return {"status": "alive"}


def _readiness_response() -> dict[str, str]:
    """Check whether the API can reach its operational database."""
    try:
        repository = get_operational_repository()
        repository.healthcheck()
    except Exception as exc:
        logger.exception("API readiness check failed")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Operational database is unavailable.",
        ) from exc

    return {"status": "ready", "environment": settings.environment}


@router.get("/ready", summary="Check API dependency readiness")
def readiness() -> dict[str, str]:
    """Confirm that required operational dependencies are reachable."""
    return _readiness_response()


@router.get("/health", summary="Legacy API health check")
def health() -> dict[str, str]:
    """Backward-compatible readiness endpoint."""
    return _readiness_response()


@router.get("/metrics", include_in_schema=False)
def metrics() -> Response:
    """Expose Prometheus metrics."""
    return Response(
        generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
        status_code=status.HTTP_200_OK,
    )
