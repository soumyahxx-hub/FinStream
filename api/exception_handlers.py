"""Centralized and safe API exception handling."""

from __future__ import annotations

import logging
from collections.abc import Mapping

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from api.schemas.errors import (
    ErrorDetail,
    ErrorResponse,
    ValidationIssue,
)

logger = logging.getLogger("api.errors")


def get_request_id(request: Request) -> str:
    """Retrieve the correlation ID assigned by request middleware."""
    return getattr(request.state, "request_id", "unknown")


def get_route(request: Request) -> str:
    """Return the route template without exposing raw URL parameters."""
    route = request.scope.get("route")
    return getattr(route, "path", "unmatched")


def build_error_response(
    request: Request,
    status_code: int,
    code: str,
    message: str,
    *,
    issues: list[ValidationIssue] | None = None,
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    """Build a consistent JSON error response."""
    payload = ErrorResponse(
        error=ErrorDetail(
            code=code,
            message=message,
            request_id=get_request_id(request),
            issues=issues,
        )
    )

    return JSONResponse(
        status_code=status_code,
        content=payload.model_dump(mode="json", exclude_none=True),
        headers=dict(headers) if headers is not None else None,
    )


async def http_exception_handler(
    request: Request,
    exc: Exception,
) -> JSONResponse:
    """Normalize expected HTTP errors."""
    if not isinstance(exc, StarletteHTTPException):
        raise exc

    error_codes = {
        400: ("INVALID_REQUEST", "The request could not be processed."),
        401: ("UNAUTHORIZED", "Authentication is required."),
        403: (
            "FORBIDDEN",
            "You do not have permission to access this resource.",
        ),
        404: ("RESOURCE_NOT_FOUND", "The requested resource was not found."),
        405: ("METHOD_NOT_ALLOWED", "The HTTP method is not allowed."),
        409: (
            "CONFLICT",
            "The request conflicts with the current resource state.",
        ),
        422: ("VALIDATION_ERROR", "Request validation failed."),
        429: ("RATE_LIMITED", "Too many requests."),
        503: (
            "SERVICE_UNAVAILABLE",
            "The requested service is unavailable.",
        ),
    }

    code, default_message = error_codes.get(
        exc.status_code,
        ("HTTP_ERROR", "The request could not be completed."),
    )

    message = (
        exc.detail if isinstance(exc.detail, str) else default_message
    )

    if exc.status_code >= 500:
        logger.warning(
            "HTTP service error",
            extra={
                "request_id": get_request_id(request),
                "method": request.method,
                "route": get_route(request),
                "status_code": exc.status_code,
            },
        )

    return build_error_response(
        request,
        exc.status_code,
        code,
        message,
        headers=exc.headers,
    )


async def validation_exception_handler(
    request: Request,
    exc: Exception,
) -> JSONResponse:
    """Normalize validation failures without exposing submitted values."""
    if not isinstance(exc, RequestValidationError):
        raise exc

    issues = [
        ValidationIssue(
            location=list(error["loc"]),
            message=error["msg"],
            error_type=error["type"],
        )
        for error in exc.errors()
    ]

    return build_error_response(
        request,
        422,
        "VALIDATION_ERROR",
        "Request validation failed.",
        issues=issues,
    )


async def unexpected_exception_handler(
    request: Request,
    exc: Exception,
) -> JSONResponse:
    """Log unexpected errors internally and return a safe response."""
    logger.exception(
        "Unhandled API exception",
        extra={
            "request_id": get_request_id(request),
            "method": request.method,
            "route": get_route(request),
            "status_code": 500,
        },
    )

    return build_error_response(
        request,
        500,
        "INTERNAL_SERVER_ERROR",
        "An unexpected internal error occurred.",
    )