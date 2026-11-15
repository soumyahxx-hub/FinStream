"""Route-normalized request metrics."""

from __future__ import annotations

from time import perf_counter

from fastapi import Request
from starlette.middleware.base import (
    BaseHTTPMiddleware,
    RequestResponseEndpoint,
)
from starlette.responses import Response

from api.observability import HTTP_DURATION, HTTP_REQUESTS


class MetricsMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        started = perf_counter()
        status_code = 500

        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            route = request.scope.get("route")
            path = getattr(route, "path", "unmatched")

            HTTP_REQUESTS.labels(
                request.method,
                path,
                status_code,
            ).inc()

            HTTP_DURATION.labels(
                request.method,
                path,
            ).observe(perf_counter() - started)
