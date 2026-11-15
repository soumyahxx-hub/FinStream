"""FastAPI application composition and resource lifecycle."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from api.config import settings
from api.exception_handlers import (
    http_exception_handler,
    unexpected_exception_handler,
    validation_exception_handler,
)
from api.logging_config import configure_logging
from api.middleware.metrics import MetricsMiddleware
from api.middleware.request_context import RequestContextMiddleware
from api.routes.analytics import customer_router
from api.routes.analytics import router as analytics_router
from api.routes.fraud_alerts import router as fraud_alerts_router
from api.routes.health import router as health_router
from api.routes.operations import router as operations_router
from api.routes.transactions import router as transactions_router
from api.services.operational import (
    close_operational_repository,
    get_operational_repository,
)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    """Manage operational database resources."""
    if settings.database_url:
        get_operational_repository().open()

    try:
        yield
    finally:
        close_operational_repository()


configure_logging()

app = FastAPI(
    title="FinStream Operational API",
    summary="Read-only operational API for real-time transaction and fraud analytics.",
    version="0.1.0",
    description="""
FinStream provides read-only access to materialized transaction,
fraud detection, analytics, customer risk, and pipeline operations data.

The API exposes operational records and derived analytics.
An alert represents a rule-based risk signal, not confirmed financial fraud.

Historical dataset replays and synthetic events are used for development
and testing. They should not be interpreted as live banking traffic.
    """,
    openapi_tags=[
        {
            "name": "system",
            "description": "Health, readiness and service diagnostics.",
        },
        {
            "name": "transactions",
            "description": "Transaction retrieval, filtering and pagination.",
        },
        {
            "name": "fraud alerts",
            "description": "Fraud detection alerts and severity information.",
        },
        {
            "name": "analytics",
            "description": "Bounded operational analytics and transaction trends.",
        },
        {
            "name": "customers",
            "description": "Customer-level operational transaction and alert summaries.",
        },
        {
            "name": "operations",
            "description": "Streaming processor and reconciliation status.",
        },
    ],
    lifespan=lifespan,
)

# Centralized exception handling.
app.add_exception_handler(
    StarletteHTTPException,
    http_exception_handler,
)
app.add_exception_handler(
    RequestValidationError,
    validation_exception_handler,
)
app.add_exception_handler(
    Exception,
    unexpected_exception_handler,
)

# Middleware.
app.add_middleware(MetricsMiddleware)
app.add_middleware(RequestContextMiddleware)

# API routes.
app.include_router(health_router)
app.include_router(transactions_router)
app.include_router(fraud_alerts_router)
app.include_router(analytics_router)
app.include_router(customer_router)
app.include_router(operations_router)
