"""Bounded analytics endpoints and customer operational summaries."""

from datetime import datetime, timedelta
from typing import Literal

from fastapi import APIRouter, HTTPException, Query

from api.schemas.models import (
    AnalyticsDistributions,
    AnalyticsOverview,
    AnalyticsSummary,
    AnalyticsTrends,
    CustomerRiskResponse,
    TransactionTrendPoint,
)
from api.services.operational import get_operational_repository

router = APIRouter(prefix="/analytics", tags=["analytics"])


MAX_ANALYTICS_RANGE = timedelta(days=90)


def validate_time_range(start_time: datetime, end_time: datetime) -> None:
    if (
        start_time.tzinfo is None
        or start_time.utcoffset() is None
        or end_time.tzinfo is None
        or end_time.utcoffset() is None
    ):
        raise HTTPException(400, "start_time and end_time must include a timezone.")

    if start_time >= end_time:
        raise HTTPException(400, "start_time must be earlier than end_time.")

    if end_time - start_time > MAX_ANALYTICS_RANGE:
        raise HTTPException(400, "Analytics time range cannot exceed 90 days.")


@router.get("/summary", response_model=AnalyticsSummary)
def summary() -> AnalyticsSummary:
    return AnalyticsSummary.model_validate(get_operational_repository().summary())


@router.get("/overview", response_model=AnalyticsOverview)
def overview(
    start_time: datetime,
    end_time: datetime,
) -> AnalyticsOverview:
    validate_time_range(start_time, end_time)
    result = get_operational_repository().analytics_overview(start_time, end_time)
    return AnalyticsOverview(
        start_time=start_time,
        end_time=end_time,
        **result,
    )


@router.get("/trends", response_model=AnalyticsTrends)
def trends(
    start_time: datetime,
    end_time: datetime,
    granularity: Literal["hour", "day"] = Query("day"),
) -> AnalyticsTrends:
    validate_time_range(start_time, end_time)

    rows = get_operational_repository().transaction_trends(start_time, end_time, granularity)

    return AnalyticsTrends(
        start_time=start_time,
        end_time=end_time,
        granularity=granularity,
        data=[TransactionTrendPoint.model_validate(row) for row in rows],
    )


@router.get("/distributions", response_model=AnalyticsDistributions)
def distributions(
    start_time: datetime,
    end_time: datetime,
) -> AnalyticsDistributions:
    validate_time_range(start_time, end_time)

    result = get_operational_repository().analytics_distributions(start_time, end_time)

    return AnalyticsDistributions(
        start_time=start_time,
        end_time=end_time,
        source_systems=result["source_systems"],
        transaction_types=result["transaction_types"],
        alert_severities=result["alert_severities"],
    )


customer_router = APIRouter(prefix="/customers", tags=["customers"])


@customer_router.get("/{customer_id}/risk", response_model=CustomerRiskResponse)
def customer_risk(customer_id: str) -> CustomerRiskResponse:
    row = get_operational_repository().customer_risk(customer_id)

    return CustomerRiskResponse(
        customer_id=customer_id,
        **row,
        note="Operational count summary; not a fraud probability.",
    )
