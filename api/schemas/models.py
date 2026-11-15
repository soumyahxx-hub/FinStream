"""Validated Pydantic response contracts for the FinStream API."""

from decimal import Decimal
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

# ---------------------------------------------------------------------------
# Shared response configuration
# ---------------------------------------------------------------------------


class APIResponseModel(BaseModel):
    """Base configuration for API response models."""

    model_config = ConfigDict(
        extra="forbid",
        from_attributes=True,
    )


# ---------------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------------


class TransactionResponse(APIResponseModel):
    event_id: str
    source_system: Literal["paysim", "ieee_cis", "synthetic"]
    source_record_id: str
    event_time: AwareDatetime
    amount: Decimal = Field(ge=0)
    currency: str = Field(min_length=3, max_length=3)
    transaction_type: str | None = None


class TransactionPage(APIResponseModel):
    data: list[TransactionResponse]
    meta: "PageMeta"


# ---------------------------------------------------------------------------
# Fraud alerts
# ---------------------------------------------------------------------------


class FraudAlertResponse(APIResponseModel):
    alert_id: str
    event_id: str
    created_at: AwareDatetime
    rule_name: str
    risk_score: Decimal = Field(ge=0, le=1)
    severity: Literal["low", "medium", "high", "critical"]
    explanation: str


class AlertPage(APIResponseModel):
    data: list[FraudAlertResponse]
    meta: "PageMeta"


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------


class PageMeta(APIResponseModel):
    limit: int = Field(ge=1, le=200)
    offset: int = Field(ge=0)
    returned: int = Field(ge=0)
    has_more: bool = False
    next_cursor: str | None = None


# ---------------------------------------------------------------------------
# Basic analytics
# ---------------------------------------------------------------------------


class AnalyticsSummary(APIResponseModel):
    transaction_count: int = Field(ge=0)
    fraud_alert_count: int = Field(ge=0)
    latest_event_time: AwareDatetime | None = None


class AnalyticsOverview(APIResponseModel):
    start_time: AwareDatetime
    end_time: AwareDatetime

    transaction_count: int = Field(ge=0)
    total_transaction_value: Decimal = Field(ge=0)
    average_transaction_value: Decimal | None = Field(default=None, ge=0)

    # Total alert records and distinct transactions generating alerts.
    alert_count: int = Field(ge=0)
    alerted_transaction_count: int = Field(ge=0)

    # Percentage of transactions that generated at least one alert.
    # This is not a confirmed fraud rate.
    alert_rate_pct: Decimal | None = Field(default=None, ge=0, le=100)


# ---------------------------------------------------------------------------
# Time-series analytics
# ---------------------------------------------------------------------------


class TransactionTrendPoint(APIResponseModel):
    bucket: AwareDatetime
    transaction_count: int = Field(ge=0)
    total_transaction_value: Decimal = Field(ge=0)
    alert_count: int = Field(ge=0)


class AnalyticsTrends(APIResponseModel):
    start_time: AwareDatetime
    end_time: AwareDatetime
    granularity: Literal["hour", "day"]
    data: list[TransactionTrendPoint]


# ---------------------------------------------------------------------------
# Analytics distributions
# ---------------------------------------------------------------------------


class DistributionItem(APIResponseModel):
    category: str
    count: int = Field(ge=0)
    total_value: Decimal | None = Field(default=None, ge=0)


class AnalyticsDistributions(APIResponseModel):
    start_time: AwareDatetime
    end_time: AwareDatetime
    source_systems: list[DistributionItem]
    transaction_types: list[DistributionItem]
    alert_severities: list[DistributionItem]


# ---------------------------------------------------------------------------
# Customer operational risk summary
# ---------------------------------------------------------------------------


class CustomerRiskResponse(APIResponseModel):
    customer_id: str
    transaction_count: int = Field(ge=0)
    alert_count: int = Field(ge=0)
    note: str


# ---------------------------------------------------------------------------
# Pipeline processing operations
# ---------------------------------------------------------------------------


class ProcessorStatus(APIResponseModel):
    processor_name: str
    source_topic: str
    partition_count: int = Field(ge=0)
    completed_partitions: int = Field(ge=0)
    failed_partitions: int = Field(ge=0)
    latest_processed_at: AwareDatetime | None = None
    latest_offset: int | None = Field(default=None, ge=0)


class ReconciliationRunSummary(APIResponseModel):
    run_id: str
    run_type: Literal["replay", "backfill", "daily"]
    source_system: Literal["paysim", "ieee_cis", "synthetic"]
    started_at: AwareDatetime
    completed_at: AwareDatetime | None = None
    status: Literal["running", "passed", "failed"]
    metric_count: int = Field(ge=0)
    mismatch_count: int = Field(ge=0)


class PipelineOperations(APIResponseModel):
    processors: list[ProcessorStatus]
    recent_reconciliations: list[ReconciliationRunSummary]
