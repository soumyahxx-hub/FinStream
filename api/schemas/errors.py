"""Standardized API error response contracts."""

from pydantic import BaseModel, ConfigDict


class ValidationIssue(BaseModel):
    """Safe, client-facing request validation issue."""

    model_config = ConfigDict(extra="forbid")

    location: list[str | int]
    message: str
    error_type: str


class ErrorDetail(BaseModel):
    """Structured API error information."""

    model_config = ConfigDict(extra="forbid")

    code: str
    message: str
    request_id: str
    issues: list[ValidationIssue] | None = None


class ErrorResponse(BaseModel):
    """Standard error envelope returned by the API."""

    model_config = ConfigDict(extra="forbid")

    error: ErrorDetail
