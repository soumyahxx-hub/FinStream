"""Filtered fraud alert routes with keyset pagination."""

from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, status

from api.schemas.models import AlertPage, FraudAlertResponse, PageMeta
from api.services.cursors import decode_cursor, encode_cursor
from api.services.operational import get_operational_repository

router = APIRouter(prefix="/fraud-alerts", tags=["fraud alerts"])

VALID_SEVERITIES = {"low", "medium", "high", "critical"}


@router.get("", response_model=AlertPage)
def list_alerts(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    cursor: str | None = Query(None, max_length=2048),
    severity: str | None = None,
    rule_name: str | None = None,
    event_id: str | None = None,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> AlertPage:
    if cursor is not None and offset != 0:
        raise HTTPException(400, "Do not combine cursor and offset.")

    if severity is not None and severity not in VALID_SEVERITIES:
        raise HTTPException(400, "Invalid severity.")

    if start_time is not None and end_time is not None:
        if start_time >= end_time:
            raise HTTPException(400, "start_time must be earlier than end_time.")

    cursor_position = None
    if cursor:
        try:
            cursor_position = decode_cursor(cursor)
        except ValueError as exc:
            raise HTTPException(400, "Invalid pagination cursor.") from exc

    rows, has_more = get_operational_repository().list_alerts(
        limit,
        offset,
        severity=severity,
        rule_name=rule_name,
        event_id=event_id,
        start_time=start_time,
        end_time=end_time,
        cursor_position=cursor_position,
    )

    data = [FraudAlertResponse.model_validate(row) for row in rows]
    next_cursor = None

    if has_more and data:
        last = data[-1]
        next_cursor = encode_cursor(last.created_at, last.alert_id)

    return AlertPage(
        data=data,
        meta=PageMeta(
            limit=limit,
            offset=offset,
            returned=len(data),
            has_more=has_more,
            next_cursor=next_cursor,
        ),
    )


@router.get("/{alert_id}", response_model=FraudAlertResponse)
def get_alert(alert_id: str) -> FraudAlertResponse:
    row = get_operational_repository().get_alert(alert_id)

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Fraud alert not found",
        )

    return FraudAlertResponse.model_validate(row)
