"""Filtered transaction routes with keyset pagination."""

from datetime import datetime
from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, status

from api.schemas.models import (
    PageMeta,
    TransactionPage,
    TransactionResponse,
)
from api.services.cursors import decode_cursor, encode_cursor
from api.services.operational import get_operational_repository

router = APIRouter(prefix="/transactions", tags=["transactions"])


@router.get("", response_model=TransactionPage)
def list_transactions(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    cursor: str | None = Query(None, max_length=2048),
    source_system: str | None = None,
    transaction_type: str | None = None,
    currency: Annotated[str | None, Query(min_length=3, max_length=3)] = None,
    min_amount: Annotated[Decimal | None, Query(ge=0)] = None,
    max_amount: Annotated[Decimal | None, Query(ge=0)] = None,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> TransactionPage:
    if cursor is not None and offset != 0:
        raise HTTPException(400, "Do not combine cursor and offset.")

    if min_amount is not None and max_amount is not None:
        if min_amount > max_amount:
            raise HTTPException(400, "min_amount cannot exceed max_amount.")

    if start_time is not None and end_time is not None:
        if start_time >= end_time:
            raise HTTPException(400, "start_time must be earlier than end_time.")

    cursor_position = None
    if cursor:
        try:
            cursor_position = decode_cursor(cursor)
        except ValueError as exc:
            raise HTTPException(400, "Invalid pagination cursor.") from exc

    rows, has_more = get_operational_repository().list_transactions(
        limit,
        offset,
        source_system=source_system,
        transaction_type=transaction_type,
        currency=currency,
        min_amount=min_amount,
        max_amount=max_amount,
        start_time=start_time,
        end_time=end_time,
        cursor_position=cursor_position,
    )

    data = [TransactionResponse.model_validate(row) for row in rows]
    next_cursor = None

    if has_more and data:
        last = data[-1]
        next_cursor = encode_cursor(last.event_time, last.event_id)

    return TransactionPage(
        data=data,
        meta=PageMeta(
            limit=limit,
            offset=offset,
            returned=len(data),
            has_more=has_more,
            next_cursor=next_cursor,
        ),
    )


@router.get("/{transaction_id}", response_model=TransactionResponse)
def get_transaction(transaction_id: str) -> TransactionResponse:
    row = get_operational_repository().get_transaction(transaction_id)

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Transaction not found",
        )

    return TransactionResponse.model_validate(row)
