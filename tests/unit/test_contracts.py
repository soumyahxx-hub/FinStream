from datetime import datetime, timezone
from decimal import Decimal

from ingestion.schemas.contracts import CanonicalTransaction, stable_event_id


def test_event_id_is_stable_for_same_source_identity() -> None:
    assert stable_event_id("paysim", "row-1", "run-1") == stable_event_id("paysim", "row-1", "run-1")


def test_contract_normalizes_currency() -> None:
    event = CanonicalTransaction(
        event_id=stable_event_id("synthetic", "1", "run"),
        source_system="synthetic",
        source_record_id="1",
        simulation_id="run",
        event_time=datetime.now(timezone.utc),
        amount=Decimal("1.00"),
        currency="inr",
    )
    assert event.currency == "INR"
