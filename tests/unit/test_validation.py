from datetime import datetime, timezone
from decimal import Decimal

from ingestion.schemas.contracts import CanonicalTransaction, stable_event_id
from ingestion.validators.transaction import validate_transaction


def test_validator_rejects_unreasonably_large_amount() -> None:
    event = CanonicalTransaction(
        event_id=stable_event_id("synthetic", "2", "run"),
        source_system="synthetic",
        source_record_id="2",
        simulation_id="run",
        event_time=datetime.now(timezone.utc),
        amount=Decimal("1000000001.00"),
    )
    assert "amount_exceeds_contract_limit" in validate_transaction(event).reasons
