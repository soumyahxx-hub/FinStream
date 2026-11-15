from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import ANY, Mock

import pytest

import fraud_detection.consumers.alert_customer as consumer_module
from fraud_detection.consumers.alert_customer import FraudAlertConsumer
from fraud_detection.engine import DetectionConfig
from ingestion.schemas.contracts import CanonicalTransaction, stable_event_id


class Message:
    def __init__(self, payload: bytes = b"payload") -> None:
        self._payload = payload

    def topic(self) -> str:
        return "transactions.validated"

    def partition(self) -> int:
        return 1

    def offset(self) -> int:
        return 42

    def key(self) -> bytes:
        return b"a" * 40

    def value(self) -> bytes:
        return self._payload


def event_payload() -> dict[str, object]:
    event = CanonicalTransaction(
        event_id=stable_event_id("synthetic", "consumer-test", "run"),
        source_system="synthetic",
        source_record_id="consumer-test",
        simulation_id="run",
        event_time=datetime.now(UTC),
        amount=Decimal("200000.00"),
    )
    payload = event.model_dump()
    payload["source_attributes_json"] = json.dumps(payload.pop("source_attributes"))
    return payload


def bare_consumer() -> FraudAlertConsumer:
    app = FraudAlertConsumer.__new__(FraudAlertConsumer)
    app.config = DetectionConfig()
    app.transaction_deserializer = lambda _value, _context: event_payload()
    app.consumer = Mock()
    app.max_processing_attempts = 2
    return app


def test_persists_and_publishes_before_committing(monkeypatch: pytest.MonkeyPatch) -> None:
    app = bare_consumer()
    persisted: list[tuple[CanonicalTransaction, object]] = []
    actions: list[str] = []

    def persist(event: CanonicalTransaction, alert: object) -> bool:
        persisted.append((event, alert))
        actions.append("persist")
        return True

    def publish(_alert: object) -> None:
        actions.append("publish")

    monkeypatch.setattr(
        consumer_module,
        "persist_detection_result",
        lambda _connection, event, alert: persist(event, alert),
    )
    app.connection = object()
    app._publish_alert = publish
    app.consumer.commit.side_effect = lambda **_kwargs: actions.append("commit")

    assert app.process_message(Message()) is True
    assert persisted and persisted[0][0].event_id
    assert actions == ["persist", "publish", "commit"]
    app.consumer.commit.assert_called_once_with(message=ANY, asynchronous=False)


def test_does_not_commit_when_persistence_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    app = bare_consumer()
    app.connection = object()
    app._publish_alert = Mock()
    monkeypatch.setattr(
        consumer_module,
        "persist_detection_result",
        Mock(side_effect=RuntimeError("database unavailable")),
    )

    with pytest.raises(RuntimeError, match="database unavailable"):
        app.process_message(Message())

    app._publish_alert.assert_not_called()
    app.consumer.commit.assert_not_called()


def test_terminal_failure_is_dlqed_before_commit(monkeypatch: pytest.MonkeyPatch) -> None:
    app = bare_consumer()
    app.process_message = Mock(side_effect=ValueError("bad record"))
    app._publish_dead_letter = Mock()
    monkeypatch.setattr(consumer_module.time, "sleep", lambda _seconds: None)
    message = Message()

    assert app.process_with_retries(message) is False

    app._publish_dead_letter.assert_called_once()
    assert app._publish_dead_letter.call_args.args[0] is message
    assert app._publish_dead_letter.call_args.args[1] == 2
    app.consumer.commit.assert_called_once_with(message=message, asynchronous=False)


def test_failed_message_without_a_key_uses_a_stable_fallback_id() -> None:
    app = bare_consumer()
    message = Message(b"unreadable-avro")
    message.key = lambda: None  # type: ignore[method-assign]

    assert len(app._event_id_for_failure(message)) == 40
