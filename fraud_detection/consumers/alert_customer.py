"""Consume validated transactions and publish/persist fraud alerts."""

from __future__ import annotations

import argparse
import base64
import json
import os
import signal
import time
from hashlib import sha1
from pathlib import Path
from typing import Any

from confluent_kafka import (  # type: ignore[import-untyped]
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
)
from confluent_kafka.schema_registry import SchemaRegistryClient  # type: ignore[import-untyped]
from confluent_kafka.schema_registry.avro import (  # type: ignore[import-untyped]
    AvroDeserializer,
    AvroSerializer,
)
from confluent_kafka.serialization import (  # type: ignore[import-untyped]
    MessageField,
    SerializationContext,
    StringSerializer,
)
from psycopg import Connection, connect

from fraud_detection.engine import DetectionConfig, evaluate_transaction
from ingestion.schemas.contracts import CanonicalTransaction
from storage.postgres.repositories.fraud_alerts import persist_detection_result


class FraudAlertConsumer:
    """At-least-once fraud detection with idempotent operational persistence."""

    def __init__(
        self,
        bootstrap_servers: str,
        registry_url: str,
        database_url: str,
        transaction_schema_path: Path,
        alert_schema_path: Path,
        dead_letter_schema_path: Path,
        group_id: str,
        config: DetectionConfig,
        max_processing_attempts: int = 3,
    ) -> None:
        if max_processing_attempts < 1:
            raise ValueError("max_processing_attempts must be positive")

        self.running = True
        self.config = config
        self.max_processing_attempts = max_processing_attempts

        registry = SchemaRegistryClient({"url": registry_url})
        transaction_schema = transaction_schema_path.read_text(encoding="utf-8")
        alert_schema = alert_schema_path.read_text(encoding="utf-8")
        dead_letter_schema = dead_letter_schema_path.read_text(encoding="utf-8")

        self.transaction_deserializer = AvroDeserializer(
            registry,
            transaction_schema,
        )
        self.alert_serializer = AvroSerializer(registry, alert_schema)
        self.dead_letter_serializer = AvroSerializer(registry, dead_letter_schema)
        self.key_serializer = StringSerializer("utf_8")

        producer_config = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "finstream-fraud-alert-producer",
            "acks": "all",
            "enable.idempotence": True,
            "retries": 5,
            "retry.backoff.ms": 250,
            "delivery.timeout.ms": 120000,
            "max.in.flight.requests.per.connection": 5,
        }
        self.producer = Producer(producer_config)

        self.consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "client.id": "finstream-fraud-detection",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "isolation.level": "read_committed",
        })
        self.consumer.subscribe(["transactions.validated"])

        self.connection: Connection = connect(database_url)

    @staticmethod
    def _decode_transaction(decoded: dict[str, Any]) -> CanonicalTransaction:
        payload = dict(decoded)
        attributes = payload.pop("source_attributes_json", "{}")
        payload["source_attributes"] = json.loads(attributes or "{}")
        return CanonicalTransaction.model_validate(payload)

    def _publish(
        self,
        topic: str,
        key: str,
        payload: dict[str, Any],
        serializer: AvroSerializer,
    ) -> None:
        context = SerializationContext(topic, MessageField.VALUE)
        delivery_errors: list[str] = []

        def on_delivery(error: Any, _: Any) -> None:
            if error is not None:
                delivery_errors.append(str(error))

        self.producer.produce(
            topic=topic,
            key=self.key_serializer(key),
            value=serializer(payload, context),
            on_delivery=on_delivery,
        )

        remaining = self.producer.flush(30)
        if remaining or delivery_errors:
            raise RuntimeError(
                f"Fraud alert delivery failed: remaining={remaining}, "
                f"errors={delivery_errors}"
            )

    def _publish_alert(self, alert: Any) -> None:
        self._publish(
            "fraud.alerts",
            alert.alert_id,
            alert.model_dump(),
            self.alert_serializer,
        )

    @staticmethod
    def _event_id_for_failure(message: Any) -> str:
        key = message.key()
        if isinstance(key, bytes):
            try:
                key = key.decode("utf-8")
            except UnicodeDecodeError:
                key = None
        if isinstance(key, str) and key:
            return key

        value = message.value() or b""
        if isinstance(value, str):
            value = value.encode("utf-8")
        return sha1(value).hexdigest()

    def _publish_dead_letter(
        self,
        message: Any,
        attempt_count: int,
        error: Exception,
    ) -> None:
        value = message.value() or b""
        if isinstance(value, str):
            value = value.encode("utf-8")
        payload: dict[str, Any] = {
            "event_id": self._event_id_for_failure(message),
            "failed_at": int(time.time() * 1000),
            "stage": "fraud_detection",
            "attempt_count": attempt_count,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "original_payload_json": json.dumps(
                {
                    "topic": message.topic(),
                    "partition": message.partition(),
                    "offset": message.offset(),
                    "key": self._event_id_for_failure(message),
                    "value_base64": base64.b64encode(value).decode("ascii"),
                },
                sort_keys=True,
            ),
            "replay_authorized": False,
        }
        self._publish(
            "transactions.dead-letter",
            payload["event_id"],
            payload,
            self.dead_letter_serializer,
        )

    def process_message(self, message: Any) -> bool:
        """Process one message. Return True when an alert was generated."""
        context = SerializationContext(
            message.topic(),
            MessageField.VALUE,
        )
        decoded = self.transaction_deserializer(message.value(), context)
        if decoded is None:
            raise ValueError("Empty validated transaction")

        event = self._decode_transaction(decoded)
        alert = evaluate_transaction(event, self.config)

        # Persist before publishing. A publication failure will replay the
        # message; stable IDs make the database write idempotent and allow the
        # alert publication to be retried. Kafka and PostgreSQL still have no
        # shared transaction, so this is deliberately at-least-once.
        persist_detection_result(self.connection, event, alert)
        if alert is not None:
            self._publish_alert(alert)

        # No alert is also a successfully processed transaction.
        self.consumer.commit(message=message, asynchronous=False)
        return alert is not None

    def process_with_retries(self, message: Any) -> bool:
        """Retry a record, then DLQ it before committing a terminal failure."""
        for attempt in range(1, self.max_processing_attempts + 1):
            try:
                return self.process_message(message)
            except Exception as error:
                if attempt == self.max_processing_attempts:
                    self._publish_dead_letter(message, attempt, error)
                    self.consumer.commit(message=message, asynchronous=False)
                    return False
                time.sleep(0.25 * attempt)

        raise AssertionError("unreachable")

    def run(self, max_messages: int | None = None) -> None:
        processed = alerts = 0

        try:
            while self.running:
                if max_messages is not None and processed >= max_messages:
                    break

                message = self.consumer.poll(1.0)
                if message is None:
                    continue

                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(message.error())

                if self.process_with_retries(message):
                    alerts += 1

                processed += 1
                if processed % 100 == 0:
                    print(
                        f"Fraud detection processed={processed} alerts={alerts}",
                        flush=True,
                    )
        finally:
            self.consumer.close()
            self.producer.flush(30)
            self.connection.close()

        print(
            f"Fraud detection complete: processed={processed}, alerts={alerts}",
            flush=True,
        )

    def stop(self) -> None:
        self.running = False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="FinStream real-time fraud detection consumer"
    )
    parser.add_argument("--max-messages", type=int)
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_EXTERNAL_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument(
        "--schema-registry-url",
        default=os.getenv("SCHEMA_REGISTRY_EXTERNAL_URL", "http://localhost:8081"),
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL"),
    )
    parser.add_argument(
        "--group-id",
        default=os.getenv(
            "KAFKA_FRAUD_GROUP_ID",
            "finstream-fraud-detection-v1",
        ),
    )
    parser.add_argument(
        "--high-value-threshold",
        default=os.getenv("FRAUD_HIGH_VALUE_THRESHOLD", "100000.00"),
    )
    parser.add_argument(
        "--balance-tolerance",
        default=os.getenv("FRAUD_BALANCE_TOLERANCE", "1.00"),
    )
    parser.add_argument(
        "--max-processing-attempts",
        type=int,
        default=int(os.getenv("FRAUD_MAX_PROCESSING_ATTEMPTS", "3")),
    )
    args = parser.parse_args()

    if args.max_messages is not None and args.max_messages < 1:
        parser.error("--max-messages must be positive")
    if args.max_processing_attempts < 1:
        parser.error("--max-processing-attempts must be positive")
    if not args.database_url:
        parser.error("DATABASE_URL must be set")

    from decimal import Decimal

    config = DetectionConfig(
        high_value_threshold=Decimal(args.high_value_threshold),
        balance_tolerance=Decimal(args.balance_tolerance),
    )

    root = Path(__file__).resolve().parents[2]
    app = FraudAlertConsumer(
        bootstrap_servers=args.bootstrap_servers,
        registry_url=args.schema_registry_url,
        database_url=args.database_url,
        transaction_schema_path=root / "streaming/kafka/schemas/transaction.v1.avsc",
        alert_schema_path=root / "streaming/kafka/schemas/fraud_alert.v1.avsc",
        dead_letter_schema_path=root / "streaming/kafka/schemas/dead_letter.v1.avsc",
        group_id=args.group_id,
        config=config,
        max_processing_attempts=args.max_processing_attempts,
    )

    signal.signal(signal.SIGINT, lambda *_: app.stop())
    signal.signal(signal.SIGTERM, lambda *_: app.stop())
    app.run(args.max_messages)


if __name__ == "__main__":
    main()
