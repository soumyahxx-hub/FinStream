"""Kafka raw-to-validated transaction routing."""

from __future__ import annotations

import argparse
import json
import os
import signal
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

from ingestion.schemas.contracts import CanonicalTransaction
from ingestion.validators.transaction import validate_transaction


class ValidationConsumer:
    """Consume raw events and route accepted and rejected transactions."""

    def __init__(
        self,
        bootstrap_servers: str,
        registry_url: str,
        transaction_schema_path: Path,
        rejected_schema_path: Path,
        group_id: str,
    ) -> None:
        self.running = True
        self.registry = SchemaRegistryClient({"url": registry_url})

        transaction_schema = transaction_schema_path.read_text(encoding="utf-8")
        rejected_schema = rejected_schema_path.read_text(encoding="utf-8")

        self.deserializer = AvroDeserializer(
            self.registry, transaction_schema
        )
        self.accepted_serializer = AvroSerializer(
            self.registry, transaction_schema
        )
        self.rejected_serializer = AvroSerializer(
            self.registry, rejected_schema
        )
        self.key_serializer = StringSerializer("utf_8")

        producer_config = {
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "retries": 5,
        }

        self.accepted_producer = Producer(producer_config)
        self.rejected_producer = Producer(producer_config)

        self.consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        })
        self.consumer.subscribe(["transactions.raw"])

    def stop(self) -> None:
        self.running = False

    @staticmethod
    def _canonical_payload(decoded: dict[str, Any]) -> CanonicalTransaction:
        payload = dict(decoded)
        attributes = payload.pop("source_attributes_json", "{}")
        payload["source_attributes"] = json.loads(attributes or "{}")

        return CanonicalTransaction.model_validate(payload)

    def _publish(
        self,
        producer: Producer,
        serializer: AvroSerializer,
        topic: str,
        key: str,
        payload: dict[str, Any],
    ) -> None:
        context = SerializationContext(topic, MessageField.VALUE)
        errors: list[str] = []

        def on_delivery(error: Any, _: Any) -> None:
            if error is not None:
                errors.append(str(error))

        producer.produce(
            topic=topic,
            key=self.key_serializer(key),
            value=serializer(payload, context),
            on_delivery=on_delivery,
        )

        remaining = producer.flush(30)
        if remaining or errors:
            raise RuntimeError(
                f"Delivery failed for {topic}: "
                f"remaining={remaining}, errors={errors}"
            )

    def run(self, max_messages: int | None = None) -> None:
        processed = accepted = rejected = 0

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

                context = SerializationContext(
                    message.topic(), MessageField.VALUE
                )
                decoded = self.deserializer(message.value(), context)
                if decoded is None:
                    raise ValueError("Empty Avro transaction")

                event = self._canonical_payload(decoded)
                result = validate_transaction(event)

                if result.accepted:
                    payload = event.model_dump()
                    payload["source_attributes_json"] = json.dumps(
                        payload.pop("source_attributes"), sort_keys=True
                    )
                    self._publish(
                        self.accepted_producer,
                        self.accepted_serializer,
                        "transactions.validated",
                        event.event_id,
                        payload,
                    )
                    accepted += 1
                else:
                    rejected_payload = {
                        "event_id": event.event_id,
                        "source_system": event.source_system,
                        "source_record_id": event.source_record_id,
                        "rejected_at": int(
                            datetime.now(timezone.utc).timestamp() * 1000
                        ),
                        "reasons": list(result.reasons),
                        "payload_reference": event.event_id,
                    }
                    self._publish(
                        self.rejected_producer,
                        self.rejected_serializer,
                        "transactions.rejected",
                        event.event_id,
                        rejected_payload,
                    )
                    rejected += 1

                # Commit only after successful downstream delivery.
                self.consumer.commit(message=message, asynchronous=False)
                processed += 1

                if processed % 100 == 0:
                    print(
                        f"Processed={processed} accepted={accepted} "
                        f"rejected={rejected}",
                        flush=True,
                    )
        finally:
            self.consumer.close()
            self.accepted_producer.flush(30)
            self.rejected_producer.flush(30)

        print(
            f"Validation complete: processed={processed}, "
            f"accepted={accepted}, rejected={rejected}",
            flush=True,
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="FinStream raw transaction validator"
    )
    parser.add_argument("--max-messages", type=int)
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv(
            "KAFKA_EXTERNAL_BOOTSTRAP_SERVERS", "localhost:9092"
        ),
    )
    parser.add_argument(
        "--schema-registry-url",
        default=os.getenv(
            "SCHEMA_REGISTRY_EXTERNAL_URL", "http://localhost:8081"
        ),
    )
    parser.add_argument(
        "--group-id",
        default=os.getenv(
            "KAFKA_VALIDATION_GROUP_ID", "finstream-validation-v1"
        ),
    )
    args = parser.parse_args()

    if args.max_messages is not None and args.max_messages < 1:
        parser.error("--max-messages must be positive")

    app = ValidationConsumer(
        args.bootstrap_servers,
        args.schema_registry_url,
        Path("streaming/kafka/schemas/transaction.v1.avsc"),
        Path("streaming/kafka/schemas/rejected_transaction.v1.avsc"),
        args.group_id,
    )
    signal.signal(signal.SIGINT, lambda *_: app.stop())
    signal.signal(signal.SIGTERM, lambda *_: app.stop())
    app.run(args.max_messages)


if __name__ == "__main__":
    main()
