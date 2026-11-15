"""Kafka producer configuration with bounded retries and delivery acknowledgement."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from ingestion.schemas.contracts import CanonicalTransaction


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str
    schema_registry_url: str
    client_id: str = "finstream-replay"


class TransactionAvroProducer:
    """Publishes canonical records to Kafka using a stable key and idempotent producer mode."""

    def __init__(self, settings: KafkaSettings, schema_text: str) -> None:
        registry = SchemaRegistryClient({"url": settings.schema_registry_url})
        self._producer = Producer(
            {
                "bootstrap.servers": settings.bootstrap_servers,
                "client.id": settings.client_id,
                "acks": "all",
                "enable.idempotence": True,
                "retries": 5,
                "retry.backoff.ms": 250,
                "delivery.timeout.ms": 120000,
                "max.in.flight.requests.per.connection": 5,
            }
        )
        self._key_serializer = StringSerializer("utf_8")
        self._value_serializer = AvroSerializer(registry, schema_text)

    def publish(self, event: CanonicalTransaction, topic: str = "transactions.raw") -> None:
        """Block for delivery confirmation; caller decides retry/DLQ policy on failure."""
        payload: dict[str, Any] = event.model_dump()
        payload["source_attributes_json"] = json.dumps(payload.pop("source_attributes"), sort_keys=True)
        delivery_errors: list[str] = []

        def delivery_callback(error: Any, _: Any) -> None:
            if error is not None:
                delivery_errors.append(str(error))

        self._producer.produce(
            topic=topic,
            key=self._key_serializer(event.event_id),
            value=self._value_serializer(payload, SerializationContext(topic, MessageField.VALUE)),
            on_delivery=delivery_callback,
        )
        outstanding = self._producer.flush(30)
        if outstanding or delivery_errors:
            raise TimeoutError(f"Kafka delivery failed; outstanding={outstanding}, errors={delivery_errors}")
