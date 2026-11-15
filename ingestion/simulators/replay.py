"""Bounded PaySim CSV replay into Kafka using Confluent Avro."""

from __future__ import annotations

import argparse
import csv
import os
import time
from pathlib import Path

from ingestion.adapters.paysim import adapt_paysim_row
from ingestion.producers.avro_producer import (
    KafkaSettings,
    TransactionAvroProducer,
)


def iter_paysim_events(path: Path, simulation_id: str, max_events: int | None):
    """Yield PaySim events with deterministic source-row identities."""
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row_number, row in enumerate(reader, start=1):
            if max_events is not None and row_number > max_events:
                break
            row["source_record_id"] = f"paysim-row-{row_number}"
            yield adapt_paysim_row(row, simulation_id)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay PaySim CSV transactions into Kafka."
    )
    parser.add_argument("csv_path", type=Path)
    parser.add_argument("--simulation-id", required=True)
    parser.add_argument("--max-events", type=int)
    parser.add_argument("--events-per-second", type=float, default=20.0)
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_EXTERNAL_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument(
        "--schema-registry-url",
        default=os.getenv("SCHEMA_REGISTRY_EXTERNAL_URL", "http://localhost:8081"),
    )
    args = parser.parse_args()

    if args.max_events is not None and args.max_events < 1:
        parser.error("--max-events must be positive")
    if args.events_per_second <= 0:
        parser.error("--events-per-second must be positive")
    if not args.csv_path.is_file():
        parser.error(f"CSV file not found: {args.csv_path}")

    schema_path = Path("streaming/kafka/schemas/transaction.v1.avsc")
    if not schema_path.is_file():
        parser.error(f"Avro schema not found: {schema_path}")

    producer = TransactionAvroProducer(
        KafkaSettings(
            bootstrap_servers=args.bootstrap_servers,
            schema_registry_url=args.schema_registry_url,
            client_id="finstream-paysim-replay",
        ),
        schema_path.read_text(encoding="utf-8"),
    )

    published = 0
    started = time.monotonic()

    try:
        for event in iter_paysim_events(
            args.csv_path, args.simulation_id, args.max_events
        ):
            producer.publish(event, topic="transactions.raw")
            published += 1

            if published % 100 == 0:
                elapsed = time.monotonic() - started
                print(
                    f"Published {published:,} events "
                    f"({published / max(elapsed, 0.001):.1f} events/sec)",
                    flush=True,
                )

            time.sleep(1 / args.events_per_second)
    finally:
        elapsed = time.monotonic() - started
        print(
            f"Replay finished. Confirmed publish calls: {published:,}. "
            f"Elapsed: {elapsed:.1f}s",
            flush=True,
        )


if __name__ == "__main__":
    main()
