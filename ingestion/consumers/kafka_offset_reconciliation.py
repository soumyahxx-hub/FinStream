"""Read-only Kafka partition offset inspection and PostgreSQL reconciliation."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any, Literal

import psycopg
from confluent_kafka import Consumer, KafkaException, TopicPartition  # type: ignore[import-untyped]

LOGGER = logging.getLogger("finstream.kafka_reconciliation")

PartitionStatus = Literal[
    "ok",
    "empty_uncommitted",
    "missing_commit",
    "offset_out_of_range",
    "committed_ahead",
]


@dataclass(frozen=True)
class PartitionOffset:
    """Offset state for one Kafka topic partition."""

    topic: str
    partition: int
    low_watermark: int
    log_end_offset: int
    committed_offset: int | None
    lag: int | None
    status: PartitionStatus


@dataclass(frozen=True)
class KafkaOffsetReport:
    """Partition-level Kafka offset reconciliation report."""

    run_id: uuid.UUID
    group_id: str
    topic: str
    status: Literal["passed", "failed"]
    partitions: tuple[PartitionOffset, ...]

    @property
    def total_lag(self) -> int:
        return sum(partition.lag or 0 for partition in self.partitions)

    @property
    def missing_commit_count(self) -> int:
        return sum(partition.status == "missing_commit" for partition in self.partitions)


def inspect_topic_offsets(
    consumer: Any,
    topic: str,
    timeout: float = 10.0,
) -> tuple[PartitionOffset, ...]:
    """Inspect partition watermarks and committed offsets without consuming."""
    metadata = consumer.list_topics(topic=topic, timeout=timeout)
    topic_metadata = metadata.topics.get(topic)

    if topic_metadata is None:
        raise RuntimeError(f"Kafka topic does not exist: {topic}")

    if topic_metadata.error is not None:
        raise KafkaException(topic_metadata.error)

    partition_ids = sorted(topic_metadata.partitions)
    if not partition_ids:
        raise RuntimeError(f"Kafka topic has no partitions: {topic}")

    requested = [TopicPartition(topic, partition_id) for partition_id in partition_ids]

    committed_partitions = consumer.committed(
        requested,
        timeout=timeout,
    )
    committed_by_partition = {item.partition: item.offset for item in committed_partitions}

    results: list[PartitionOffset] = []

    for partition_id in partition_ids:
        low, high = consumer.get_watermark_offsets(
            TopicPartition(topic, partition_id),
            timeout=timeout,
            cached=False,
        )

        committed = committed_by_partition.get(partition_id)

        if committed is None or committed < 0:
            if high > low:
                status: PartitionStatus = "missing_commit"
                lag: int | None = None
            else:
                status = "empty_uncommitted"
                lag = 0
        elif committed < low:
            status = "offset_out_of_range"
            lag = high - low
        elif committed > high:
            status = "committed_ahead"
            lag = None
        else:
            status = "ok"
            lag = high - committed

        results.append(
            PartitionOffset(
                topic=topic,
                partition=partition_id,
                low_watermark=low,
                log_end_offset=high,
                committed_offset=committed if committed is not None and committed >= 0 else None,
                lag=lag,
                status=status,
            )
        )

    return tuple(results)


def _report_status(
    partitions: tuple[PartitionOffset, ...],
) -> Literal["passed", "failed"]:
    """Fail only when offsets indicate missing or invalid progress."""
    failed_statuses = {
        "missing_commit",
        "offset_out_of_range",
        "committed_ahead",
    }
    return "failed" if any(item.status in failed_statuses for item in partitions) else "passed"


def _build_metrics(
    partitions: tuple[PartitionOffset, ...],
) -> dict[str, tuple[int, int]]:
    """Convert partition observations into BIGINT-compatible DB metrics."""
    metrics: dict[str, tuple[int, int]] = {
        "partitions_checked": (len(partitions), len(partitions)),
        "partitions_with_valid_commits": (
    sum(
        item.status != "empty_uncommitted"
        for item in partitions
    ),
    sum(item.status == "ok" for item in partitions),
),
        "total_consumer_lag": (0, sum(item.lag or 0 for item in partitions)),
        "partitions_missing_commits": (
            0,
            sum(item.status == "missing_commit" for item in partitions),
        ),
        "partitions_with_expired_offsets": (
            0,
            sum(item.status == "offset_out_of_range" for item in partitions),
        ),
        "partitions_with_ahead_offsets": (
            0,
            sum(item.status == "committed_ahead" for item in partitions),
        ),
    }

    for item in partitions:
        prefix = f"partition_{item.partition}"
        metrics[f"{prefix}_log_end_offset"] = (
            item.log_end_offset,
            item.log_end_offset,
        )
        metrics[f"{prefix}_low_watermark"] = (
            item.low_watermark,
            item.low_watermark,
        )
        metrics[f"{prefix}_committed_offset"] = (
            item.log_end_offset,
            item.committed_offset if item.committed_offset is not None else -1,
        )
        metrics[f"{prefix}_lag"] = (
            0,
            item.lag if item.lag is not None else -1,
        )

    return metrics


def persist_offset_report(
    database_url: str,
    report: KafkaOffsetReport,
) -> None:
    """Persist one Kafka offset report using the existing reconciliation tables."""
    started_at = datetime.now(UTC)
    manifest_uri = f"kafka://{report.topic}/{report.group_id}"

    metrics = _build_metrics(report.partitions)

    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO core.reconciliation_runs (
                    run_id,
                    run_type,
                    source_system,
                    started_at,
                    completed_at,
                    status,
                    input_manifest_uri,
                    notes
                )
                VALUES (%s, 'daily', 'kafka', %s, %s, %s, %s, %s)
                """,
                (
                    report.run_id,
                    started_at,
                    datetime.now(UTC),
                    report.status,
                    manifest_uri,
                    f"Kafka consumer group offset inspection: {report.group_id}",
                ),
            )

            cursor.executemany(
                """
                INSERT INTO core.reconciliation_results (
                    run_id,
                    metric_name,
                    expected_value,
                    actual_value
                )
                VALUES (%s, %s, %s, %s)
                """,
                [
                    (report.run_id, name, expected, actual)
                    for name, (expected, actual) in metrics.items()
                ],
            )


def reconcile_kafka_offsets(
    database_url: str,
    bootstrap_servers: str,
    group_id: str,
    topic: str,
    timeout: float = 10.0,
) -> KafkaOffsetReport:
    """Inspect a Kafka group and persist its partition offset report."""
    if not group_id.strip():
        raise ValueError("group_id must not be empty")
    if not topic.strip():
        raise ValueError("topic must not be empty")
    if timeout <= 0:
        raise ValueError("timeout must be positive")

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "client.id": "finstream-offset-inspector",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "allow.auto.create.topics": False,
        }
    )

    try:
        partitions = inspect_topic_offsets(
            consumer,
            topic,
            timeout,
        )
    finally:
        consumer.close()

    report = KafkaOffsetReport(
        run_id=uuid.uuid4(),
        group_id=group_id,
        topic=topic,
        status=_report_status(partitions),
        partitions=partitions,
    )

    persist_offset_report(database_url, report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect Kafka consumer offsets and persist reconciliation results."
    )
    parser.add_argument("--topic", required=True)
    parser.add_argument("--group-id", required=True)
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_EXTERNAL_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL"),
    )
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args()

    if not args.database_url:
        parser.error("DATABASE_URL or --database-url is required")

    try:
        report = reconcile_kafka_offsets(
            database_url=args.database_url,
            bootstrap_servers=args.bootstrap_servers,
            group_id=args.group_id,
            topic=args.topic,
            timeout=args.timeout,
        )
    except (ValueError, RuntimeError, KafkaException, psycopg.Error) as exc:
        LOGGER.error("Kafka offset reconciliation failed: %s", exc)
        sys.exit(1)

    output = {
        "run_id": str(report.run_id),
        "group_id": report.group_id,
        "topic": report.topic,
        "status": report.status,
        "total_lag": report.total_lag,
        "missing_commit_count": report.missing_commit_count,
        "partitions": [asdict(item) for item in report.partitions],
    }
    print(json.dumps(output, indent=2))

    if report.status == "failed":
        sys.exit(2)


if __name__ == "__main__":
    main()
