from types import SimpleNamespace

from ingestion.consumers.kafka_offset_reconciliation import (
    _report_status,
    inspect_topic_offsets,
)


class FakeConsumer:
    def __init__(
        self,
        partitions: dict[int, tuple[int, int]],
        committed: dict[int, int],
    ) -> None:
        self.partitions = partitions
        self.committed_offsets = committed

    def list_topics(self, topic: str, timeout: float):
        return SimpleNamespace(
            topics={
                topic: SimpleNamespace(
                    error=None,
                    partitions={partition: object() for partition in self.partitions},
                )
            }
        )

    def committed(self, partitions, timeout: float):
        return [
            SimpleNamespace(
                partition=partition.partition,
                offset=self.committed_offsets.get(partition.partition, -1),
            )
            for partition in partitions
        ]

    def get_watermark_offsets(
        self,
        partition,
        timeout: float,
        cached: bool,
    ):
        return self.partitions[partition.partition]


def test_inspects_partition_lag() -> None:
    consumer = FakeConsumer(
        partitions={0: (0, 100)},
        committed={0: 75},
    )

    result = inspect_topic_offsets(consumer, "transactions.raw")

    assert len(result) == 1
    assert result[0].partition == 0
    assert result[0].committed_offset == 75
    assert result[0].log_end_offset == 100
    assert result[0].lag == 25
    assert result[0].status == "ok"


def test_detects_missing_commit_with_available_records() -> None:
    consumer = FakeConsumer(
        partitions={0: (0, 100)},
        committed={},
    )

    result = inspect_topic_offsets(consumer, "transactions.raw")

    assert result[0].committed_offset is None
    assert result[0].lag is None
    assert result[0].status == "missing_commit"
    assert _report_status(result) == "failed"


def test_empty_partition_without_commit_is_not_failed() -> None:
    consumer = FakeConsumer(
        partitions={0: (0, 0)},
        committed={},
    )

    result = inspect_topic_offsets(consumer, "transactions.raw")

    assert result[0].status == "empty_uncommitted"
    assert result[0].lag == 0
    assert _report_status(result) == "passed"


def test_detects_committed_offset_outside_retention_window() -> None:
    consumer = FakeConsumer(
        partitions={0: (50, 100)},
        committed={0: 20},
    )

    result = inspect_topic_offsets(consumer, "transactions.raw")

    assert result[0].status == "offset_out_of_range"
    assert result[0].lag == 50
    assert _report_status(result) == "failed"


def test_detects_committed_offset_ahead_of_log_end() -> None:
    consumer = FakeConsumer(
        partitions={0: (0, 100)},
        committed={0: 120},
    )

    result = inspect_topic_offsets(consumer, "transactions.raw")

    assert result[0].status == "committed_ahead"
    assert result[0].lag is None
    assert _report_status(result) == "failed"


def test_inspects_multiple_partitions_independently() -> None:
    consumer = FakeConsumer(
        partitions={
            0: (0, 100),
            1: (0, 50),
            2: (0, 0),
        },
        committed={
            0: 90,
            1: 40,
        },
    )

    result = inspect_topic_offsets(consumer, "transactions.validated")

    assert [item.partition for item in result] == [0, 1, 2]
    assert [item.lag for item in result] == [10, 10, 0]
    assert [item.status for item in result] == [
        "ok",
        "ok",
        "empty_uncommitted",
    ]
    assert _report_status(result) == "passed"
