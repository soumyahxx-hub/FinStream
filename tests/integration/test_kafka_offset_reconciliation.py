
from uuid import uuid4

import psycopg

from ingestion.consumers.kafka_offset_reconciliation import (
    KafkaOffsetReport,
    PartitionOffset,
    persist_offset_report,
)


def test_persists_kafka_offset_reconciliation_report(
    test_database_url: str,
) -> None:
    run_id = uuid4()

    report = KafkaOffsetReport(
        run_id=run_id,
        group_id="integration-test-offset-group",
        topic="transactions.validated",
        status="passed",
        partitions=(
            PartitionOffset(
                topic="transactions.validated",
                partition=0,
                low_watermark=0,
                log_end_offset=100,
                committed_offset=100,
                lag=0,
                status="ok",
            ),
            PartitionOffset(
                topic="transactions.validated",
                partition=1,
                low_watermark=0,
                log_end_offset=50,
                committed_offset=45,
                lag=5,
                status="ok",
            ),
        ),
    )

    try:
        persist_offset_report(test_database_url, report)

        with psycopg.connect(test_database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT run_type, source_system, status,
                           input_manifest_uri
                    FROM core.reconciliation_runs
                    WHERE run_id = %s
                    """,
                    (run_id,),
                )
                run = cursor.fetchone()

                assert run is not None
                assert run[0] == "daily"
                assert run[1] == "kafka"
                assert run[2] == "passed"
                assert "transactions.validated" in run[3]

                cursor.execute(
                    """
                    SELECT metric_name, expected_value, actual_value
                    FROM core.reconciliation_results
                    WHERE run_id = %s
                    """,
                    (run_id,),
                )
                metrics = {
                    name: (expected, actual)
                    for name, expected, actual in cursor.fetchall()
                }

                assert metrics["partitions_checked"] == (2, 2)
                assert metrics["total_consumer_lag"] == (0, 5)
                assert metrics["partitions_missing_commits"] == (0, 0)
                assert metrics["partition_0_lag"] == (0, 0)
                assert metrics["partition_1_lag"] == (0, 5)

    finally:
        with psycopg.connect(test_database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM core.reconciliation_results
                    WHERE run_id = %s
                    """,
                    (run_id,),
                )
                cursor.execute(
                    """
                    DELETE FROM core.reconciliation_runs
                    WHERE run_id = %s
                    """,
                    (run_id,),
                )
