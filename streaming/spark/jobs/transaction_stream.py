"""Executable entry point for local single-driver transaction processing."""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from pyspark.sql import SparkSession

from streaming.spark.streaming.transaction_stream import build_transaction_stream, start_parquet_sink


def main() -> None:
    root = Path(__file__).resolve().parents[3]
    spark = (
        SparkSession.builder.appName("finstream-transaction-stream")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    events = build_transaction_stream(
        spark,
        os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        root / "streaming/kafka/schemas/transaction.v1.avsc",
    )
    query = start_parquet_sink(
        events,
        os.environ.get("SPARK_CHECKPOINT_ROOT", "/opt/finstream/checkpoints"),
        os.environ.get("LAKE_URI", "file:///opt/finstream/data/lake"),
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
