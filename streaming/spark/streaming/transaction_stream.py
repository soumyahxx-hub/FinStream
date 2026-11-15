"""Structured Streaming construction for the canonical transaction topic.

This job provides checkpointing, watermark-based deduplication, and an
idempotent-file-sink-friendly event projection. Kafka and Spark provide
at-least-once paths; there is no end-to-end exactly-once claim.
"""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr


def read_registered_avro(stream: DataFrame, avro_schema: str) -> DataFrame:
    """Decode Confluent-framed Avro with an explicitly selected compatible schema.

    Confluent values begin with one magic byte and four schema-ID bytes. Registry
    lookup/evolution is intentionally a control-plane concern, not a hidden UDF.
    """
    payload = stream.select(
        col("key").cast("string").alias("kafka_key"),
        expr("substring(value, 6, length(value) - 5)").alias("avro_payload"),
        col("timestamp").alias("kafka_timestamp"),
    )
    return payload.select(
        "kafka_key", "kafka_timestamp", from_avro("avro_payload", avro_schema).alias("event")
    ).select("kafka_key", "kafka_timestamp", "event.*")


def build_transaction_stream(spark: SparkSession, bootstrap_servers: str, schema_path: Path) -> DataFrame:
    """Build, but do not start, a replayable validated transaction stream."""
    avro_schema = json.loads(schema_path.read_text(encoding="utf-8"))
    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", "transactions.validated")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", "5000")
        .load()
    )
    decoded = read_registered_avro(source, json.dumps(avro_schema))
    return decoded.withWatermark("event_time", "15 minutes").dropDuplicates(["event_id"])


def start_parquet_sink(events: DataFrame, checkpoint_root: str, lake_uri: str):
    """Start an append-only Parquet sink with a durable checkpoint location."""
    return (
        events.writeStream.format("parquet")
        .option("path", f"{lake_uri.rstrip('/')}/transactions")
        .option("checkpointLocation", f"{checkpoint_root.rstrip('/')}/transactions-v1")
        .partitionBy("source_system")
        .outputMode("append")
        .start()
    )
