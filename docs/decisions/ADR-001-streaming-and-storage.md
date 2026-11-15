# ADR 001: Kafka KRaft, Spark, and Parquet

**Status:** Accepted. Kafka KRaft with Schema Registry/Avro is the event layer;
Spark Structured Streaming is the primary processor; Parquet is the initial lake
format. This fits local-first learning and avoids ZooKeeper, Flink, Delta, and
Iceberg as mandatory dependencies. Revisit Iceberg only after a documented need
for table-level mutation, time travel, or catalog-managed concurrency.
