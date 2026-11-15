# Detailed system architecture

```mermaid
sequenceDiagram
  participant R as Replay runner
  participant K as Kafka / Registry
  participant S as Spark stream
  participant L as Parquet lake
  participant P as PostgreSQL
  participant A as API
  R->>K: Avro event keyed by stable event_id
  K->>S: at-least-once source offsets
  S->>S: watermark + deduplicate + checkpoint
  S->>L: append Parquet output
  S->>P: idempotent upsert (future verified sink)
  A->>P: parameterized read query
```

The SQL primary key and producer idempotence reduce duplicate effects but do not
turn multiple independent systems into one global transaction. Reconciliation
compares input manifests, Kafka offset ranges, lake records, and operational
store unique IDs after bounded runs.
