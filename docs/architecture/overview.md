# Architecture overview

FinStream ingests only replayed or synthetic traffic. A source adapter converts a
PaySim or IEEE-CIS record to a canonical, provenance-preserving transaction. The
stable identity is `BLAKE2b(source_system, source_record_id, simulation_id)`;
the same replay row produces the same `event_id`.

Kafka carries Confluent-framed Avro. A validator routes invalid records to a
rejected channel; repeated terminal failures go to the DLQ for review, not loss.
Spark uses event time, a 15-minute watermark, `event_id` deduplication, and a
durable checkpoint. Its first durable analytical sink is Parquet. PostgreSQL is
used for operational serving metadata and selected materialized records only.

Airflow coordinates bounded historical ingestion, dbt, reconciliation, quality,
and maintenance. It deliberately does not own a permanently running stream.
