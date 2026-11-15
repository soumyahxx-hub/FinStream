# Technology selection

Kafka KRaft removes ZooKeeper from the local footprint. Schema Registry and Avro
make contract evolution explicit. Spark Structured Streaming supplies a mature
micro-batch model, watermarking, and checkpoint support. S3 plus Parquet remains
the first lake design because it is understandable and cheap; Iceberg is a later
decision after update/delete and catalog requirements are known.

PostgreSQL is the operational serving store, dbt transforms warehouse tables,
and Airflow manages finite operations. FastAPI provides a small presentation
layer. Prometheus/Grafana and CloudWatch planning cover observability. Flink is
not installed; it is an optional comparison only.
