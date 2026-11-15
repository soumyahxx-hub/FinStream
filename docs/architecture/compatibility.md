# Compatibility and verification notes

Selected pins are deliberately conservative and recorded in `pyproject.toml`
and Compose. The following upstream material was checked when creating this
foundation:

- [Spark 3.5.7 Kafka connector documentation](https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html) specifies matching `spark-sql-kafka-0-10_2.12:3.5.7`; the foundation also uses matching `spark-avro_2.12:3.5.7`.
- [Spark's Kafka write semantics](https://spark.apache.org/docs/3.5.6/structured-streaming-kafka-integration.html) warn that writes can be at least once and duplicate; the repository therefore does not claim exactly once.
- [Confluent Platform 7.8 Docker reference](https://docs.confluent.io/platform/7.8/installation/docker/config-reference.html) documents environment configuration conventions used for Kafka/Schema Registry.
- [confluent-kafka 2.8.2](https://pypi.org/project/confluent-kafka/2.8.2/) documents the `avro,schemaregistry` extras used for the producer client.
- [Airflow Docker image reference](https://airflow.apache.org/docs/docker-stack/index.html) documents versioned Python-specific images; Airflow is isolated in its own profile.
- [dbt-postgres 1.9.0 package release](https://pypi.org/project/dbt-postgres/1.9.0/) confirms the adapter pin; core and adapter are kept at the same version.

Not yet runtime-verified: full Confluent-framed Avro replay through Spark,
Airflow 3 DAG parse in the selected image, dbt connection and run, Terraform
provider initialization, and Docker image pulls on the target machine. Run
these in the documented order and record the results in `PROJECT_STATUS.md`.
