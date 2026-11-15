# Project status

**Current phase:** Phase 1 — project foundation

| Area | Status | Evidence |
|---|---|---|
| Phase 0 decisions | Approved | `docs/decisions/` |
| Canonical contracts and adapters | Implemented, unexecuted against raw data | `ingestion/` |
| Kafka/Schema Registry | Configured, not runtime-tested here | `docker-compose.yml`, `streaming/kafka/` |
| Spark stream | Implemented foundation, integration pending | `streaming/spark/` |
| PostgreSQL | DDL and idempotent repository implemented | `sql/`, `storage/postgres/` |
| dbt | Parse-ready foundation, database run pending | `warehouse/dbt/` |
| Airflow | Airflow 3 finite-workflow foundations | `orchestration/airflow/` |
| FastAPI | Read-only routes implemented, DB integration pending | `api/` |
| Monitoring | Configured, no metric results collected | `monitoring/` |
| Terraform | Safe opt-in foundation, no resources created | `infrastructure/terraform/` |

## Technical decisions

- Kafka KRaft, Avro, and Schema Registry; no ZooKeeper.
- Spark Structured Streaming is the core engine; Flink is optional later.
- Parquet is the initial lake format; evaluate Iceberg only with a written decision.
- PaySim and IEEE-CIS are separate sources with preserved provenance.
- Airflow runs finite work only, not a continuous streaming process.
- Reliability controls are mandatory; no false exactly-once claim.

## Pinned / selected compatibility set

Python app 3.11; Spark 3.5.7; Spark Kafka/Avro packages 3.5.7 (Scala 2.12); Confluent Platform 7.8.0; PostgreSQL 16.6; dbt Core/Postgres 1.9.0; Airflow 3.4.0 Python 3.11 image. See `docs/architecture/compatibility.md` for verification and remaining runtime checks.

## Validation snapshot

- All Compose profiles rendered statically with `.env.example`.
- Six JSON/Avro assets parsed successfully.
- Dynamic Python, dbt, Terraform, Docker, Airflow, Spark, and integration checks remain pending; see `VALIDATION_REPORT.md`.

## Known constraints

- This workspace has no usable Python interpreter or Terraform binary, so Python/TF execution was not performed here.
- Docker Desktop's Linux engine is unavailable, so image pulls and full multi-service runtime could not be performed here.
- Raw data has not been downloaded, profiled, or included.

## Next action

Follow `docs/deployment/local-setup.md`, validate the core profile, and record the first bounded replay's manifest and reconciliation result before enabling downstream automation.
