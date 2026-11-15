# Airflow configuration

Use Airflow 3.4 in its dedicated compose profile. It handles finite backfills,
dbt, reconciliation, data-quality checks, and maintenance—not continuous Spark
stream supervision. Start only after PostgreSQL is healthy and initialise the
metadata database with the documented command.
