# Airflow guide

The Airflow 3 DAGs are intentionally finite workflow boundaries: historical ingestion, reconciliation, dbt, quality, and maintenance. They return explicit scaffold messages until parameterized operators and manifests are implemented; they do not claim to run production jobs. Before enabling scheduling, initialize the Airflow metadata database, parse DAGs in the selected image, add least-privilege connections/secrets, and test retries, alerts, and idempotent task behavior.

