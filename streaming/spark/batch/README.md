# Batch workloads

Historical backfills run as finite, parameterized Spark jobs with an explicit input manifest, bounded date range, checkpoint/output path, and reconciliation record. Airflow may schedule such jobs; it does not supervise the continuous transaction stream.

