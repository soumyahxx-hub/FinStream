# Lake writers

The Spark job writes append-only Parquet partitions with checkpoints. Compaction,
catalog registration, Iceberg migration, and S3 lifecycle policies are deferred
until volume and query requirements are measured.
