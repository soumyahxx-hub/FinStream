# Docker profiles

The Compose file has named volumes for database, Kafka, lake, checkpoints, and metrics. Resource limits are local planning controls; measure actual behavior with the target Docker Desktop version.

The streaming profile starts Kafka, Schema Registry, and a one-shot topic initializer. The Spark profile uses `local[1]`, two shuffle partitions, a 1 GiB driver setting, and a 2 GiB container cap. Do not delete the spark checkpoint volume casually: it is recovery state.

