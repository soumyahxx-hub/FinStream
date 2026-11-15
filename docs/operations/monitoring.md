# Monitoring guide

Prometheus scrapes API request count and duration. Grafana provisions an overview dashboard without fabricated values. Add Kafka consumer lag, Spark batch duration, end-to-end latency, duplicate count, DLQ depth, PostgreSQL write latency, storage use, and quality failure metrics only after selecting supported exporters and measuring their memory cost.

Every alert needs an owner, severity, runbook, and tested notification path before it is considered operational.

