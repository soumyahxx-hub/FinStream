# Operational model boundary

PostgreSQL is an indexed serving and metadata store, not a mirror of every raw
high-volume Kafka event. The canonical DDL owns uniqueness and reconciliation.
