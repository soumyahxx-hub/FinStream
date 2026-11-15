# Kafka consumers

Consumers must disable auto-commit when they coordinate a durable sink, record source offset ranges, tolerate replay, and route terminal failures to the DLQ. Consumer groups are documented per topic in topics.yaml.

