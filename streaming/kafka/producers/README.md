# Kafka producers

The application producer in ingestion/producers uses an Avro serializer, all acknowledgements, idempotence, five bounded retries, and stable event keys. Add business-specific producers only when they use the canonical contract and their delivery/retry/DLQ behavior is tested.

