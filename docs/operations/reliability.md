# Reliability model

| Term | FinStream meaning |
|---|---|
| At-most-once | A record may be lost; not acceptable for the primary path. |
| At-least-once | A record may be redelivered after retry/failure; Kafka/Spark paths can behave this way. |
| Exactly-once | A precise guarantee over defined boundaries; not claimed end-to-end here. |
| Effectively-once | Duplicates may travel, but a stable key and idempotent sink make final effect once for a defined sink. |

Controls are deterministic IDs, Kafka idempotent producer settings, bounded retry, rejected/DLQ channels, Spark checkpointing, watermark/deduplication, SQL unique constraints, input manifests, and reconciliation. These are controls to test, not a substitute for operational evidence.

