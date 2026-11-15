# ADR 003: Explicit effective-once model

**Status:** Accepted. Stable identity, idempotent producer settings, Spark
checkpointing/watermarks, conflict-safe SQL writes, DLQ, bounded retry, replay,
and reconciliation are mandatory. The platform claims at-least-once transport
and strives for effectively-once sink outcomes where a unique key is verified.
It never claims end-to-end exactly-once based on checkpointing alone.
