# Failure recovery runbook

1. Stop the affected replay or streaming deployment without deleting its checkpoint.
2. Preserve logs, checkpoint reference, source manifest, topic/partition offsets, DLQ IDs, and environment version.
3. Classify the failure: schema, source data, broker, sink, code, or infrastructure.
4. Fix and test the cause with a bounded fixture; register compatible schema changes before reprocessing.
5. Choose replay scope from saved offsets or a manifest; do not rely on an unrecorded earliest replay.
6. Re-run with the same stable identity and idempotent sink path.
7. Reconcile input, accepted, rejected, DLQ, lake, and database unique-event counts.
8. Mark the run passed only if differences are explained; otherwise retain evidence and escalate.

For lost offsets or a removed checkpoint, create a documented new replay run. Do not silently reset offsets and call the result complete.

