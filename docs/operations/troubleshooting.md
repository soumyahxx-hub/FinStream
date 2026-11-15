# Troubleshooting

| Symptom | First checks | Safe response |
|---|---|---|
| Kafka will not start | Compose logs, KRaft listeners, volume ownership | Stop and inspect; do not delete data before recording state. |
| Schema Registry unavailable | Broker health and Registry bootstrap address | Restore broker first; verify schema subjects before replay. |
| Spark fails after restart | Checkpoint path, package resolution, source offsets | Preserve checkpoint; investigate before reset. |
| Duplicate rows | Stable IDs, SQL conflict count, replay scope | Reconcile and fix sink behavior; retain audit evidence. |
| API returns 503 | DATABASE_URL, PostgreSQL health, schema initialization | Repair configuration; no fake fallback payload is returned. |
| Laptop pressure | Docker resource view, active profiles | Stop nonessential profiles and reduce test rate. |

