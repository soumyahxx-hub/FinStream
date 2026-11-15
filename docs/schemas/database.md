# Database schema

`core.transactions` is keyed by `event_id`; `core.fraud_alerts` is keyed by
`alert_id` and references it. Processing metadata tracks offset ranges and
reconciliation tables record expected/actual values for bounded runs. Indexes
support time, source identity, account lookup, and alert list reads. PostgreSQL
is not the unbounded raw-event archive.
