# Kafka topics

`transactions.raw` is pre-validation canonical input; `transactions.validated`
is valid input to streaming; `transactions.rejected` captures contract failures;
`transactions.enriched` is downstream material; `fraud.alerts` holds generated
alerts; and `transactions.dead-letter` holds terminal processing failures.

All use stable keys. The local profile has one broker and replication factor one,
which is a learning environment—not production replication. See `topics.yaml`
for partition, retention, consumer-group, and semantics details.

The fraud consumer reads only `transactions.validated` using
`finstream-fraud-detection-v1` by default. It writes an idempotent transaction
and optional alert to PostgreSQL before publishing an alert to `fraud.alerts`.
It commits the input offset only after those downstream operations succeed. A
crash between the database commit and Kafka delivery can replay the record, so
the flow is at-least-once rather than exactly-once. After bounded processing
failures, it writes a replay-disabled envelope to `transactions.dead-letter`
before committing that input offset; investigate and authorize replay manually.
