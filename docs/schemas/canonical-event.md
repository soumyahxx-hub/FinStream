# Canonical event contract

`TransactionEvent` v1 carries stable `event_id`, contract version, source system,
source record ID, optional simulation ID, UTC event/ingestion times, amount and
currency, optional business identifiers, historical fraud label, and JSON source
attributes. The contract deliberately keeps source-specific attributes rather
than pretending every public dataset has the same domain semantics.

Schema files are versioned under `streaming/kafka/schemas`. Backward-compatible
additions need defaults. Breaking changes need a new version, migration plan,
compatibility test, and ADR.
