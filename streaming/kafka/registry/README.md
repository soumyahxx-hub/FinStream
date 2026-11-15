# Schema Registry policy

Schemas live in `../schemas` and are registered under versioned subjects such as `transactions.raw-value`.
Producers use Confluent wire format; Spark must remove the five-byte wire prefix before `from_avro`, or use a Schema Registry-aware decoder.
The included Spark job documents this boundary instead of pretending a plain Avro reader understands Schema Registry framing.

Set subject compatibility to `BACKWARD` for evolving value schemas. Additive fields need defaults; field removals and type changes require a compatibility review, a migration plan, and an ADR.
Schema registration and compatibility checks are an operator step before deploying a producer.
