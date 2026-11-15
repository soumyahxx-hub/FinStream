# Testing strategy

Unit tests cover deterministic identities, validation, reconciliation arithmetic, and schema assets. Integration tests must start actual isolated services and exercise schema registration, producer delivery, offsets, and PostgreSQL uniqueness. Streaming tests must inject duplicates and late records, restart from checkpoint, and verify recovery. End-to-end tests use a small permitted fixture, a manifest, and reconciliation, never raw public data committed to Git.

