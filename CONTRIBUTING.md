# Contributing

Use Python 3.11 and work from the WSL2 filesystem. Copy `.env.example` to `.env`; never commit it.

Before a change, run `make lint`, `make test`, and `make validate`. A pull request must state whether a component is **implemented**, **scaffolded**, or **unverified**. Keep PaySim and IEEE-CIS workflows separate, preserve provenance, and do not add raw datasets.

Changes to event schemas require a compatibility review, a versioned Avro file, contract tests, and an ADR when they alter operational semantics.
