# Integration tests

Add Docker-backed tests only after profile startup is measured locally. They must
exercise schema registration, replay, restart, checkpoint recovery, and actual
PostgreSQL uniqueness—not mocked end-to-end success.
