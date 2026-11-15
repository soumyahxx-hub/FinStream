.DEFAULT_GOAL := help

.PHONY: help install test lint typecheck compose-core compose-streaming compose-monitoring validate spark-run dbt-parse

help: ## Print available commands
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "%-20s %s\n", $$1, $$2}'

install: ## Install application and development dependencies
	pip install -e '.[dev]'

test: ## Run unit tests
	pytest

lint: ## Run static lint checks
	ruff check .

typecheck: ## Run type checks for application packages
	mypy api ingestion fraud_detection storage

compose-core: ## Start PostgreSQL only
	docker compose --env-file .env --profile core up -d postgres

compose-streaming: ## Start Kafka, Schema Registry, and topic initializer
	docker compose --env-file .env --profile streaming up -d kafka schema-registry init-topics

compose-monitoring: ## Start metrics profile; API must be started separately
	docker compose --env-file .env --profile monitoring up -d prometheus grafana

validate: ## Run offline repository checks
	python scripts/validation/validate_project.py

spark-run: ## Submit the transaction stream job in one local Spark container
	docker compose --env-file .env --profile spark run --rm spark

dbt-parse: ## Parse dbt project against configured profile
	dbt parse --project-dir warehouse/dbt --profiles-dir warehouse/dbt
