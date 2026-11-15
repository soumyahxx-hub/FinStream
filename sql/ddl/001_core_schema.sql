CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.transactions (
    event_id CHAR(40) PRIMARY KEY,
    event_version SMALLINT NOT NULL CHECK (event_version > 0),
    source_system TEXT NOT NULL CHECK (source_system IN ('paysim', 'ieee_cis', 'synthetic')),
    source_record_id TEXT NOT NULL,
    simulation_id TEXT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    amount NUMERIC(18, 2) NOT NULL CHECK (amount >= 0),
    currency CHAR(3) NOT NULL,
    transaction_type TEXT NULL,
    origin_account_id TEXT NULL,
    destination_account_id TEXT NULL,
    merchant_id TEXT NULL,
    historical_fraud_label BOOLEAN NULL,
    source_attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE NULLS NOT DISTINCT (source_system, source_record_id, simulation_id)
);

CREATE TABLE IF NOT EXISTS core.fraud_alerts (
    alert_id CHAR(40) PRIMARY KEY,
    event_id CHAR(40) NOT NULL REFERENCES core.transactions(event_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    rule_name TEXT NOT NULL,
    risk_score NUMERIC(5, 4) NOT NULL CHECK (risk_score BETWEEN 0 AND 1),
    severity TEXT NOT NULL CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    explanation TEXT NOT NULL,
    UNIQUE (event_id, rule_name)
);

CREATE TABLE IF NOT EXISTS core.processing_metadata (
    processor_name TEXT NOT NULL,
    checkpoint_id TEXT NOT NULL,
    source_topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    offset_start BIGINT NOT NULL,
    offset_end BIGINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    status TEXT NOT NULL CHECK (status IN ('started', 'completed', 'failed')),
    PRIMARY KEY (processor_name, checkpoint_id, source_topic, partition_id)
);

CREATE TABLE IF NOT EXISTS core.reconciliation_runs (
    run_id UUID PRIMARY KEY,
    run_type TEXT NOT NULL CHECK (run_type IN ('replay', 'backfill', 'daily')),
    source_system TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'passed', 'failed')),
    input_manifest_uri TEXT NOT NULL,
    notes TEXT NULL
);

CREATE TABLE IF NOT EXISTS core.reconciliation_results (
    run_id UUID NOT NULL REFERENCES core.reconciliation_runs(run_id),
    metric_name TEXT NOT NULL,
    expected_value BIGINT NOT NULL,
    actual_value BIGINT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, metric_name)
);
