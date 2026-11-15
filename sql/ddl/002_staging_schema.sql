CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.transactions (
    event_id CHAR(40) PRIMARY KEY,
    event_version SMALLINT NOT NULL CHECK (event_version > 0),
    source_system TEXT NOT NULL
        CHECK (source_system IN ('paysim', 'ieee_cis', 'synthetic')),
    source_record_id TEXT NOT NULL,
    simulation_id TEXT,
    event_time TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    amount NUMERIC(18, 2) NOT NULL CHECK (amount >= 0),
    currency CHAR(3) NOT NULL,
    transaction_type TEXT,
    origin_account_id TEXT,
    destination_account_id TEXT,
    merchant_id TEXT,
    historical_fraud_label BOOLEAN,
    source_attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
    kafka_key TEXT,
    kafka_timestamp TIMESTAMPTZ,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_staging_source_identity
        UNIQUE NULLS NOT DISTINCT (
            source_system,
            source_record_id,
            simulation_id
        )
);

CREATE INDEX IF NOT EXISTS idx_staging_transactions_event_time
    ON staging.transactions (event_time DESC);

CREATE INDEX IF NOT EXISTS idx_staging_transactions_source
    ON staging.transactions (source_system, source_record_id);

CREATE INDEX IF NOT EXISTS idx_staging_transactions_simulation
    ON staging.transactions (simulation_id);
