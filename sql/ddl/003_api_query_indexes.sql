-- FinStream API query-performance indexes.
-- These indexes support pagination, time filtering, customer lookups,
-- and operational status queries.

-- Transaction keyset pagination and bounded time-range analytics.
CREATE INDEX IF NOT EXISTS idx_transactions_event_time_event_id
    ON core.transactions (event_time DESC, event_id DESC);

-- Customer account lookup.
CREATE INDEX IF NOT EXISTS idx_transactions_origin_account
    ON core.transactions (origin_account_id)
    WHERE origin_account_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_transactions_destination_account
    ON core.transactions (destination_account_id)
    WHERE destination_account_id IS NOT NULL;

-- Fraud alert keyset pagination and time-range filtering.
CREATE INDEX IF NOT EXISTS idx_fraud_alerts_created_at_alert_id
    ON core.fraud_alerts (created_at DESC, alert_id DESC);

-- Latest processor status per topic partition.
CREATE INDEX IF NOT EXISTS idx_processing_metadata_latest_status
    ON core.processing_metadata (
        processor_name,
        source_topic,
        partition_id,
        processed_at DESC
    );

-- Recent reconciliation history.
CREATE INDEX IF NOT EXISTS idx_reconciliation_runs_started_at
    ON core.reconciliation_runs (started_at DESC);
