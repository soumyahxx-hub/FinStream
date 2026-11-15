CREATE INDEX IF NOT EXISTS idx_transactions_event_time ON core.transactions (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_source_record ON core.transactions (source_system, source_record_id);
CREATE INDEX IF NOT EXISTS idx_transactions_origin_event_time ON core.transactions (origin_account_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_fraud_alerts_created_at ON core.fraud_alerts (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_fraud_alerts_severity_created ON core.fraud_alerts (severity, created_at DESC);
