SELECT event_id, source_system, source_record_id, event_time, amount, currency, transaction_type
FROM core.transactions
WHERE event_id = :event_id;
