select
    alert_id,
    event_id,
    created_at,
    rule_name,
    risk_score,
    severity,
    explanation
from {{ source('core', 'fraud_alerts') }}
