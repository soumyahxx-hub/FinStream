select
    t.source_system,
    date_trunc('day', a.created_at) as alert_date,
    count(*) as alert_count,
    count(*) filter (where t.historical_fraud_label is true) as alerts_with_historical_label,
    avg(a.risk_score) as average_risk_score
from {{ ref('fct_fraud_alerts') }} as a
join {{ ref('fct_transactions') }} as t using (event_id)
group by 1, 2
