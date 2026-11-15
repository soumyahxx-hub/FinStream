select
    t.*,
    date_trunc('day', t.event_time) as event_date,
    case
        when t.amount >= {{ var('high_value_amount', 100000) }} then 'high'
        when t.amount >= {{ var('medium_value_amount', 10000) }} then 'medium'
        else 'standard'
    end as amount_band
from {{ ref('stg_transactions') }} as t
