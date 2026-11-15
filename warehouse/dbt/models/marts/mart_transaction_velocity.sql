select
    origin_account_id as customer_id,
    date_trunc('hour', event_time) as event_hour,
    count(*) as transactions_per_hour,
    sum(amount) as amount_per_hour
from {{ ref('fct_transactions') }}
where origin_account_id is not null
group by 1, 2
