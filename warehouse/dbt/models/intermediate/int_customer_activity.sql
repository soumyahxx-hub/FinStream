select
    origin_account_id as customer_id,
    event_date,
    count(*) as transaction_count,
    sum(amount) as total_amount
from {{ ref('int_transaction_enrichment') }}
where origin_account_id is not null
group by 1, 2
