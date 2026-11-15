{{ config(materialized='table') }}

select
    origin_account_id as customer_id,
    min(event_time) as first_seen_at,
    max(event_time) as last_seen_at,
    count(*) as lifetime_transaction_count
from {{ ref('stg_transactions') }}
where origin_account_id is not null
group by 1
