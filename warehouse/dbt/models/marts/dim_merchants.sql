{{ config(materialized='table') }}

select
    merchant_id,
    min(event_time) as first_seen_at,
    max(event_time) as last_seen_at,
    count(*) as transaction_count
from {{ ref('stg_transactions') }}
where merchant_id is not null
group by 1
