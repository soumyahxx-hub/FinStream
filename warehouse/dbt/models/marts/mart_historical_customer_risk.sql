{{ config(materialized='table') }}

select
    origin_account_id as customer_id,

    count(*) as transaction_count,

    count(*) filter (
        where historical_fraud_label is true
    ) as historical_fraud_count,

    count(*) filter (
        where historical_fraud_label is false
    ) as legitimate_transaction_count,

    round(
        100.0 * count(*) filter (
            where historical_fraud_label is true
        ) / nullif(
            count(*) filter (
                where historical_fraud_label is not null
            ), 0
        ),
        4
    ) as historical_fraud_rate_percent,

    sum(amount) as total_transaction_amount,

    coalesce(
        sum(amount) filter (
            where historical_fraud_label is true
        ), 0
    ) as historical_fraud_amount,

    min(event_time) as first_transaction_at,
    max(event_time) as last_transaction_at

from {{ ref('fct_transactions') }}

where origin_account_id is not null

group by origin_account_id