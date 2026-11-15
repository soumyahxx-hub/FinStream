{{ config(materialized='table') }}

select
    source_system,
    transaction_type,

    count(*) as total_transactions,

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

    sum(amount) as total_amount,

    coalesce(
        sum(amount) filter (
            where historical_fraud_label is true
        ), 0
    ) as historical_fraud_amount,

    avg(amount) as average_transaction_amount,

    max(amount) as maximum_transaction_amount

from {{ ref('fct_transactions') }}

group by
    source_system,
    transaction_type