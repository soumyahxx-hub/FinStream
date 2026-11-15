{{ config(materialized='table') }}

with transaction_metrics as (

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
        ) as historical_fraud_amount

    from {{ ref('fct_transactions') }}

    where origin_account_id is not null

    group by origin_account_id

),

alert_metrics as (

    select
        t.origin_account_id as customer_id,

        count(a.alert_id) as generated_alert_count,

        max(a.risk_score) as maximum_generated_alert_score

    from {{ ref('fct_transactions') }} as t

    inner join {{ ref('fct_fraud_alerts') }} as a
        on t.event_id = a.event_id

    where t.origin_account_id is not null

    group by t.origin_account_id

)

select
    t.*,

    coalesce(a.generated_alert_count, 0)
        as generated_alert_count,

    a.maximum_generated_alert_score

from transaction_metrics as t

left join alert_metrics as a
    using (customer_id)