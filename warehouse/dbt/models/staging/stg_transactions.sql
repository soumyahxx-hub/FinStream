with source as (

    select *
    from {{ source('staging', 'transactions') }}

)

select
    event_id,
    event_version,
    source_system,
    source_record_id,
    simulation_id,
    event_time,
    ingested_at,
    amount,
    currency,
    transaction_type,
    origin_account_id,
    destination_account_id,
    merchant_id,
    historical_fraud_label,
    source_attributes

from source