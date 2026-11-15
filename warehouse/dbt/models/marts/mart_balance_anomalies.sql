{{ config(materialized='table') }}

with parsed as (

    select
        event_id,
        source_system,
        source_record_id,
        simulation_id,
        event_time,
        origin_account_id,
        transaction_type,
        amount,
        historical_fraud_label,

        nullif(source_attributes ->> 'oldbalanceOrg', '')::numeric
            as old_origin_balance,

        nullif(source_attributes ->> 'newbalanceOrig', '')::numeric
            as new_origin_balance,

        nullif(source_attributes ->> 'oldbalanceDest', '')::numeric
            as old_destination_balance,

        nullif(source_attributes ->> 'newbalanceDest', '')::numeric
            as new_destination_balance,

        coalesce(
            source_attributes ->> 'isFlaggedFraud' in ('1', 'true'),
            false
        ) as original_dataset_flag

    from {{ ref('stg_transactions') }}

    where source_system = 'paysim'

),

calculated as (

    select
        *,

        case
            when transaction_type in ('TRANSFER', 'CASH_OUT', 'DEBIT')
                then old_origin_balance - new_origin_balance
            when transaction_type = 'CASH_IN'
                then new_origin_balance - old_origin_balance
            else null
        end as observed_origin_balance_movement

    from parsed

)

select
    *,

    case
        when observed_origin_balance_movement is null then null
        else observed_origin_balance_movement - amount
    end as origin_balance_movement_difference,

    case
        when observed_origin_balance_movement is null then false
        when abs(observed_origin_balance_movement - amount) > 0.01
            then true
        else false
    end as origin_balance_mismatch,

    case
        when historical_fraud_label is true then 'historically_fraud'
        when historical_fraud_label is false then 'historically_legitimate'
        else 'unlabelled'
    end as historical_outcome

from calculated