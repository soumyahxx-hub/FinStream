{{ config(materialized='incremental', unique_key='alert_id') }}

select * from {{ ref('stg_fraud_events') }}
{% if is_incremental() %}
where created_at > (select coalesce(max(created_at), '1900-01-01'::timestamptz) from {{ this }})
{% endif %}
