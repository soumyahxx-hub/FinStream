{{ config(materialized='incremental', unique_key='event_id', on_schema_change='append_new_columns') }}

select * from {{ ref('int_transaction_enrichment') }}
{% if is_incremental() %}
where ingested_at > (select coalesce(max(ingested_at), '1900-01-01'::timestamptz) from {{ this }})
{% endif %}
