{{ config(materialized='table') }}

select distinct
    event_date as date_day,
    extract(year from event_date)::int as year_number,
    extract(month from event_date)::int as month_number,
    extract(day from event_date)::int as day_number
from {{ ref('int_transaction_enrichment') }}
