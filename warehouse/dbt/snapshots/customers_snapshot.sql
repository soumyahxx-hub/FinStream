{% snapshot customers_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='check',
      check_cols=['last_seen_at', 'lifetime_transaction_count']
    )
}}

select * from {{ ref('dim_customers') }}

{% endsnapshot %}
