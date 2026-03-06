{{
    config(
        materialized = 'table',
        schema       = 'MARTS',
        tags         = ['marts', 'dimension']
    )
}}

{#
  Customer dimension — current state of each customer with latest RFM segment.
  SCD Type 2 history tracked via customer_snapshot.sql in snapshots/.
#}

-- TODO (Phase 4): implement dimension SQL
with customers as (
    select * from {{ ref('stg_customers') }}
),

segments as (
    select * from {{ ref('int_customer_segments') }}
),

final as (
    select
        {{ generate_surrogate_key(['c.customer_id']) }}   as customer_sk,
        c.customer_id,
        c.first_name,
        c.last_name,
        c.date_of_birth,
        c.age,
        c.gender,
        c.city,
        c.postal_code,
        c.is_domestic_only,
        c.customer_since,

        -- Current RFM segment (point-in-time; snapshot tracks history)
        s.rfm_segment,
        s.recency_days,
        s.frequency                                       as transaction_count,
        s.monetary_value                                  as total_spend_nok,
        s.last_transaction_date,

        current_timestamp()                               as _loaded_at
    from customers c
    left join segments s using (customer_id)
)

select * from final
