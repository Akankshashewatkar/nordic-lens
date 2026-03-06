{{
    config(
        materialized = 'ephemeral',
        tags         = ['intermediate', 'transactions']
    )
}}

{#
  Intermediate — enriches staged transactions with customer & merchant context.

  Derived fields added:
    - is_round_amount         : amount divisible by 1000
    - is_late_night           : hour_of_day between 0 and 5
    - rolling_7d_avg_amount   : 7-day rolling average per customer (window fn)
    - days_since_last_txn     : days elapsed since customer's prior transaction
#}

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

enriched as (
    select
        -- Transaction core fields
        t.transaction_id,
        t.customer_id,
        t.merchant_id,
        t.merchant_name,
        t.mcc_code,
        t.mcc_category,
        t.amount_nok,
        t.currency,
        t.transaction_type,
        t.transaction_date,
        t.is_weekend,
        t.hour_of_day,
        t.is_flagged,
        t.country_code,
        t.channel,

        -- Customer demographics
        c.age,
        c.gender,
        c.city,
        c.is_domestic_only,
        c.customer_since,

        -- Derived flags
        (t.amount_nok % 1000 = 0)::boolean           as is_round_amount,
        (t.hour_of_day between 0 and 4)::boolean      as is_late_night,

        -- 7-day rolling average spend per customer (RANGE requires TIMESTAMP_NTZ ordering)
        avg(t.amount_nok) over (
            partition by t.customer_id
            order by t.transaction_date
            range between interval '6 days' preceding and current row
        )                                              as rolling_7d_avg_amount,

        -- Days since the same customer's previous transaction (NULL for first txn)
        datediff(
            'day',
            lag(t.transaction_date) over (
                partition by t.customer_id
                order by t.transaction_date
            ),
            t.transaction_date
        )                                              as days_since_last_txn

    from transactions t
    left join customers c using (customer_id)
)

select * from enriched
