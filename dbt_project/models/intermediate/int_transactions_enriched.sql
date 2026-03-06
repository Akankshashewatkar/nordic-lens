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

-- TODO (Phase 4): implement enrichment SQL
with transactions as (
    select * from {{ ref('stg_transactions') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

enriched as (
    select
        t.*,
        c.city,
        c.age,
        c.is_domestic_only,
        c.customer_since,

        -- Derived flags
        (t.amount_nok % 1000 = 0)::boolean                               as is_round_amount,
        (t.hour_of_day between 0 and 4)::boolean                         as is_late_night,

        -- Rolling 7-day average amount per customer
        avg(t.amount_nok) over (
            partition by t.customer_id
            order by t.transaction_date
            range between interval '6 days' preceding and current row
        )                                                                  as rolling_7d_avg_amount,

        -- Days since last transaction
        datediff(
            'day',
            lag(t.transaction_date) over (
                partition by t.customer_id
                order by t.transaction_date
            ),
            t.transaction_date
        )                                                                  as days_since_last_txn

    from transactions t
    left join customers c using (customer_id)
)

select * from enriched
