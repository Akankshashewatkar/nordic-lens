{{
    config(
        materialized = 'ephemeral',
        tags         = ['intermediate', 'anomaly']
    )
}}

{#
  Intermediate — rule-based anomaly detection on enriched transactions.

  Flag types:
    1. statistical_outlier     : amount > (customer_avg + 3 * customer_stddev)
    2. velocity_spike          : 3+ transactions within 10 minutes for same customer
    3. large_round_amount      : amount % 1000 = 0 AND amount > 50,000 NOK
    4. suspicious_late_night   : hour 00–04 AND amount > 5,000 NOK
    5. unusual_geography       : foreign txn for a domestic-only customer
#}

with enriched as (
    select * from {{ ref('int_transactions_enriched') }}
),

customer_stats as (
    select
        customer_id,
        avg(amount_nok)    as avg_amount,
        stddev(amount_nok) as stddev_amount
    from enriched
    group by customer_id
),

velocity as (
    select
        transaction_id,
        count(*) over (
            partition by customer_id
            order by transaction_date
            range between interval '{{ var("velocity_window_minutes") }} minutes' preceding
                      and current row
        ) as txn_count_in_window
    from enriched
),

flagged as (
    select
        e.transaction_id,
        e.customer_id,
        e.amount_nok,
        e.transaction_date,

        -- Flag: statistical outlier
        (e.amount_nok > (cs.avg_amount + {{ var("anomaly_stddev_threshold") }} * cs.stddev_amount))
                                                                          as is_statistical_outlier,

        -- Flag: velocity spike
        (v.txn_count_in_window >= {{ var("velocity_count_threshold") }})  as is_velocity_spike,

        -- Flag: large round amount
        (e.is_round_amount and e.amount_nok > {{ var("large_round_amount_threshold") }})
                                                                          as is_large_round_amount,

        -- Flag: suspicious late night
        (e.is_late_night and e.amount_nok > {{ var("suspicious_late_night_amount") }})
                                                                          as is_suspicious_late_night,

        -- Flag: unusual geography
        (e.is_domestic_only and e.country_code != 'NO')                   as is_unusual_geography,

        -- Convenience: any anomaly flag raised
        (
            (e.amount_nok > (cs.avg_amount + {{ var("anomaly_stddev_threshold") }} * cs.stddev_amount))
            or (v.txn_count_in_window >= {{ var("velocity_count_threshold") }})
            or (e.is_round_amount and e.amount_nok > {{ var("large_round_amount_threshold") }})
            or (e.is_late_night and e.amount_nok > {{ var("suspicious_late_night_amount") }})
            or (e.is_domestic_only and e.country_code != 'NO')
        )                                                                  as has_any_anomaly_flag

    from enriched e
    left join customer_stats cs using (customer_id)
    left join velocity v using (transaction_id)
)

select * from flagged
